from __future__ import annotations

import logging
import os
import re
from hashlib import sha256
from typing import TypeVar, Literal, Sequence, Optional

import asyncio

import aiofiles

from app.disk_api_client import DiskApiClient, DiskFile
from app.repository import DiskRepository
from app.types import FileStatus, SyncConfig
from app.settings import config, DB_PATH, QUEUE_SIZE, READ_FILE_CHUNK_SIZE
from app.utils import normalize_part_path, handle_tasks, handle_error_decorator, error_handler

T = TypeVar("T")

logger = logging.root


class DiskSyncHandler:

    def __init__(
        self,
        sema: asyncio.Semaphore,
        api_client: DiskApiClient,
        disk_repository: DiskRepository,
        user_uid: str,
        sync_config: SyncConfig,
        conflict_priority: Literal["disk", "local"],
        ignore_regexes: Optional[Sequence[str]] = None,
    ):
        self._api_client = api_client
        self._disk_repository = disk_repository
        self._disk_dir = sync_config["disk_dir"]
        self._local_dir = sync_config["local_dir"]
        self._sema = sema
        self._conflict_priority = conflict_priority
        self._ignore_regexes  = [re.compile(text, re.I) for text in ignore_regexes or []]
        self._regexes = [re.compile(text, re.I) for text in sync_config.get("regexes") or []]
        self.__user_uid = user_uid
        self._skip_ext = [".tmp"]
        self._lock = asyncio.Lock()

    def need_ignore(self, path: str):
        if self._ignore_regexes:
            if any(r.match(path) for r in self._ignore_regexes):
                return True
        return False

    def check_for_allow(self, file_path: str) -> bool:
        if self.need_ignore(file_path):
            return False
        if self._regexes:
            return any(r.match(file_path) for r in self._regexes)
        return True

    @handle_error_decorator(error_handler)
    async def get_file_status(self, file_: DiskFile):
        async with self._sema:
            path = normalize_part_path(file_.path)
            path = os.path.join(self._local_dir, path)

            if not os.path.exists(path):
                return FileStatus(exists=False, from_local_changed=False, from_disk_changed=False, file=file_)

            async with aiofiles.open(path, "rb") as f:
                hash_sum = sha256()
                chunk_size = READ_FILE_CHUNK_SIZE
                chunk = await f.read(chunk_size)
                while chunk:
                    hash_sum.update(chunk)
                    chunk = await f.read(chunk_size)
                file_hash = hash_sum.hexdigest()
                changed = file_hash != file_.sha256
                from_disk_changed = False
                from_local_changed = False
                if changed:
                    local_modify = round(os.path.getmtime(path))
                    if local_modify < file_.modified.timestamp():
                        from_disk_changed = True
                    else:
                        from_local_changed = True
                return FileStatus(
                    exists=True,
                    from_local_changed=from_local_changed,
                    from_disk_changed=from_disk_changed,
                    file=file_
                )

    async def handle_file(self, file_: DiskFile, remove_from_disk_if_local_not_exists: bool):
        file_status = await self.get_file_status(file_)
        if not file_status.exists and remove_from_disk_if_local_not_exists:
            await self.remove_file_from_disk(file_.path)
        elif not file_status.exists or file_status.from_disk_changed:
            await self.download_file_to_local(file_)
        elif file_status.from_local_changed:
            await self.put_file_to_disk(file_.path, overwrite=True)

    @handle_error_decorator(error_handler)
    async def sync_disk_files(self):
        batch_size = 10000
        files = []

        local_last_sync = self._disk_repository.get_last_sync()
        disk_last_sync = await self._api_client.get_last_sync_for_disk_dir(self._sema, self._disk_dir)

        remove_from_disk_if_local_not_exists = False

        if local_last_sync and disk_last_sync:
            remove_from_disk_if_local_not_exists = disk_last_sync <= local_last_sync

        self._disk_repository.clear()

        async for file_ in self._api_client.get_files_flat_list(self._sema, self._disk_dir):
            if not self.check_for_allow(file_.path):
                continue
            if len(files) == batch_size:
                finished, __ = await asyncio.wait(
                    [
                        asyncio.create_task(
                            self.handle_file(file_, remove_from_disk_if_local_not_exists)
                        )
                        for file_ in files
                    ]
                )
                handle_tasks(finished, error_handler)
                self._disk_repository.update(files)
                files = []
            else:
                files.append(file_)
        if files:
            finished, __ = await asyncio.wait([
                asyncio.create_task(
                    self.handle_file(file_, remove_from_disk_if_local_not_exists)
                )
                for file_ in files
            ])
            handle_tasks(finished, error_handler)
            self._disk_repository.update(files)

        await self._api_client.add_last_sync_to_disk_dir(
            self._sema,
            self._disk_dir,
            self._disk_repository.update_last_sync(),
        )

    @handle_error_decorator(error_handler)
    async def sync_local_files(self):
        local_dir = self._local_dir
        disk_dir = self._disk_dir

        if local_dir.endswith("/"):
            local_dir = local_dir[:-1]

        if disk_dir.endswith("/"):
            disk_dir = disk_dir[:-1]

        files_tasks = []
        batch_size = 10000

        for root, dirs, files in os.walk(local_dir):
            if self.need_ignore(root):
                continue
            paths = {os.path.join(root, file_path).replace(local_dir, "") for file_path in files}
            put_files = set(filter(self.check_for_allow, paths)) ^ self._disk_repository.get_files_by_path(paths)
            if put_files:
                await self.prepare_dirs(root.replace(local_dir, disk_dir), is_file=False)
                for file_path in put_files:
                    files_tasks.append(asyncio.create_task(
                        self.put_file_to_disk(file_path, prepare_dir=False)
                    ))
                    if len(files_tasks) == batch_size:
                        finished, __ = await asyncio.wait(files_tasks)
                        handle_tasks(finished, error_handler)
                        files_tasks = []
        if files_tasks:
            finished, __ = await asyncio.wait(files_tasks)
            handle_tasks(finished, error_handler)

    async def prepare_dirs(self, path: str, is_file: bool = True):
        if is_file:
            path = os.path.dirname(path)
        tree_dirs = list(filter(bool, path.split("/")))
        not_exists_idx = -1
        for idx in reversed(range(len(tree_dirs))):
            _path = os.path.join(*tree_dirs[:idx + 1])
            if await self._api_client.exists(self._sema, _path):
                break
            else:
                not_exists_idx = idx
        if not_exists_idx == -1:
           return
        for i, dir_name in enumerate(tree_dirs[not_exists_idx:]):
            _path = os.path.join(*tree_dirs[:not_exists_idx + i], dir_name)
            await self._api_client.make_dir(self._sema, _path)
            logger.info(f"Created a folder {_path}")

    @handle_error_decorator(error_handler)
    async def remove_file_from_disk(self, file_path: str):
        file_path = normalize_part_path(file_path.replace(self._local_dir, ""))
        await self._api_client.remove_resource(self._sema, self._disk_dir, file_path, permanently=False)
        disk_file_path = os.path.join(self._disk_dir, file_path)
        logger.info(f"remove_file_from_disk: '{disk_file_path}' success")

    @handle_error_decorator(error_handler)
    async def put_file_to_disk(self, file_path: str, overwrite: bool = False, prepare_dir: bool = True):
        if any([file_path.endswith(ext) for ext in self._skip_ext]):
            return
        file_path = normalize_part_path(file_path.replace(self._local_dir, ""))

        if prepare_dir:
            async with self._lock:
                await self.prepare_dirs(os.path.join(self._disk_dir, file_path))

        status = None

        if overwrite:
            status = await self._api_client.put_file(
                sema=self._sema,
                disk_dir=self._disk_dir,
                local_dir=self._local_dir,
                file_path=file_path,
                overwrite=overwrite,
                chunk_size=READ_FILE_CHUNK_SIZE,
            )

        if not await self._api_client.exists(self._sema, os.path.join(self._disk_dir, file_path)):
            status = await self._api_client.put_file(
                sema=self._sema,
                disk_dir=self._disk_dir,
                local_dir=self._local_dir,
                file_path=file_path,
                overwrite=False,
                chunk_size=READ_FILE_CHUNK_SIZE,
            )
        if status:
            logger.info(f"put_file_to_disk: '{file_path}' with status '{status}'")

    @handle_error_decorator(error_handler)
    async def move_file(self, from_path: str, to_path: str):
        if not to_path.startswith(self._local_dir):
            return False

        from_path = normalize_part_path(from_path.replace(self._local_dir, ""))
        to_path = normalize_part_path(to_path.replace(self._local_dir, ""))

        async with self._lock:
            await self.prepare_dirs(os.path.join(self._disk_dir, to_path))

        if await self._api_client.exists(self._sema, os.path.join(self._disk_dir, from_path)):
            return await self._api_client.move(
                sema=self._sema,
                disk_dir=self._disk_dir,
                from_path=from_path,
                to_path=to_path,
            )
        else:
            await self.put_file_to_disk(to_path, prepare_dir=False)
            return True

    @handle_error_decorator(error_handler)
    async def download_file_to_local(self, file_: DiskFile):
        file_path = file_.path
        file_path = normalize_part_path(file_path)
        local_path = os.path.join(self._local_dir, file_path)

        local_file_dir = os.path.dirname(local_path)

        if not os.path.exists(local_file_dir):
            os.makedirs(local_file_dir)

        gen = self._api_client.get_file_content(
            self._sema,
            file_,
            chunk_size=READ_FILE_CHUNK_SIZE,
        )
        async with aiofiles.open(local_path, "wb") as f:
            async for chunk in gen:
                await f.write(chunk)
        logger.info(f"download file from disk to {local_path}")

    @classmethod
    async def init(
        cls,
        sema: asyncio.Semaphore,
        sync_config: SyncConfig,
        ignore_regexes: Optional[Sequence[str]] = None,
    ):
        api_client = DiskApiClient(oauth_token=config["disc_api.oauth_token"])
        user_uid = await api_client.get_user_uid()
        disk_repository = DiskRepository(user_uid=user_uid, app_id=config["disc_api.app_id"], db_path=DB_PATH)
        disk_repository.migrate()
        instance = cls(
            sema=sema,
            api_client=api_client,
            disk_repository=disk_repository,
            user_uid=user_uid,
            sync_config=sync_config,
            conflict_priority="disk",
            ignore_regexes=ignore_regexes,
        )
        await instance.prepare_dirs(sync_config["disk_dir"], is_file=False)
        return instance

    @classmethod
    async def sync(
        cls,
        *,
        sema: asyncio.Semaphore,
        sync_config: SyncConfig,
        params:  Literal["disk", "local", "all"],
        ignore_regexes: Optional[Sequence[str]] = None,
    ):
        instance = await cls.init(sema, sync_config, ignore_regexes)
        if params in {"disk", "all"}:
            logger.info("run sync disk files")
            await instance.sync_disk_files()
        if params in {"local", "all"}:
            logger.info("run sync local files")
            await instance.sync_local_files()
        return instance


async def sync_disk(params: Literal["disk", "local", "all"]):
    sema = asyncio.Semaphore(QUEUE_SIZE)
    tasks = [
        asyncio.create_task(
            DiskSyncHandler.sync(
                sema=sema,
                sync_config=sync_config,
                params=params,
                ignore_regexes=config.get("ignore_regexes")
            )
        )
        for sync_config in config["sync"]
    ]
    finished, __ = await asyncio.wait(tasks)
    handle_tasks(finished, error_handler)
