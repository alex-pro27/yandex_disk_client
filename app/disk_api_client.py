from __future__ import annotations

import asyncio
import logging
import os.path
from collections.abc import AsyncGenerator
from datetime import datetime
from functools import cached_property
from typing import Literal, Tuple, List, Any, Optional

import aiofiles
import aiohttp
import math

from aiohttp_retry import RetryClient, ExponentialRetry
from app.types import DiskFile
from app.utils import batch_generator, normalize_part_path, handle_tasks, error_handler, sizeof_fmt

logger = logging.root


class DiskApiClientError(Exception):
    def __init__(self, code: str | int, message: List[str] | Tuple[str] | str):
        super().__init__((code, message))
        self.code = code
        if not isinstance(message, list | tuple):
            message = [message]
        self.messages = tuple(message)


async def get_error_message(resp: aiohttp.ClientResponse):
    try:
        content = await resp.json()
        if "message" in content:
            return content["message"]
        return content
    except aiohttp.ContentTypeError:
        content = await resp.content.read()
        content = content.decode()
    return content

class DiskApiClient:

    API_URL = "https://cloud-api.yandex.net/v1/disk"

    def __init__(self, oauth_token: str):
        self.__oauth_token = oauth_token

    @cached_property
    def headers(self):
        return {"Authorization": f"OAuth {self.__oauth_token}"}

    def get_url(self, path: str):
        return self.API_URL + path

    async def get_user_uid(self) -> str:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.API_URL, headers=self.headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data["user"]["uid"]
                else:
                    raise DiskApiClientError(
                        code=resp.status,
                        message=[
                            f"get_files error with status code {resp.status}",
                            await get_error_message(resp)
                        ],
                    )

    async def exists(self, sema: asyncio.Semaphore, path: str):
        async with sema, aiohttp.ClientSession() as session:
            retry_client = RetryClient(
                client_session=session,
                retry_options=ExponentialRetry(attempts=3)
            )
            async with retry_client.get(
                self.get_url("/resources"),
                params={"path": path},
                headers=self.headers
            ) as resp:
                if resp.status == 200:
                    return True
                if resp.status == 404:
                    return False
                raise DiskApiClientError(
                    message=[
                        f"exists_dir error with status code {resp.status}",
                        await get_error_message(resp),
                    ],
                    code=resp.status,
                )

    async def make_dir(self, sema: asyncio.Semaphore, dir_name: str):
        async with sema, aiohttp.ClientSession() as session:
            retry_client = RetryClient(
                client_session=session,
                retry_options=ExponentialRetry(attempts=3)
            )
            async with retry_client.put(
                self.get_url("/resources"),
                params={"path": dir_name},
                headers=self.headers,
            ) as resp:
                if resp.status in {201, 409}:
                    return True
                raise DiskApiClientError(
                    message=[
                        f"make_dir error with status code {resp.status}",
                        await get_error_message(resp),
                    ],
                    code=resp.status,
                )

    async def move(self, sema: asyncio.Semaphore, disk_dir: str, from_path: str, to_path: str):
        async with sema, aiohttp.ClientSession() as session:
            _from_path = normalize_part_path(from_path)
            _to_path = normalize_part_path(to_path)

            _from_path = os.path.join(disk_dir, _from_path)
            _to_path = os.path.join(disk_dir, _to_path)

            retry_client = RetryClient(
                client_session=session,
                retry_options=ExponentialRetry(attempts=3)
            )

            async with retry_client.post(
                self.get_url("/resources/move"),
                params={
                    "from": _from_path,
                    "path": _to_path,
                    "overwrite": "false",
                },
                headers=self.headers,
            ) as resp:
                if resp.status == 201:
                    return True
                else:
                    raise DiskApiClientError(
                        message=[
                            f"move error with status code {resp.status}",
                            await get_error_message(resp),
                        ],
                        code=resp.status,
                    )

    async def remove_resource(self, sema: asyncio.Semaphore, disk_dir: str, path: str, permanently: bool):
        async with sema, aiohttp.ClientSession() as session:
            retry_client = RetryClient(
                client_session=session,
                retry_options=ExponentialRetry(attempts=3)
            )
            async with retry_client.delete(
                self.get_url("/resources"),
                params={
                    "path": os.path.join(disk_dir, normalize_part_path(path))
                },
                headers=self.headers,
            ) as resp:
                if resp.status not in {204, 202}:
                    raise DiskApiClientError(
                        message=[
                            f"remove_resource error with status code {resp.status}",
                            await get_error_message(resp),
                        ],
                        code=resp.status,
                    )

    async def get_files_dir(
        self,
        sema: asyncio.Semaphore,
        disk_dir: str,
        dir_path: str = "",
        limit: int = 100,
        offset: int = 0,
    ):
        files: List[DiskFile] = []
        dirs: List[str] = []

        async with sema, aiohttp.ClientSession() as session:
            _path = normalize_part_path(dir_path)
            async with session.get(
                self.get_url(f"/resources"),
                params={
                    "path": os.path.join(disk_dir, _path),
                    "limit": limit,
                    "offset": offset,
                },
                headers=self.headers,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data["_embedded"]["items"]:
                        item_type = item["type"]
                        item_path: str = item["path"].replace(f"disk:{disk_dir}", "")

                        if not item_path.startswith("/"):
                            item_path = "/" + item_path

                        if item_type == "file":
                            files.append(
                                DiskFile(
                                    name=item["name"],
                                    path=item_path,
                                    md5=item["md5"],
                                    sha256=item["sha256"],
                                    resource_id=item["resource_id"],
                                    created=datetime.fromisoformat(item["created"]),
                                    modified=datetime.fromisoformat(item["modified"]),
                                    download_url=item["file"],
                                    size=item["size"],
                                )
                            )
                        elif item_type == "dir":
                            dirs.append(item_path)
                    total: int = data["_embedded"]["total"]
                    return files, dirs, total
                else:
                    raise DiskApiClientError(
                        message=[
                            f"get_files error with status code {resp.status}",
                            await get_error_message(resp),
                        ],
                        code=resp.status,
                    )

    async def get_files_flat_list(
        self,
        sema: asyncio.Semaphore,
        disk_dir: str,
        dir_path: str = "",
        limit: int = 100,
    ) -> AsyncGenerator[DiskFile, None, None]:

        async def _get_files(_dir_path):
            files, dirs, total = await self.get_files_dir(
                sema,
                disk_dir=disk_dir,
                dir_path=_dir_path,
                limit=limit,
                offset=0,
            )

            yield files, dirs

            if total > limit:
                count_iter = math.ceil(total / limit)
                offsets = []

                for i in range(1, count_iter):
                    offsets.append(i * limit)

                for batch in batch_generator(offsets, 100):
                    tasks = [
                        asyncio.create_task(
                            self.get_files_dir(
                                sema,
                                disk_dir=disk_dir,
                                dir_path=_dir_path,
                                limit=limit,
                                offset=offset,
                            )
                        )
                        for offset in batch
                    ]
                    finished, __ = await asyncio.wait(tasks)
                    results = handle_tasks(finished, error_handler)
                    for _files, _dirs, __ in results:
                        yield _files, _dirs

        async def _get_files_from_dir(_dir_path):
            return [
                _file
                async for _file in self.get_files_flat_list(sema, disk_dir, _dir_path)
            ]

        async for _files, _dirs in _get_files(dir_path):
            for _file in _files:
                yield _file
            if _dirs:
                finished, __ = await asyncio.wait([
                    asyncio.create_task(_get_files_from_dir(_dir))
                    for _dir in _dirs
                ])
                for results in handle_tasks(finished, error_handler):
                    for _file in results:
                        yield _file

    async def put_custom_props_to_resource(
        self,
        sema: asyncio.Semaphore,
        disk_dir: str,
        path: str,
        custom_props: dict[str, Any],
    ):
        async with sema, aiohttp.ClientSession() as session:
            retry_client = RetryClient(
                client_session=session,
                retry_options=ExponentialRetry(attempts=3)
            )
            async with retry_client.patch(
                self.get_url("/resources"),
                params={"path": os.path.join(disk_dir, normalize_part_path(path))},
                json={"custom_properties": custom_props},
                headers=self.headers,
                timeout=3600,
            ) as resp:
                if resp.status != 200:
                    raise DiskApiClientError(
                        message=[
                            f"put_custom_props error with status code {resp.status}",
                            await get_error_message(resp),
                        ],
                        code=resp.status,
                    )

    async def add_last_sync_to_disk_dir(self, sema: asyncio.Semaphore, disk_dir: str, last_sync: datetime):
        await self.put_custom_props_to_resource(
            sema, disk_dir, "", {"last_sync_ya_disk_client": last_sync.isoformat(timespec="seconds")}
        )

    async def get_last_sync_for_disk_dir(self, sema: asyncio.Semaphore, disk_dir: str) -> Optional[datetime]:
        async with sema, aiohttp.ClientSession() as session:
            retry_client = RetryClient(
                client_session=session,
                retry_options=ExponentialRetry(attempts=3)
            )
            async with retry_client.get(
                self.get_url("/resources"),
                params={
                    "path": normalize_part_path(disk_dir),
                    "fields": "custom_properties",
                    "limit": 0,
                },
                headers=self.headers,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    props = data.get("custom_properties")
                    if props:
                        return datetime.fromisoformat(props["last_sync_ya_disk_client"])
                    return None

            raise DiskApiClientError(
                message=[
                    f"put_custom_props error with status code {resp.status}",
                    await get_error_message(resp),
                ],
                code=resp.status,
            )

    async def _get_upload_url(self, file_path: str, overwrite: bool) -> str:
        async with aiohttp.ClientSession() as session:
            retry_client = RetryClient(
                client_session=session,
                retry_options=ExponentialRetry(attempts=3)
            )
            async with retry_client.get(
                self.get_url("/resources/upload"),
                params={"path": file_path, "overwrite": ("false", "true")[overwrite]},
                headers=self.headers,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data["href"]
                raise DiskApiClientError(
                    message=[
                        f"get_download_url error with status code {resp.status}",
                        await get_error_message(resp),
                    ],
                    code=resp.status,
                )

    async def put_file(
        self,
        sema: asyncio.Semaphore,
        disk_dir: str,
        local_dir,
        file_path: str,
        overwrite: bool,
        chunk_size: int = 64 * 1024
    ) -> Literal["conflict", "uploaded", "large_file"]:
        async with sema, aiohttp.ClientSession() as session:
            file_path = normalize_part_path(file_path)
            try:
                url = await self._get_upload_url(os.path.join(disk_dir, file_path), overwrite=overwrite)
            except DiskApiClientError as e:
                if e.code == 409:
                    return "conflict"
                else:
                    raise e
            retry_client = RetryClient(
                client_session=session,
                retry_options=ExponentialRetry(attempts=3)
            )
            async with retry_client.put(
                url,
                data=self.file_sender(os.path.join(local_dir, file_path), chunk_size),
                timeout=3600,
            ) as resp:
                if resp.status == 201:
                    return "uploaded"
                if resp.status == 413:
                    return "large_file"
                raise DiskApiClientError(
                    message=[
                        f"put_file: {file_path}, error with status code {resp.status}",
                        await get_error_message(resp),
                    ],
                    code=resp.status,
                )

    async def file_sender(self, file_path: str, chunk_size: int):
        file_stats = os.stat(file_path)
        file_size = sizeof_fmt(file_stats.st_size)

        async with aiofiles.open(file_path, 'rb') as f:
            chunk = await f.read(chunk_size)
            i = 0
            while chunk:
                yield chunk
                i += 1
                sending_size = sizeof_fmt(len(chunk) * i)
                logger.info(
                    f"send file: {file_path} - {sending_size} / {file_size}"
                )
                chunk = await f.read(chunk_size)

    async def get_file_content(self, sema: asyncio.Semaphore, file_: DiskFile, chunk_size: int = 64 * 1024):
        async with sema, aiohttp.ClientSession() as session:
            async with session.get(file_.download_url, headers=self.headers) as resp:
                if resp.status == 200:
                    chunk = await resp.content.read(chunk_size)
                    while chunk:
                        yield chunk
                        chunk = await resp.content.read(chunk_size)
                else:
                    raise DiskApiClientError(
                        message=[
                            f"get_file_content error with status code {resp.status}",
                            await get_error_message(resp),
                        ],
                        code=resp.status,
                    )
