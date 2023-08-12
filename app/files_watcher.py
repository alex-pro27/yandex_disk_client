from __future__ import annotations

import asyncio
import logging

from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler

from app.disk_sync_handler import DiskSyncHandler
from app.types import SyncConfig
from app.settings import config, QUEUE_SIZE
from app.utils import async_to_sync

logger = logging.root


class DiscClientEventHandler(RegexMatchingEventHandler):

    def __init__(self, disk_sync_handler: DiskSyncHandler, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._disk_sync_handler = disk_sync_handler

    def on_moved(self, event):
        what = "directory" if event.is_directory else "file"
        logger.info("Moved %s: from %s to %s", what, event.src_path, event.dest_path)
        coro = self._disk_sync_handler.move_file(from_path=event.src_path, to_path=event.dest_path)
        async_to_sync(coro)

    def on_created(self, event):
        what = "directory" if event.is_directory else "file"
        logger.info("Created %s: %s", what, event.src_path)
        coro = self._disk_sync_handler.put_file_to_disk(event.src_path)
        async_to_sync(coro)

    def on_deleted(self, event):
        if not event.is_directory:
            self._disk_sync_handler.remove_file_from_disk(event.src_path)
        what = "directory" if event.is_directory else "file"
        logger.info("Deleted %s: %s", what, event.src_path)

    def on_modified(self, event):
        what = "directory" if event.is_directory else "file"
        logger.info("Modified %s: %s", what, event.src_path)
        coro = self._disk_sync_handler.put_file_to_disk(event.src_path, overwrite=True)
        async_to_sync(coro)


async def watch(sema: asyncio.BoundedSemaphore, sync_config: SyncConfig):
    disk_sync_handler = await DiskSyncHandler.init(
        sema=sema,
        sync_config=sync_config,
        ignore_regexes=config.get("ignore_regexes"),
    )
    event_handler = DiscClientEventHandler(
        disk_sync_handler=disk_sync_handler,
        regexes=sync_config.get("regexes"),
        ignore_regexes=config.get("ignore_regexes"),
        ignore_directories=sync_config.get("ignore_directories", False),
    )
    observer = Observer()
    observer.schedule(
        event_handler,
        sync_config["local_dir"],
        recursive=sync_config["recursive"],
    )
    observer.start()


async def watch_all():
    futures = []
    sema = asyncio.BoundedSemaphore(QUEUE_SIZE)

    for watch_config in config["sync"]:
        futures.append(watch(sema, watch_config))
    await asyncio.wait(futures)
