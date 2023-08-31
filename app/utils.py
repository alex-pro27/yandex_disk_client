from __future__ import annotations

import asyncio
import logging
import os.path
import threading
from collections.abc import Coroutine
from functools import wraps
from typing import Iterable, TypeVar, Generator, Sequence, Callable, List

T = TypeVar("T")

logger = logging.root


def batch_generator(iterable: Iterable[T], batch_size=10000) -> Generator[List[T], None, None]:
    batch = []
    for item in iterable:
        if len(batch) == batch_size:
            yield batch
            batch = []
        batch.append(item)
    if len(batch):
        yield batch


def get_diff_index(*compared: Sequence[str]):
    arr_A = compared[0]
    diff_idx = set()
    for j in range(1, len(compared)):
        arr_B = compared[j]
        for i in range(len(arr_A)):
            if arr_A[i] != arr_B[i]:
                diff_idx.add(i)
    return diff_idx


def get_default_event_loop() -> asyncio.AbstractEventLoop:
    _thread = getattr(get_default_event_loop, "_thread", None)
    _loop = getattr(get_default_event_loop, "_loop", None)
    if _thread is None:
        if _loop is None:
            try:
                get_default_event_loop._loop = _loop = asyncio.get_running_loop()
            except RuntimeError:
                get_default_event_loop._loop = _loop = asyncio.new_event_loop()
                asyncio.set_event_loop(_loop)
        if not _loop.is_running():
            get_default_event_loop._thread = _thread = threading.Thread(
                target=_loop.run_forever,
                daemon=True
            )
            _thread.start()
    return _loop


def async_to_sync(coroutine: Coroutine):
    loop = get_default_event_loop()
    future = asyncio.run_coroutine_threadsafe(coroutine, loop)
    return future.result()


def normalize_part_path(path: str):
    if path.startswith("/"):
        path = path[1:]
    return path


def join_path(base: str, *paths: str):
    return os.path.join(base, *map(normalize_part_path, paths))


def handle_tasks(
    futures: Iterable[asyncio.Future],
    handle_error: Callable[[BaseException], None] | None = None
):
    results = []
    for future in futures:
        if future.cancelled():
            continue
        error = future.exception()
        if error:
            handle_error and handle_error(error)
            continue
        else:
            results.append(future.result())
    return results


def handle_error_decorator(handler: Callable[[BaseException], None]):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except BaseException as e:
                handler(e)
        return wrapper
    return decorator


def error_handler(error: BaseException):
    from app.disk_api_client import DiskApiClientError
    if isinstance(error, DiskApiClientError):
        logger.error(f"API ERROR - Code: {error.code}. Message: {', '.join(error.messages)}")
    else:
        logger.exception(error)


def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.2f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.2f}Yi{suffix}"
