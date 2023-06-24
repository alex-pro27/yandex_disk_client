from __future__ import annotations

import datetime
from typing import NamedTuple, TypedDict, Optional, List


class SyncConfig(TypedDict):
    local_dir: str
    disk_dir: str
    regexes: Optional[List[str]]
    recursive: bool
    ignore_directories: bool
    sync_period: int


class FileStatus(NamedTuple):
    file: DiskFile
    from_disk_changed: bool
    from_local_changed: bool
    exists: bool


class DiskFile(NamedTuple):
    name: str
    path: str
    md5: str
    sha256: str
    size: int
    resource_id: str
    created: datetime.datetime
    modified: datetime.datetime
    download_url: str
