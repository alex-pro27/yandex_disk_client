from __future__ import annotations

import contextlib
import datetime
import logging
import math
import sqlite3
from collections.abc import Generator, Iterable
from functools import cached_property
from typing import Sequence, List, Set

from app.types import DiskFile

logger = logging.root


class DiskRepository:

    def __init__(self, user_uid: str, app_id: str, db_path: str):
        self.__user_uid = user_uid
        self.__app_id = app_id
        self.__db_path = db_path

    def get_connection(self):
        return sqlite3.connect(self.__db_path)

    @contextlib.contextmanager
    def get_cursor(self) -> contextlib.AbstractContextManager[sqlite3.Cursor]:
        connection = sqlite3.connect(self.__db_path)
        try:
            yield connection.cursor()
            connection.commit()
        except Exception as e:
            connection.rollback()
            raise e
        finally:
            connection.close()

    def migrate(self):
        with self.get_cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS disk_structure
                (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_uid TEXT NOT NULL,
                    app_id TEXT NOT NULL,
                    last_sync INTEGER
                )
            """)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS disk_item_structure
                (
                    resource_id TEXT PRIMARY KEY,
                    name TEXT,
                    path TEXT,
                    md5 TEXT,
                    sha256 TEXT,
                    created TIMESTAMP,
                    modified TIMESTAMP,
                    size INTEGER,
                    disk_structure_id INTEGER NOT NULL,
                    download_url TEXT,
                    FOREIGN KEY(disk_structure_id) REFERENCES disk_structure(id) on delete CASCADE
                )
                """
            )

    def clear(self):
        with self.get_cursor() as cursor:
            cursor.execute("""
                DELETE FROM disk_item_structure
                WHERE disk_structure_id = ?
                """,
               (self.disk_structure_id,)
            )

    @cached_property
    def disk_structure_id(self):
        with self.get_cursor() as cursor:
            cursor.execute(
                "SELECT id FROM disk_structure WHERE user_uid = ? AND app_id = ?",
                (self.__user_uid, self.__app_id),
            )
            result = cursor.fetchone()
            if result:
                disk_structure_id = result[0]
            else:
                cursor.execute(
                    "INSERT INTO disk_structure (user_uid, app_id) VALUES (?, ?)",
                    (self.__user_uid, self.__app_id)
                )
                disk_structure_id = cursor.lastrowid
            return disk_structure_id

    def get_last_sync(self):
        with self.get_cursor() as cursor:
            cursor.execute(
                "SELECT last_sync FROM disk_structure WHERE id = ?",
                (self.disk_structure_id,),
            )
            last_sync = cursor.fetchone()[0]
            if last_sync:
                return datetime.datetime.fromtimestamp(last_sync)

    def update_last_sync(self):
        with self.get_cursor() as cursor:
            last_sync = datetime.datetime.now()
            cursor.execute(
                "UPDATE disk_structure SET last_sync = ? WHERE id = ?",
                (int(last_sync.timestamp()), self.disk_structure_id,),
            )
            return last_sync

    def update(self, files: Sequence[DiskFile]):
        with self.get_cursor() as cursor:
            for file_ in files:
                cursor.execute(
                    """
                    INSERT INTO disk_item_structure 
                    (resource_id, name, path, md5, sha256, created, modified, size, download_url, disk_structure_id)
                    VALUES 
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        file_.resource_id,
                        file_.name,
                        file_.path,
                        file_.md5,
                        file_.sha256,
                        file_.created.timestamp(),
                        file_.modified.timestamp(),
                        file_.size,
                        file_.download_url,
                        self.disk_structure_id,
                     )
                )

    def get_files_by_path(self, paths: Iterable[str]) -> Set[str]:
        with self.get_cursor() as cursor:
            cursor.execute(
                f"""
                SELECT path FROM disk_item_structure as it
                INNER JOIN disk_structure as ds ON ds.id = it.disk_structure_id
                WHERE ds.user_uid = ? AND ds.app_id = ? AND it.path IN ({",".join(("'" + p + "'" for p in paths))})
                """,
                (self.__user_uid, self.__app_id,)
            )
            return {item[0] for item in cursor.fetchall()}

    def get_all_files(self, batch_size: int = 10000) -> Generator[List[DiskFile], None, None]:
        with self.get_cursor() as cursor:
            cursor.execute(
                """
                SELECT id FROM disk_structure WHERE user_uid = ? AND app_id = ?
                """,
                (self.__user_uid, self.__app_id,)
            )
            disk_structure_id = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT count(resource_id) FROM disk_item_structure as it
                WHERE disk_structure_id = ?
                """,
                (disk_structure_id,)
            )
            count = cursor.fetchone()[0]
            limit = batch_size
            count_iter = math.ceil(count / limit)

            for i in range(count_iter):
                offset = i * limit
                cursor.execute(
                    """
                    SELECT 
                        resource_id,
                        name,
                        path,
                        md5,
                        sha256,
                        created,
                        modified,
                        size,
                        disk_structure_id,
                        download_url
                    FROM disk_item_structure
                    WHERE disk_structure_id = ? 
                    LIMIT ? OFFSET ?
                    """,
                    (disk_structure_id, limit, offset)
                )
                data = cursor.fetchall()
                yield [
                    DiskFile(
                        resource_id=item[0],
                        name=item[1],
                        path=item[2],
                        md5=item[3],
                        sha256=item[4],
                        created=datetime.datetime.fromtimestamp(item[5]),
                        modified=datetime.datetime.fromtimestamp(item[6]),
                        size=item[7],
                        download_url=item[8],
                    )
                    for item in data
                ]
