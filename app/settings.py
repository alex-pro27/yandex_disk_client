from __future__ import annotations

import logging
import os
from typing import TypedDict, List

from app.types import SyncConfig
from app.config_parser import get_config


AppConfigType = TypedDict("AppConfigType", {
    "disk_api.app_id": str,
    "disk_api.oauth_token": str,
    "ignore_regexes": List[str],
    "sync": List[SyncConfig],
    "system.tasks_pool_size": int,
    "system.read_file_chunk_size": int,
})

BASE_PATH = os.path.dirname(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_PATH, "default.sqlite")

config_path = os.environ.get("YA_DISK_CLIENT_CONFIG") or os.path.join(BASE_PATH, "config.yaml")
config: AppConfigType = get_config(config_path)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

TASKS_POOL_SIZE = config["system.tasks_pool_size"]
READ_FILE_CHUNK_SIZE = config["system.read_file_chunk_size"]
