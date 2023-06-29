from __future__ import annotations

import logging
import os
from typing import TypedDict, List

from app.types import SyncConfig
from app.config_parser import get_config


AppConfigType = TypedDict("AppConfigType", {
    "disc_api.app_id": str,
    "disc_api.oauth_token": str,
    "ignore_regexes": List[str],
    "sync": List[SyncConfig],
})

BASE_PATH = os.path.dirname(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_PATH, "default.sqlite")

config_path = os.environ.get("YA_DISK_CONFIG") or os.path.join(BASE_PATH, "config.yaml")
config: AppConfigType = get_config(config_path)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

QUEUE_SIZE = 100
READ_FILE_CHUNK_SIZE = 5 * 1024 * 1024
