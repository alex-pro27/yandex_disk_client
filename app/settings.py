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

config: AppConfigType = get_config("config.yaml")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


BASE_PATH = os.path.dirname(os.path.dirname(__file__))

DB_PATH = os.path.join(BASE_PATH, "default.sqlite")

QUEUE_SIZE = 100
READ_FILE_CHUNK_SIZE = 5 * 1024 * 1024
