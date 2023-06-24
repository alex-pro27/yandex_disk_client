from __future__ import annotations

import argparse
import signal
import sys

from app.disk_sync_handler import sync_disk
from app.files_watcher import watch_all
from app.utils import async_to_sync


def sig_handler(signum, frame):
    sys.exit(signum)


signal.signal(signal.SIGINT, sig_handler)


def args_parse():
    parser = argparse.ArgumentParser(description="run")
    parser.add_argument(
        "--sync",
        dest="sync",
        default=None,
        required=False,
        help="run sync files (disk | local | all)",
    )
    parser.add_argument(
        "--watch",
        dest="watch",
        action='store_true',
        default=False,
        required=False,
        help="run watch files",
    )
    return parser.parse_args()


def main(args: argparse.Namespace):
    if args.sync:
        if args.sync in {"local", "disk", "all"}:
            async_to_sync(sync_disk(args.sync))
        else:
            print("arg sync should contain ('disk' | 'local' | 'all') ")
            exit(1)
    if args.watch:
        async_to_sync(watch_all())
        print(' [*] Watching files. To exit press CTRL+C')
        signal.pause()


if __name__ == "__main__":
    args = args_parse()
    main(args)
