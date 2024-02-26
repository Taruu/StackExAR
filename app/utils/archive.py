import asyncio
from pathlib import Path

from py7zr import SevenZipFile

from .config import settings
import glob

from .app_types import DataArchiveReader

# from ..global_app import app


archive_readers = {}


def get_archive_reader(name: str) -> DataArchiveReader:
    print(f"GET READER {name}")
    if name in archive_readers:
        return archive_readers.get(name)
    archive_list = glob.glob(f"{settings.archive_folder}/*.7z")
    data_archives_set = set([Path(path).name for path in archive_list])
    if name in data_archives_set:
        archive_reader = DataArchiveReader(f"{settings.archive_folder}/{name}")
        archive_readers.update({name: archive_reader})
        return archive_reader
    else:
        raise ValueError(f"{settings.archive_folder}/{name} does exist")
