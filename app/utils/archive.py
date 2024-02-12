import asyncio

from py7zr import SevenZipFile

from .config import settings
import glob

from .types import DataArchiveReader


class ArchiveFeed:
    """Find all valid archive to index and use"""
    archive_folder = settings.archive_folder

    def __init__(self):
        pass

    def _lookup_archive(self, path: str):
        pass

    async def list_sources(self) -> list:
        archive_list = glob.glob(f"{self.archive_folder}/*.7z")
        data_archives_list = []
        for path in archive_list:
            try:
                data_archives_list.append(DataArchiveReader(path))
            except ValueError:
                continue
        task_list = []
        async with asyncio.TaskGroup() as tg:
            for data_archive in data_archives_list:
                tg.create_task(data_archive.index_posts())
        return []
