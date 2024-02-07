from py7zr import SevenZipFile

from .config import settings
import glob

from .types import DataArchive


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
                data_archives_list.append(DataArchive(path))
            except ValueError:
                continue
        for data_archive in data_archives_list:
            await data_archive.read_tags()
        return []
