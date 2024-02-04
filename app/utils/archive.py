from py7zr import SevenZipFile

from .config import settings
import glob


class ArchiveObject:
    """Find all valid archive to index and use"""
    archive_folder = settings.archive_folder

    def __init__(self):
        pass

    def _lookup_archive(self, path: str):

        pass

    def list_sources(self) -> list:
        return glob.glob(f"{self.archive_folder}/*.7z")
