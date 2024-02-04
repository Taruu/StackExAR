from typing import List

from py7zr import SevenZipFile
from ..utils import config


class DataArchive:
    """Data archive object"""
    name = ""
    archive_path = ""
    reader = None

    def _check_file(self):
        with SevenZipFile(self.archive_path, 'r') as zip_read:
            allfiles = zip_read.getnames()
            print(allfiles)

    def __init__(self, archive_path: List[str] | str):
        self._check_file(archive_path)
        pass

    def __del__(self):
        pass


class Post:
    def __init__(self):
        pass

    pass


class PostAnswer(Post):
    pass


class Tag:
    pass
