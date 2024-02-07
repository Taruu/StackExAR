from typing import List
from enum import Enum
from pathlib import Path

from asyncio import get_event_loop, gather, sleep
import aiofiles

from py7zr import SevenZipFile, is_7zfile
from ..utils import config

POSTS_FILENAME = "Posts.xml"
TAGS_FILENAME = "Tags.xml"


class AsyncFileReader:
    def __init__(self):
        pass

    def read(self, bytes):
        pass


class DataArchive:
    """Data archive object"""
    name = None

    post_archive_path = None
    tags_archive_path = None

    reader = None

    def _sync_read_archive(self, archive_path, filename):
        pass

    @staticmethod
    async def async_read_archive(archive_path, filename):
        with SevenZipFile(archive_path, 'r') as archive_read:
            file_reader = archive_read.read(targets=filename).get('Tags.xml')
            print(file_reader.read(1000))
            print(archive_read, file_reader)

    async def read_tags(self):
        await self.async_read_archive(self.tags_archive_path, TAGS_FILENAME)

    def __init__(self, archive_path: List[str] | str):
        if not is_7zfile(archive_path):
            raise ValueError(f"Not a archvie: {archive_path}")

        with SevenZipFile(archive_path, 'r') as archive_read:
            all_archive_files = archive_read.getnames()

        obj_path = Path(archive_path)
        self.name = obj_path.name

        if (POSTS_FILENAME in all_archive_files) and (TAGS_FILENAME in all_archive_files):

            self.post_archive_path = archive_path
            self.tags_archive_path = archive_path

        elif (POSTS_FILENAME in all_archive_files) and ("-" in obj_path.name):
            # Big archive use '-' for separate
            archive_name = obj_path.name.split('-')[0]
            tags_archive_path = f"{obj_path.parent}/{archive_name}-Tags.7z"

            with SevenZipFile(tags_archive_path, 'r') as temp_read:
                temp_all_archive_files = temp_read.getnames()

            if TAGS_FILENAME in temp_all_archive_files:
                self.tags_archive_path = Path(tags_archive_path)
                self.post_archive_path = obj_path
            else:
                raise ValueError(f"{tags_archive_path} not exist")
        else:
            raise ValueError(f"Not correct archive: {archive_path}")

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
