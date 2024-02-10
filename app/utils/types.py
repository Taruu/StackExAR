import asyncio
import multiprocessing
import queue
import time
from collections import deque
from queue import Queue
from typing import List
from enum import Enum
from pathlib import Path
import functools
import xml.etree.ElementTree as ElementTree

from sqlalchemy import select, delete

from ..database.function import get_database_session
from ..database.models import Tag, QuestionPost, Base

from asyncio import get_event_loop, gather, sleep

from py7zr import SevenZipFile, is_7zfile
from tortoise import Tortoise

from .. import global_app
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

    @staticmethod
    def _sync_read_archive_by_address(archive_path, filename, start: int, length: int):
        with SevenZipFile(archive_path, 'r') as archive_read:
            file_reader = archive_read.read(targets=filename).get(filename)
            file_reader.seek(start)
            request_bytes = file_reader.read(length)
        return request_bytes

    @staticmethod
    def _sync_read_archive(archive_path, filename, bytes_queue):
        with SevenZipFile(archive_path, 'r') as archive_read:
            file_reader = archive_read.read(targets=filename).get(filename)

            for line in file_reader.readlines():
                bytes_queue.put_nowait(line)
        return True

    async def async_read_archive_by_address(self, archive_path, filename, start: int, length: int):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(global_app.app.process_pools, self._sync_read_archive_by_address,
                                          archive_path, filename, start, length)

    async def async_read_archive_generator(self, archive_path, filename):
        loop = asyncio.get_running_loop()

        m = multiprocessing.Manager()
        bytes_queue: multiprocessing.Queue = m.Queue()
        sync_future: asyncio.Future = loop.run_in_executor(
            global_app.app.process_pools, self._sync_read_archive, archive_path, filename, bytes_queue)

        while (not bytes_queue.empty()) or (not sync_future.done()):
            try:
                value_temp: bytes = bytes_queue.get_nowait()
                yield value_temp
            except queue.Empty:
                await asyncio.sleep(0)

    async def index_tags(self):
        async_session = await get_database_session(f"{Path(self.post_archive_path).parent}/{self.name}.db")
        async with async_session() as session:
            stmt = (
                delete(Tag)
            )
            await session.execute(stmt)
            await session.commit()
        # print(self.name)
        # xml_parser = ElementTree.XMLPullParser(['end'])
        # start_points = 0
        # async for line in self.async_read_archive_generator(self.tags_archive_path, TAGS_FILENAME):
        #     xml_parser.feed(line)
        #     for dict_xml_tag in xml_parser.read_events():
        #         xml_tag: ElementTree.Element = dict_xml_tag[1]
        #         if xml_tag.tag == "row":
        #             await Tag.update_or_create(id=xml_tag.attrib.get('Id'),
        #                                        name=xml_tag.attrib.get("TagName"),
        #                                        count_usage=xml_tag.attrib.get("Count"))
        #     start_points += len(line)
        # pass

    def __init__(self, archive_path: List[str] | str):
        if not is_7zfile(archive_path):
            raise ValueError(f"Not a archvie: {archive_path}")

        with SevenZipFile(archive_path, 'r') as archive_read:
            all_archive_files = archive_read.getnames()

        obj_path = Path(archive_path)
        self.name = obj_path.name[:-3]

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
