import asyncio
import io
import multiprocessing
import queue
import time
from collections import deque
from queue import Queue
from typing import List
from enum import Enum
from pathlib import Path
import functools
import xml.etree.ElementTree as XmlElementTree
import time
from sqlalchemy import select, delete, update, insert

from ..database.function import get_database_session
from ..database.models import Tag, QuestionPost, Base, AnswerPost

from asyncio import get_event_loop, gather, sleep

from py7zr import SevenZipFile, is_7zfile
from tortoise import Tortoise

from .. import global_app
from ..utils import config

POSTS_FILENAME = "Posts.xml"
TAGS_FILENAME = "Tags.xml"


class MagicStepIO(io.FileIO):
    left_step_len = 0
    bytes_magic = b"BZh91AY&SY"

    def __init__(self, *args, **kwargs):
        super(MagicStepIO, self).__init__(*args, **kwargs)

        values = super().read(1024)
        step = values.find(self.bytes_magic)
        self.left_step_len = step
        self.seek(0)

    def fileno(self) -> int:
        return -1

    def tell(self):
        # print("tell", super().tell() - self.left_step_len)
        return super().tell() - self.left_step_len

    def seek(self, __offset: int, __whence: int = 0) -> int:
        # print("seek", __offset, __whence)
        if __whence == 0:
            return super().seek(__offset + self.left_step_len, __whence) - self.left_step_len
        else:
            return super().seek(__offset, __whence) - self.left_step_len

    def read(self, __size: int = ...) -> bytes:
        # print("read", __size, self.tell(), super().tell())
        temp_bytes = super().read(__size)
        return temp_bytes


class AsyncFileReader:
    def __init__(self):
        pass

    def read(self, bytes):
        pass


class DataArchiveReader:
    """Data archive object"""
    name = None

    post_archive_path = None
    tags_archive_path = None
    database_path = None

    @property
    def archive_post_reader(self):
        if self.post_archive_path != self.tags_archive_path:
            return MagicStepIO(self.post_archive_path, mode="r")
        else:
            file = SevenZipFile(self.post_archive_path, 'r')
            return file.read(targets=[POSTS_FILENAME]).get(POSTS_FILENAME)

    def __del__(self):
        if self.archive_post_reader:
            self.archive_post_reader.close()

    def _sync_read_archive_by_address(self, start: int, length: int):
        self.archive_post_reader.seek(start)
        request_bytes = self.archive_post_reader.read(length)
        return request_bytes

    def _sync_read_archive(self, archive_path, filename, bytes_queue):
        self.archive_post_reader.seek(0)
        for line in self.archive_post_reader.readlines():
            bytes_queue.put_nowait(line)
        return True

    async def async_read_post_archive_by_address(self, start: int, length: int):
        loop = asyncio.get_running_loop()
        requested_bytes = await loop.run_in_executor(global_app.app.process_pools, self._sync_read_archive_by_address,
                                                     start, length)
        tag_obj = XmlElementTree.fromstring(requested_bytes.decode())
        if tag_obj.tag == "row":
            print(tag_obj)
        return

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

    async def index_posts(self):
        time_start = time.time()

        # clean table
        async_session = await get_database_session(self.database_path)
        async with async_session() as session:
            await session.execute(delete(AnswerPost))
            await session.execute(delete(QuestionPost))

            await session.commit()

        xml_parser = XmlElementTree.XMLPullParser(['end'])
        cursor_pos = 0
        async for line in self.async_read_archive_generator(self.post_archive_path, POSTS_FILENAME):

            xml_parser.feed(line)

            for dict_xml_tag in xml_parser.read_events():
                xml_tag: XmlElementTree.Element = dict_xml_tag[1]

                if xml_tag.tag == "row":
                    print(xml_tag.attrib["Id"])
                    pass

            cursor_pos += len(line)
            line = None
        else:
            print(time.time() - time_start, self.post_archive_path, POSTS_FILENAME, cursor_pos)

    async def index_tags(self):
        """Index all tags in posts"""
        async_session = await get_database_session(self.database_path)
        async with async_session() as session:
            stmt = (
                delete(Tag)
            )
            await session.execute(stmt)
            await session.commit()

        async with async_session() as session:
            xml_parser = XmlElementTree.XMLPullParser(['end'])

            count = 0
            temp_list_tags = []
            async for line in self.async_read_archive_generator(self.tags_archive_path, TAGS_FILENAME):
                xml_parser.feed(line)

                for dict_xml_tag in xml_parser.read_events():
                    xml_tag: XmlElementTree.Element = dict_xml_tag[1]

                    if xml_tag.tag == "row":
                        temp_list_tags.append({
                            "id": xml_tag.attrib['Id'],
                            "name": xml_tag.attrib['TagName'],
                            "count_usage": xml_tag.attrib['Count']
                        })
                        count += 1

                if count >= 1000:
                    stmt = (
                        insert(Tag).values(temp_list_tags)
                    )

                    await session.execute(stmt)
                    await session.flush()

                    count = 0
                    temp_list_tags = []

            await session.commit()

    def tags_list(self, offset=0, limit=100):
        pass

    def get_post(self, post_id):
        pass

    def query_posts(self, offset=0, limit=10, tags=List[str | int]):

        pass

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

        self.database_path = f"{Path(self.post_archive_path).parent}/{self.name}.db"
