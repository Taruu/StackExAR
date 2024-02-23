import asyncio
import io
import multiprocessing
import os
import pickle
import queue
import re
import time
from collections import deque
from queue import Queue
from typing import List
from enum import Enum
from pathlib import Path
import functools
import xml.etree.ElementTree as XmlElementTree
import time

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.testing import in_

from indexed_bzip2 import IndexedBzip2File
from sqlalchemy import select, delete, insert

from ..database.function import get_database_session
from ..database.models import Tag, QuestionPost, AnswerPost

from asyncio import get_event_loop, gather, sleep

from py7zr import SevenZipFile, is_7zfile
from tortoise import Tortoise

from .. import global_app  # monkey code
from ..utils import config
from loguru import logger
import indexed_bzip2 as ibz2

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


class ArchiveWorker:
    """File archive reader"""
    # TODO make as child
    indexed_bzip2_reader = False
    reader = None

    def __init__(self, path, filename=None):
        if "-" in path:
            # big file )))
            self.indexed_bzip2_reader = True

            path_obj = Path(path)
            block_offsets_index_path = f"{path_obj.parent}/{path_obj.name}-index.dat"

            file_custom = MagicStepIO(path, 'r')

            if not os.path.exists(block_offsets_index_path):
                # index to save blocks
                self.reader = ibz2.open(file_custom, parallelization=os.cpu_count())
                block_offsets = self.reader.block_offsets()
                with open(block_offsets_index_path, 'wb') as offsets_file:
                    pickle.dump(block_offsets, offsets_file)
                self.reader.close()
            else:
                with open(block_offsets_index_path, 'rb') as offsets_file:
                    block_offsets = pickle.load(offsets_file)

            self.reader = ibz2.open(file_custom, parallelization=os.cpu_count())
            self.reader.set_block_offsets(block_offsets)

        else:
            if not filename:
                raise ValueError("filename not set")
            zip_file = SevenZipFile(path, 'r')
            self.reader = zip_file.read(targets=[filename]).get(filename)


class File(Enum):
    POST_FILE = 1
    TAGS_FILE = 2


# TODO db session on class
class DataArchiveReader:
    """Data archive object"""
    name = None

    post_archive_reader = None
    tags_archive_reader = None
    database_path = None

    _session: AsyncSession = None

    async def get_session(self) -> AsyncSession:
        async_session = await get_database_session(self.database_path)
        async with async_session() as session:
            return session

    def _sync_generator(self, file: File, bytes_queue):
        if file.POST_FILE == file:
            self.post_archive_reader.seek(0)
            for line in self.post_archive_reader.readlines():
                bytes_queue.put_nowait(line)
        else:
            self.tags_archive_reader.seek(0)
            for line in self.tags_archive_reader.readlines():
                bytes_queue.put_nowait(line)

    def _sync_address(self, file: File, start: int, length: int):
        if file.POST_FILE == file:
            self.post_archive_reader.seek(start)
            return self.post_archive_reader.read(length)
        else:
            self.tags_archive_reader.seek(start)
            return self.tags_archive_reader.read(length)

    async def async_read_post_archive_by_address(self, file: File, start: int, length: int):
        loop = asyncio.get_running_loop()
        requested_bytes = await loop.run_in_executor(global_app.app.process_pools, self._sync_address, file,
                                                     start, length)
        tag_obj = XmlElementTree.fromstring(requested_bytes.decode())
        if tag_obj.tag == "row":
            print(tag_obj)
        return

    async def async_readlines_generator(self, file: File):
        cursor_pos = 0
        loop = asyncio.get_running_loop()
        m = multiprocessing.Manager()
        bytes_queue: multiprocessing.Queue = m.Queue()
        sync_future: asyncio.Future = loop.run_in_executor(
            global_app.app.process_pools, self._sync_generator, file, bytes_queue)

        while (not bytes_queue.empty()) or (not sync_future.done()):
            try:
                value_temp: bytes = bytes_queue.get_nowait()
                yield cursor_pos, value_temp
                cursor_pos += len(value_temp)
            except queue.Empty:
                await asyncio.sleep(0)

    async def index_posts(self):
        time_start = time.time()
        logger.info(f"start index posts: {self.name}")
        # clean table
        session = await self.get_session()
        await session.execute(delete(AnswerPost))
        await session.execute(delete(QuestionPost))

        await session.commit()

        xml_parser = XmlElementTree.XMLPullParser(['end'])

        post_count = 0

        temp_list_posts = []
        temp_list_answers = []
        async for cursor, line in self.async_readlines_generator(File.POST_FILE):

            xml_parser.feed(line)
            line_length = len(line)
            dict_xml_tag = list(xml_parser.read_events())

            if len(dict_xml_tag) < 1:
                continue

            xml_tag: XmlElementTree.Element = dict_xml_tag[0][1]

            if xml_tag.tag != "row":
                continue

            new_tag = {
                "id": xml_tag.attrib["Id"],
                "start": cursor,
                "length": line_length,
                "score": int(xml_tag.attrib.get("Score"))
            }

            if xml_tag.attrib.get('PostTypeId') == '1':
                if xml_tag.attrib.get('Tags'):
                    tags = re.findall(r"<(.+?)>", xml_tag.attrib.get("Tags"))
                temp_accepted_answer_id = xml_tag.attrib.get("AcceptedAnswerId")
                new_tag.update({
                    "accepted_answer_id": int(temp_accepted_answer_id) if temp_accepted_answer_id else None,
                })
                temp_list_posts.append(new_tag)
                post_count += 1
            elif xml_tag.attrib.get('PostTypeId') == '2':
                temp_question_post_id = xml_tag.attrib.get("ParentId")
                new_tag.update({
                    "question_post_id": int(temp_question_post_id) if temp_question_post_id else None,
                })
                temp_list_answers.append(new_tag)
                post_count += 1

            if post_count >= 1000:
                await session.execute(insert(QuestionPost).values(temp_list_posts))
                await session.execute(insert(AnswerPost).values(temp_list_answers))
                await session.flush()
                post_count = 0
                temp_list_posts = []
                temp_list_answers = []

            line = None
        else:
            print(time.time() - time_start, POSTS_FILENAME, )
        await session.execute(insert(QuestionPost).values(temp_list_posts))
        await session.execute(insert(AnswerPost).values(temp_list_answers))
        await session.commit()

    async def index_tags(self):
        """Index all tags in posts"""
        logger.info(f"start index tags: {self.name}")
        session = await self.get_session()
        stmt = (
            delete(Tag)
        )
        await session.execute(stmt)
        await session.commit()

        xml_parser = XmlElementTree.XMLPullParser(['end'])

        count = 0
        temp_list_tags = []
        async for cursor, line in self.async_readlines_generator(File.TAGS_FILE):
            xml_parser.feed(line)

            dict_xml_tag = list(xml_parser.read_events())

            if len(dict_xml_tag) < 1:
                continue
            xml_tag: XmlElementTree.Element = dict_xml_tag[0][1]

            if xml_tag.tag != "row":
                continue

            temp_list_tags.append({
                "id": xml_tag.attrib['Id'],
                "name": xml_tag.attrib['TagName'],
                "count_usage": xml_tag.attrib['Count']
            })
            count += 1

            if count >= 1000:
                await session.execute(insert(Tag).values(temp_list_tags))
                await session.flush()
                count = 0
                temp_list_tags = []

        await session.execute(insert(Tag).values(temp_list_tags))
        await session.commit()
        logger.info(f"end index tags: {self.name}")

        return True

    async def tags_list(self, offset=0, limit=100):
        offset = offset if offset > 0 else 1
        start = time.time()
        session = await self.get_session()
        print(time.time() - start)
        print(Tag.__table__.c)
        stmt = select(Tag.__table__).order_by(Tag.__table__.c.count_usage.desc()).offset(offset).limit(limit)
        result_orm = await session.execute(stmt)
        result = {}
        for item in result_orm:
            result.update({item.id: {"name": item.name, "count_usage": item.count_usage}})
        return result

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

            self.post_archive_reader = ArchiveWorker(archive_path, POSTS_FILENAME).reader
            self.tags_archive_reader = ArchiveWorker(archive_path, TAGS_FILENAME).reader

        elif (POSTS_FILENAME in all_archive_files) and ("-" in obj_path.name):
            # Big archive use '-' for separate
            archive_name = obj_path.name.split('-')[0]
            tags_archive_path = f"{obj_path.parent}/{archive_name}-Tags.7z"

            with SevenZipFile(tags_archive_path, 'r') as temp_read:
                temp_all_archive_files = temp_read.getnames()

            if TAGS_FILENAME in temp_all_archive_files:
                self.post_archive_reader = ArchiveWorker(archive_path, POSTS_FILENAME).reader
                self.tags_archive_reader = ArchiveWorker(archive_path, TAGS_FILENAME).reader
            else:
                raise ValueError(f"{tags_archive_path} not exist")
        else:
            raise ValueError(f"Not correct archive: {archive_path}")

        self.database_path = f"{Path(archive_path).parent}/{self.name}.db"
