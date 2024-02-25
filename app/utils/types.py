import asyncio
import multiprocessing
import queue
import re
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List
from enum import Enum
from pathlib import Path

import xml.etree.ElementTree as XmlElementTree
import time

from sqlalchemy import select, delete, insert

from .archive_extra import get_archive_reader
from ..database.function import get_database_session
from ..database.models import Tag, QuestionPost, AnswerPost, post_tags, ConfigValues

from py7zr import SevenZipFile, is_7zfile

from .. import global_app  # monkey code
from ..utils import config
from loguru import logger

POSTS_FILENAME = "Posts.xml"
TAGS_FILENAME = "Tags.xml"

process_pools = ThreadPoolExecutor(max_workers=config.settings.count_workers)


class File(Enum):
    POST_FILE = 1
    TAGS_FILE = 2


class DataArchiveReader:
    """Data archive object"""
    name = None

    post_archive_reader = None
    tags_archive_reader = None
    database_path = None

    def __init__(self, archive_path: List[str] | str):
        if not is_7zfile(archive_path):
            raise ValueError(f"Not a archvie: {archive_path}")

        with SevenZipFile(archive_path, 'r') as archive_read:
            all_archive_files = archive_read.getnames()

        obj_path = Path(archive_path)
        self.name = obj_path.name[:-3]

        if (POSTS_FILENAME in all_archive_files) and (TAGS_FILENAME in all_archive_files):

            self.post_archive_reader = get_archive_reader(archive_path, POSTS_FILENAME)
            self.tags_archive_reader = get_archive_reader(archive_path, TAGS_FILENAME)

        elif (POSTS_FILENAME in all_archive_files) and ("-" in obj_path.name):
            # Big archive use '-' for separate
            archive_name = obj_path.name.split('-')[0]
            tags_archive_path = f"{obj_path.parent}/{archive_name}-Tags.7z"

            with SevenZipFile(tags_archive_path, 'r') as temp_read:
                temp_all_archive_files = temp_read.getnames()

            if TAGS_FILENAME in temp_all_archive_files:
                self.post_archive_reader = get_archive_reader(archive_path, POSTS_FILENAME)
                self.tags_archive_reader = get_archive_reader(tags_archive_path, TAGS_FILENAME)
            else:
                raise ValueError(f"{tags_archive_path} not exist")
        else:
            raise ValueError(f"Not correct archive: {archive_path}")
        self.database_path = f"{Path(archive_path).parent}/{self.name}.db"

    def _sync_generator(self, file: File, bytes_queue: queue.Queue):
        """Custom sync reader form files"""
        if file.POST_FILE == file:
            current_reader = self.post_archive_reader
        else:
            current_reader = self.tags_archive_reader

        current_reader.seek(0)
        data_buffer = current_reader.read(512 * 1024)
        buffer_last = b""
        while data_buffer != b"":
            data_buffer = buffer_last + data_buffer
            data_lines = data_buffer.rsplit(b'\r\n')
            for line in data_lines:
                if line.endswith(b">"):
                    bytes_queue.put(line + b'\r\n')
                else:
                    buffer_last = line
            del data_lines
            data_buffer = current_reader.read(512 * 1024)
        return

    def _sync_address(self, file: File, start: int, length: int):
        """Take row by address"""
        if file.POST_FILE == file:
            self.post_archive_reader.seek(start)
            return self.post_archive_reader.read(length)
        else:
            self.tags_archive_reader.seek(start)
            return self.tags_archive_reader.read(length)

    async def async_read_post(self, file: File, start: int, length: int):
        loop = asyncio.get_running_loop()
        requested_bytes = await loop.run_in_executor(global_app.app.process_pools, self._sync_address, file,
                                                     start, length)

        tag_obj = XmlElementTree.fromstring(requested_bytes.decode())
        if tag_obj.tag == "row":
            print(tag_obj)
        return

    async def async_readlines_generator(self, file: File):
        print("ASYNC", file)
        cursor_pos = 0
        loop = asyncio.get_running_loop()
        m = multiprocessing.Manager()
        bytes_queue: queue.Queue = queue.Queue(8192)

        sync_future: asyncio.Future = loop.run_in_executor(
            global_app.app.process_pools, self._sync_generator, file, bytes_queue)

        print("READLINES", (not bytes_queue.empty()) or (not sync_future.done()))
        while (not bytes_queue.empty()) or (not sync_future.done()):
            try:
                value_temp: bytes = bytes_queue.get_nowait()
                yield cursor_pos, value_temp
                cursor_pos += len(value_temp)
                # logger.info(f"qsize {bytes_queue.qsize()}")
            except queue.Empty:
                await asyncio.sleep(0)

    async def index_posts(self):
        logger.info(f"start index posts: {self.name}")
        # clean table
        async_sessionmaker = await get_database_session(self.database_path)
        async with async_sessionmaker() as session:
            #session.execute(select(ConfigValues.__table__).where(ConfigValues.__table__.c.name == ""))
            await session.execute(delete(AnswerPost))
            await session.execute(delete(QuestionPost))
            await session.execute(delete(post_tags))
            await session.commit()

        xml_parser = XmlElementTree.XMLPullParser(['end'])

        post_count = 0
        global_count = 0
        temp_list_posts = []
        temp_list_answers = []
        temp_tags_to_post = []

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

                async with async_sessionmaker() as session:
                    stmt = select(Tag.__table__).where(Tag.__table__.c.name.in_(tags))
                    tags_values = await session.execute(stmt)

                temp_tags_to_post.extend([{'tag_id': tag.id, "post_id": new_tag.get("id")} for tag in tags_values])

                temp_accepted_answer_id = xml_tag.attrib.get("AcceptedAnswerId")
                new_tag.update({
                    "accepted_answer_id": int(temp_accepted_answer_id) if temp_accepted_answer_id else None,
                })
                temp_list_posts.append(new_tag)

            elif xml_tag.attrib.get('PostTypeId') == '2':
                temp_question_post_id = xml_tag.attrib.get("ParentId")
                new_tag.update({
                    "question_post_id": int(temp_question_post_id) if temp_question_post_id else None,
                })
                temp_list_answers.append(new_tag)

            post_count += 1

            xml_tag.clear()
            del xml_tag

            if post_count >= 4096:
                async with async_sessionmaker() as session:
                    await session.execute(insert(QuestionPost).values(temp_list_posts))
                    await session.execute(insert(AnswerPost).values(temp_list_answers))
                    await session.execute(insert(post_tags).values(temp_tags_to_post))
                    await session.flush()

                global_count += post_count
                logger.info(f"index {post_count} posts: {self.name} {global_count}/77,031,708")
                post_count = 0
                temp_list_posts.clear()
                temp_list_answers.clear()
                temp_tags_to_post.clear()
                # temp_list_posts, temp_list_answers, temp_tags_to_post = [], [], []
        async with async_sessionmaker() as session:
            await session.execute(insert(QuestionPost).values(temp_list_posts))
            await session.execute(insert(AnswerPost).values(temp_list_answers))
            await session.execute(insert(post_tags).values(temp_tags_to_post))
            await session.commit()

        global_count += post_count
        logger.info(f"END INDEX {post_count} posts: {self.name} {global_count}/77,031,708")
        global_count += post_count
        logger.info(f"index 1024 posts: {self.name} {global_count}/24,090,793")
        logger.info(f"finish_index: {self.name}")

    async def index_tags(self):
        """Index all tags in posts"""
        logger.info(f"start index tags: {self.name}")
        async_sessionmaker = await get_database_session(self.database_path)
        async with async_sessionmaker() as session:
            await session.execute(delete(Tag))
            await session.execute(delete(post_tags))
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
                async with async_sessionmaker() as session:
                    await session.execute(insert(Tag).values(temp_list_tags))
                    await session.commit()
                count = 0
                temp_list_tags = []
        async with async_sessionmaker() as session:
            await session.execute(insert(Tag).values(temp_list_tags))
            await session.commit()
        logger.info(f"end index tags: {self.name}")
        return True

    async def tags_list(self, offset=0, limit=100):
        offset = offset if offset > 0 else 1
        async_sessionmaker = await get_database_session(self.database_path)
        async with async_sessionmaker() as session:
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
