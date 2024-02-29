import asyncio
import hashlib
import multiprocessing
import queue
import re
import sys
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List
from enum import Enum
from pathlib import Path

import xml.etree.ElementTree as XmlElementTree
import time

from sqlalchemy import select, delete, insert, func, update
from sqlalchemy.ext.asyncio import AsyncSession

from .archive_extra import get_archive_filenames, ArchiveFileReader
from ..database.function import get_database_session, database_session_makers
from ..database.models import (
    Tag,
    QuestionPost,
    AnswerPost,
    TagToPost,
    ConfigValues,
    Base,
)

from py7zr import SevenZipFile, is_7zfile

from .. import global_app  # monkey code
from ..utils import config
from loguru import logger

POSTS_FILENAME = "Posts.xml"
TAGS_FILENAME = "Tags.xml"

process_pools = ThreadPoolExecutor(max_workers=config.settings.count_workers)
import sys
from types import ModuleType, FunctionType
from gc import get_referents

# Custom objects know their class.
# Function objects seem to know way too much, including modules.
# Exclude modules as well.
BLACKLIST = type, ModuleType, FunctionType


def getsize(obj):
    """sum size of object & members."""
    if isinstance(obj, BLACKLIST):
        raise TypeError("getsize() does not take argument of type: " + str(type(obj)))
    seen_ids = set()
    size = 0
    objects = [obj]
    while objects:
        need_referents = []
        for obj in objects:
            if not isinstance(obj, BLACKLIST) and id(obj) not in seen_ids:
                seen_ids.add(id(obj))
                size += sys.getsizeof(obj)
                need_referents.append(obj)
        objects = get_referents(*need_referents)
    return size


class File(Enum):
    POST_FILE = 1
    TAGS_FILE = 2


class DatabaseWorker:

    def __init__(self, database_path: str):
        self.database_path = database_path
        self.async_sessionmaker = None
        self.session: AsyncSession = None

    async def init_session(self):
        if not self.async_sessionmaker:
            self.async_sessionmaker = await get_database_session(self.database_path)

        self.session = self.async_sessionmaker()

    async def get_cursor_start(self):
        quest_count = await self.session.scalar(
            select(func.count()).select_from(QuestionPost)
        )
        if quest_count <= 100_000:
            await self.clear_posts()
            return 0, 0

        quest_item = await self.session.scalar(
            select(QuestionPost).order_by(QuestionPost.id.desc()).limit(1)
        )
        answer_item = await self.session.scalar(
            select(AnswerPost).order_by(AnswerPost.id.desc()).limit(1)
        )

        if quest_item.id > answer_item.id:
            item_to_continue = quest_item
        else:
            item_to_continue = answer_item

        return item_to_continue.id, item_to_continue.start + item_to_continue.length

    async def insert_post_data(
        self, question_posts: list, answers_posts: list, tags_to_post: list
    ):
        await self.session.execute(insert(QuestionPost).values(question_posts))
        await self.session.execute(insert(AnswerPost).values(answers_posts))
        await self.session.execute(insert(TagToPost).values(tags_to_post))

    async def clear_posts(self):
        await self.session.execute(delete(AnswerPost))
        await self.session.execute(delete(QuestionPost))
        await self.session.execute(delete(TagToPost))
        await self.session.commit()

    async def clear_tags(self):
        await self.session.execute(delete(Tag))
        await self.session.execute(delete(TagToPost))
        await self.session.commit()

    async def insert_tags(self, tags_list_to_add):
        await self.session.execute(insert(Tag).values(tags_list_to_add))

    async def get_tags_ids(self, tags_list):
        stmt = select(Tag).where(Tag.name.in_(tags_list))
        result = await self.session.scalars(stmt)
        return result

    async def flush(self):
        return await self.session.flush()

    async def commit(self):
        return await self.session.commit()

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None


class ArchiveWorker:
    def __init__(self):
        pass


class DataArchiveReader:
    """Data archive object"""

    name = None
    database_path = None

    post_archive_reader = None
    tags_archive_reader = None

    post_archive_path = None
    tags_archive_path = None

    def __init__(self, archive_path: List[str] | str):
        if not is_7zfile(archive_path):
            raise ValueError(f"Not a archvie: {archive_path}")

        all_archive_files = get_archive_filenames(archive_path)

        obj_path = Path(archive_path)
        self.name = obj_path.name[:-3]

        if (POSTS_FILENAME in all_archive_files) and (
            TAGS_FILENAME in all_archive_files
        ):
            self.post_archive_reader = ArchiveFileReader(archive_path, POSTS_FILENAME)
            self.tags_archive_reader = ArchiveFileReader(archive_path, TAGS_FILENAME)

        elif (POSTS_FILENAME in all_archive_files) and ("-" in obj_path.name):
            # Big archive use '-' for separate
            archive_name = obj_path.name.split("-")[0]
            tags_archive_path = f"{obj_path.parent}/{archive_name}-Tags.7z"

            with SevenZipFile(tags_archive_path, "r") as temp_read:
                temp_all_archive_files = temp_read.getnames()

            if TAGS_FILENAME in temp_all_archive_files:
                self.post_archive_reader = ArchiveFileReader(
                    archive_path, POSTS_FILENAME
                )
                self.tags_archive_reader = ArchiveFileReader(
                    tags_archive_path, TAGS_FILENAME
                )
            else:
                raise ValueError(f"{tags_archive_path} not exist")
        else:
            raise ValueError(f"Not correct archive: {archive_path}")

        self.database_worker = DatabaseWorker(
            f"{Path(archive_path).parent}/{self.name}.db"
        )

    async def index_posts(self):
        logger.info(f"start index posts: {self.name}")
        # clean table
        await self.database_worker.init_session()
        global_count, start_bytes = await self.database_worker.get_cursor_start()
        post_count = 0

        temp_list_posts = []
        temp_list_answers = []
        temp_tags_to_post = []

        async for cursor, line in self.post_archive_reader.readlines(
            start_bytes=start_bytes
        ):

            line_length = len(line)
            try:
                xml_tag = XmlElementTree.fromstring(line)
            except Exception:
                continue

            if xml_tag.tag != "row":
                continue

            new_tag = {
                "id": xml_tag.attrib["Id"],
                "start": cursor,
                "length": line_length,
                "score": int(xml_tag.attrib.get("Score")),
            }

            if xml_tag.attrib.get("PostTypeId") == "1":
                if xml_tag.attrib.get("Tags"):
                    tags = re.findall(r"<(.+?)>", xml_tag.attrib.get("Tags"))

                tags_ids = await self.database_worker.get_tags_ids(tags)
                temp_tags_to_post.extend(
                    [
                        {"tag_id": tag.id, "post_id": new_tag.get("id")}
                        for tag in tags_ids
                    ]
                )

                temp_accepted_answer_id = xml_tag.attrib.get("AcceptedAnswerId")
                new_tag.update(
                    {
                        "accepted_answer_id": (
                            int(temp_accepted_answer_id)
                            if temp_accepted_answer_id
                            else None
                        ),
                    }
                )
                temp_list_posts.append(new_tag)

            elif xml_tag.attrib.get("PostTypeId") == "2":
                temp_question_post_id = xml_tag.attrib.get("ParentId")
                new_tag.update(
                    {
                        "question_post_id": (
                            int(temp_question_post_id)
                            if temp_question_post_id
                            else None
                        ),
                    }
                )
                temp_list_answers.append(new_tag)

            post_count += 1
            del xml_tag

            if post_count >= 4096:
                await self.database_worker.insert_post_data(
                    temp_list_posts, temp_list_answers, temp_tags_to_post
                )
                await self.database_worker.commit()
                global_count += post_count
                logger.info(
                    f"index {post_count} posts: {self.name} {global_count}/77,031,708"
                )
                post_count = 0
                temp_list_posts.clear()
                temp_list_answers.clear()
                temp_tags_to_post.clear()

        await self.database_worker.insert_post_data(
            temp_list_posts, temp_list_answers, temp_tags_to_post
        )
        await self.database_worker.commit()
        await self.database_worker.close()

        global_count += post_count
        logger.info(
            f"END INDEX {post_count} posts: {self.name} {global_count}/77,031,708"
        )
        global_count += post_count
        logger.info(f"index 1024 posts: {self.name} {global_count}/24,090,793")
        logger.info(f"finish_index: {self.name}")

    async def index_tags(self):
        """Index all tags in posts"""
        logger.info(f"start index tags: {self.name}")
        await self.database_worker.init_session()
        await self.database_worker.clear_tags()

        count = 0
        insert_items = []
        async for cursor, line in self.tags_archive_reader.readlines():
            try:
                xml_tag = XmlElementTree.fromstring(line)
            except Exception:
                continue

            if xml_tag.tag != "row":
                continue

            insert_items.append(
                {
                    "id": xml_tag.attrib["Id"],
                    "name": xml_tag.attrib["TagName"],
                    "count_usage": xml_tag.attrib["Count"],
                }
            )
            count += 1

            if count >= 1000:
                await self.database_worker.insert_tags(insert_items)
                await self.database_worker.commit()
                count = 0
                insert_items = []
        await self.database_worker.insert_tags(insert_items)
        await self.database_worker.commit()
        await self.database_worker.close()
        logger.info(f"end index tags: {self.name}")

        return True

    async def tags_list(self, offset=0, limit=100):
        offset = offset if offset > 0 else 1
        async_sessionmaker = await get_database_session(self.database_path)
        async with async_sessionmaker() as session:
            stmt = (
                select(Tag.__table__)
                .order_by(Tag.__table__.c.count_usage.desc())
                .offset(offset)
                .limit(limit)
            )
            result_orm = await session.execute(stmt)
        result = {}
        for item in result_orm:
            result.update(
                {item.id: {"name": item.name, "count_usage": item.count_usage}}
            )
        return result

    def get_post(self, post_id):
        pass

    def query_posts(self, offset=0, limit=10, tags=List[str | int]):

        pass
