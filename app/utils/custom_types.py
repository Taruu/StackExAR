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

from sqlalchemy import select, delete, insert, func, update, and_
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

    async def is_indexed(self, name: str, hash_file: str) -> [bool | None]:
        stmt = (
            select(ConfigValues)
            .where(
                and_(
                    ConfigValues.hash_file == hash_file,
                    ConfigValues.name == name,
                )
            )
            .limit(1)
        )
        result = await self.session.scalar(stmt)
        if not result:
            return None
        return result.index_done

    async def set_index(self, name: str, hash_file: str, index=False):
        stmt = (
            select(ConfigValues)
            .where(
                and_(
                    ConfigValues.hash_file == hash_file,
                    ConfigValues.name == name,
                )
            )
            .limit(1)
        )
        result = await self.session.scalar(stmt)
        if result:
            result.index_done = True
            return
        stmt = insert(ConfigValues).values(
            [{"name": name, "hash_file": hash_file, "index_done": index}]
        )
        await self.session.execute(stmt)

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

    async def get_tags(self, offset: int, limit: int):
        stmt = select(Tag).offset(offset).limit(limit)
        return await self.session.scalars(stmt)

    async def get_post(self, post_id: int):
        return await self.session.get(QuestionPost, post_id)

    async def get_posts(self, offset: int, limit: int, tags: List[str]):
        stmt = select(QuestionPost).offset(offset).limit(limit)
        for tag in tags:
            stmt = stmt.where(QuestionPost.tags.any(Tag.name == tag))
        res = await self.session.scalars(stmt)
        return list(res)

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
        status = await self.database_worker.is_indexed(
            "posts", self.post_archive_reader.str_archive_md5
        )
        if status is None:
            await self.database_worker.clear_posts()
            await self.database_worker.set_index(
                "posts", self.post_archive_reader.str_archive_md5, False
            )
            await self.database_worker.commit()
        elif status is True:
            await self.database_worker.close()
            return

        global_count, start_bytes = await self.database_worker.get_cursor_start()
        post_count = 0
        last_id = 0
        temp_list_posts, temp_list_answers, temp_tags_to_post = [], [], []

        # Get length
        async for cursor, line in self.post_archive_reader.readlines(
            self.post_archive_reader.size - (512 * 1024), 0
        ):
            try:
                xml_tag = XmlElementTree.fromstring(line)
            except Exception:
                continue
            if xml_tag.tag == "row":
                last_id = xml_tag.attrib.get("Id")

        # start index posts
        async for cursor, line in self.post_archive_reader.readlines(
            start_bytes=start_bytes
        ):
            try:
                xml_tag = XmlElementTree.fromstring(line)
                if xml_tag.tag != "row":
                    continue
            except Exception:
                continue

            line_length = len(line)
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

            if post_count >= 4096:
                await self.database_worker.insert_post_data(
                    temp_list_posts, temp_list_answers, temp_tags_to_post
                )
                await self.database_worker.commit()
                global_count += post_count

                temp_list_posts, temp_list_answers, temp_tags_to_post = [], [], []

                logger.info(
                    f"index {post_count} posts: {self.name} {global_count}/{last_id}"
                )
                post_count = 0

        await self.database_worker.insert_post_data(
            temp_list_posts, temp_list_answers, temp_tags_to_post
        )
        await self.database_worker.set_index(
            "posts", self.post_archive_reader.str_archive_md5, True
        )
        await self.database_worker.commit()
        await self.database_worker.close()
        global_count += post_count
        logger.info(f"end index {self.name} {global_count}/{last_id} indexed")

    async def index_tags(self):
        """Index all tags in posts"""
        logger.info(f"start index tags: {self.name}")
        await self.database_worker.init_session()

        if not await self.database_worker.is_indexed(
            "tags", self.tags_archive_reader.str_archive_md5
        ):
            await self.database_worker.clear_tags()
            await self.database_worker.clear_posts()
            await self.database_worker.commit()
        else:
            await self.database_worker.close()
            logger.info(f"tags already indexed : {self.name}")
            return

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
        await self.database_worker.set_index(
            "tags", self.tags_archive_reader.str_archive_md5, True
        )
        await self.database_worker.commit()

        logger.info(f"end index tags: {self.name}")

        return True

    async def tags_list(self, offset=0, limit=100):
        await self.database_worker.init_session()
        items = await self.database_worker.get_tags(offset, limit)
        tag_list = {tag.name: {"count_usage": tag.count_usage} for tag in items}
        await self.database_worker.close()
        return tag_list

    async def get_post(self, post_id: int):
        # TODO remade on upper level?
        await self.database_worker.init_session()
        post_item = await self.database_worker.get_post(post_id)
        if not post_item:
            await self.database_worker.close()
            return None
        answer_item_list: List[AnswerPost] = (
            await post_item.awaitable_attrs.answer_posts
        )
        tags: List[Tag] = await post_item.awaitable_attrs.tags
        await self.database_worker.close()

        question_text = await self.post_archive_reader.get(
            post_item.start, post_item.length
        )
        answer_texts = [
            await self.post_archive_reader.get(answer_item.start, answer_item.length)
            for answer_item in answer_item_list
        ]

        # Not need try become we use index tables

        question_xml_tag = XmlElementTree.fromstring(question_text).attrib
        temp_xml_answers: List[dict] = [
            XmlElementTree.fromstring(answer_text).attrib
            for answer_text in answer_texts
        ]

        dict_answers = {
            int(xml_answer.get("Id")): {
                "creation_date": xml_answer.get("CreationDate"),
                "score": xml_answer.get("Score"),
                "last_activity_date": xml_answer.get("LastActivityDate"),
                "body": xml_answer.get("Body"),
            }
            for xml_answer in temp_xml_answers
        }

        fetched_post = {
            "id": question_xml_tag.get("Id"),
            "creation_date": question_xml_tag.get("CreationDate"),
            "last_edit_date": question_xml_tag.get("LastEditDate"),
            "last_activity_date": question_xml_tag.get("LastActivityDate"),
            "title": question_xml_tag.get("Title"),
            "body": question_xml_tag.get("Body"),
            "tags": [tag.name for tag in tags],
            "score": question_xml_tag.get("Score"),
            "answers": dict_answers,
        }

        if post_item.accepted_answer_id:
            answer = fetched_post["answers"].pop(post_item.accepted_answer_id)
            fetched_post.update({"accepted_answer": answer})

        return fetched_post

    async def query_posts(self, offset=0, limit=10, tags: List[str] = []):
        # TODO make custom types
        fetched_posts = {}
        answers_items = []
        await self.database_worker.init_session()
        post_items = await self.database_worker.get_posts(offset, limit, tags)
        if not post_items:
            await self.database_worker.close()
            return None
        for post_item in post_items:
            tags: List[Tag] = await post_item.awaitable_attrs.tags
            fetched_posts.update(
                {post_item.id: {"tags": [tag.name for tag in tags], "answers": {}}}
            )
            answers_items.extend(await post_item.awaitable_attrs.answer_posts)
        await self.database_worker.close()

        queue_list: List[QuestionPost | AnswerPost] = []
        queue_list.extend(post_items)
        queue_list.extend(answers_items)
        queue_list.sort(key=lambda item: item.start)

        for item in queue_list:
            line_text = await self.post_archive_reader.get(item.start, item.length)

            dict_item: dict = XmlElementTree.fromstring(line_text).attrib
            type_id = int(dict_item.get("PostTypeId"))
            if type_id == 1:
                post_id = int(dict_item.get("Id"))
                fetched_posts[post_id].update(
                    {
                        "creation_date": dict_item.get("CreationDate"),
                        "last_edit_date": dict_item.get("LastEditDate"),
                        "last_activity_date": dict_item.get("LastActivityDate"),
                        "title": dict_item.get("Title"),
                        "body": dict_item.get("Body"),
                        "score": dict_item.get("Score"),
                    }
                )
            elif type_id == 2:
                post_id = int(dict_item.get("ParentId"))
                answer_id = int(dict_item.get("Id"))
                fetched_posts[post_id]["answers"].update(
                    {
                        answer_id: {
                            "creation_date": dict_item.get("CreationDate"),
                            "score": dict_item.get("Score"),
                            "last_activity_date": dict_item.get("LastActivityDate"),
                            "body": dict_item.get("Body"),
                        }
                    }
                )
            else:
                continue
        for post_item in post_items:
            if post_item.accepted_answer_id:
                answer = fetched_posts[post_item.id]["answers"].pop(
                    post_item.accepted_answer_id
                )
                fetched_posts[post_item.id].update({"accepted_answer": answer})

        return fetched_posts
