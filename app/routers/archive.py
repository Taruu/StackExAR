import asyncio
import glob
from pathlib import Path
from typing import Annotated, List

from loguru import logger

from ..utils.archive import get_archive_reader
from ..utils.config import settings
from fastapi import APIRouter, Depends, Query
from ..utils import archive
from ..utils.custom_types import DataArchiveReader

router = APIRouter(prefix="/archive")


@router.get("/tags")
async def tags_list(
    archive_reader: Annotated[DataArchiveReader, Depends(get_archive_reader)],
    offset: int,
    limit=100,
):
    tag_list = await archive_reader.tags_list(offset, limit)
    return tag_list


@router.get("/get/post")
async def get_post(
    archive_reader: Annotated[DataArchiveReader, Depends(get_archive_reader)],
    post_id: int,
):
    post = await archive_reader.get_post(post_id)
    return post


@router.get("/get/posts")
async def get_posts(
    archive_reader: Annotated[DataArchiveReader, Depends(get_archive_reader)],
    offset: int,
    tags: List[str] = Query([]),
    limit=100,
):

    return await archive_reader.query_posts(offset, limit, tags)
