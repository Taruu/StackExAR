import asyncio
import glob
from pathlib import Path
from typing import Annotated, List

from ..utils.archive import get_archive_reader
from ..utils.config import settings
from fastapi import APIRouter, Depends, Query
from ..utils.custom_types import DataArchiveReader

router = APIRouter(prefix="/archive")


@router.get("/load")
async def load(
    archive_reader: Annotated[DataArchiveReader, Depends(get_archive_reader)],
):
    """## load archive in cache"""
    return


@router.get("/load_all")
async def load_all():
    """## load archives in cache"""
    archive_list = glob.glob(f"{settings.archive_folder}/*.com.7z")
    archive_list.extend(glob.glob(f"{settings.archive_folder}/*-Posts.7z"))
    data_archives_list = [Path(path).name for path in archive_list]
    for name in data_archives_list:
        get_archive_reader(name)
    return


@router.get("/tags")
async def tags_list(
    archive_reader: Annotated[DataArchiveReader, Depends(get_archive_reader)],
    offset: int,
    limit=100,
):
    """## get tags for archive"""
    tag_list = await archive_reader.tags_list(offset, limit)
    return tag_list


@router.get("/get/post")
async def get_post(
    archive_reader: Annotated[DataArchiveReader, Depends(get_archive_reader)],
    post_id: int,
):
    """## get post by id"""
    post = await archive_reader.get_post(post_id)
    return post


@router.get("/get/posts")
async def get_posts(
    archive_reader: Annotated[DataArchiveReader, Depends(get_archive_reader)],
    offset: int,
    tags: List[str] = Query([]),
    limit=100,
):
    """## get post with filters"""
    return await archive_reader.query_posts(offset, limit, tags)
