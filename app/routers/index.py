import asyncio
import glob
from pathlib import Path
from typing import Annotated

from loguru import logger

from ..utils.archive import get_archive_reader
from ..utils.config import settings
from fastapi import APIRouter, Depends
from ..utils.custom_types import DataArchiveReader

router = APIRouter(prefix="/indexing")





@router.put("/process")
async def send(
    archive_reader: Annotated[DataArchiveReader, Depends(get_archive_reader)]
):
    """## send archive to index"""
    logger.info(f"start index {archive_reader.name} archive")
    await archive_reader.index_tags()
    await archive_reader.index_posts()
    logger.info(f"end index {archive_reader.name} archive")
    return True


@router.put("/process/all")
async def send_all():
    """## send all archives to index"""
    logger.info("start index all archives")
    archive_list = glob.glob(f"{settings.archive_folder}/*.com.7z")
    archive_list.extend(glob.glob(f"{settings.archive_folder}/*-Posts.7z"))
    logger.info("start index all tags")

    data_archive_readers = [
        get_archive_reader(Path(path).name) for path in archive_list
    ]

    async with asyncio.TaskGroup() as tg:
        for data_archive in data_archive_readers:
            tg.create_task(data_archive.index_tags())
    async with asyncio.TaskGroup() as tg:
        for data_archive in data_archive_readers:
            tg.create_task(data_archive.index_posts())
