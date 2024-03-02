import asyncio
import glob
from pathlib import Path
from typing import Annotated

from loguru import logger

from ..utils.archive import get_archive_reader
from ..utils.config import settings
from fastapi import APIRouter, Depends
from ..utils import archive
from ..utils.custom_types import DataArchiveReader, File

router = APIRouter(prefix="/indexing")


@router.get("/list")
async def file_list():
    archive_list = glob.glob(f"{settings.archive_folder}/*.7z")
    data_archives_list = [Path(path).name for path in archive_list]
    return data_archives_list


@router.put("/send")
async def send(archive_reader: Annotated[DataArchiveReader, Depends(get_archive_reader)]):
    # await archive_reader.check_valid_database(File.POST_FILE)
    await archive_reader.index_tags()
    await archive_reader.index_posts()
    return True


@router.put("/send_all")
async def send_all():
    logger.info("start index all")
    archive_list = glob.glob(f"{settings.archive_folder}/*.com.7z")
    archive_list.extend(glob.glob(f"{settings.archive_folder}/*-Posts.7z"))
    logger.info("start index all tags")

    data_archive_readers = [get_archive_reader(Path(path).name) for path in archive_list]
    task_list = []
    async with asyncio.TaskGroup() as tg:
        for data_archive in data_archive_readers:
            tg.create_task(data_archive.index_tags())
    async with asyncio.TaskGroup() as tg:
        for data_archive in data_archive_readers:
            tg.create_task(data_archive.index_posts())
