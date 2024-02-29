import asyncio
import glob
from pathlib import Path
from typing import Annotated

from loguru import logger

from ..utils.archive import get_archive_reader
from ..utils.config import settings
from fastapi import APIRouter, Depends
from ..utils import archive
from ..utils.app_types import DataArchiveReader

router = APIRouter(prefix="/archive")


@router.get("/tags")
async def tags_list(
    archive_reader: Annotated[DataArchiveReader, Depends(get_archive_reader)],
    offset: int,
    limit=100,
):
    tag_list = await archive_reader.tags_list(offset, limit)
    return tag_list
