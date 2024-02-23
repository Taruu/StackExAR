import asyncio
import glob
from pathlib import Path
from loguru import logger

from ..utils.archive import get_archive_reader
from ..utils.config import settings
from fastapi import APIRouter
from ..utils import archive
from ..utils.types import DataArchiveReader

router = APIRouter(prefix="/archive")


@router.get("/tags")
async def tags_list(name: str, offset: int, limit=100):
    archive_reader = get_archive_reader(name)
    tag_list = await archive_reader.tags_list(offset, limit)
    return tag_list
