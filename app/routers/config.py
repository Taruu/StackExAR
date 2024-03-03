import os

from fastapi import APIRouter
from ..utils import config

router = APIRouter(prefix="/config")


@router.get("/")
async def get_current_config():
    """## get current config"""
    return config.settings.model_dump()
