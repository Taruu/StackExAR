from fastapi import APIRouter
from ..utils import archive
router = APIRouter(prefix="/indexing")


@router.put("/send")
async def send(name: str):
    pass


@router.put("/send_all")
async def send_all():
    picker = archive.ArchiveFeed()
    return await picker.list_sources()
    pass


@router.get("/in_processing")
async def get_in_processing():
    pass


@router.delete("/in_processing")
async def delete_from_processing():
    pass


@router.get("/status")
async def get_status(name: str):
    pass
