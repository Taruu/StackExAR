from fastapi import FastAPI
import concurrent.futures

from sqlalchemy.ext.asyncio import AsyncSession

from .routers import index, config, archive

app = FastAPI()
app.database_session_makers = {}
app.archives_readers = {}
app.process_pools = concurrent.futures.ProcessPoolExecutor(max_workers=config.config.settings.count_workers)

app.include_router(index.router)
app.include_router(config.router)
app.include_router(archive.router)
