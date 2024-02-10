from fastapi import FastAPI
import concurrent.futures

from sqlalchemy.ext.asyncio import AsyncSession

from .routers import index, config


app = FastAPI()

app.process_pools = concurrent.futures.ProcessPoolExecutor(max_workers=config.config.settings.count_workers)


app.include_router(index.router)
app.include_router(config.router)
