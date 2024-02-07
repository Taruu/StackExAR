from fastapi import FastAPI

from .routers import index, config
import concurrent.futures

app = FastAPI()

app.process_pools = concurrent.futures.ProcessPoolExecutor(max_workers=config.config.settings.count_workers)

app.include_router(index.router)
app.include_router(config.router)
