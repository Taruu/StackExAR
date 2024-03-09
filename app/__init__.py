from fastapi import FastAPI

from .routers import index, config, archive

app = FastAPI()

app.include_router(index.router)
app.include_router(config.router)
app.include_router(archive.router)
