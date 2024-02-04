from fastapi import FastAPI

from .routers import index, config


app = FastAPI()


app.include_router(index.router)
app.include_router(config.router)
