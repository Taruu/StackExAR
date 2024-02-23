from sqlalchemy import create_engine

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.schema import MetaData

from app.database.models import Base
from .. import global_app  # monkey code

from sqlalchemy import orm

mapper_registry = orm.registry()

database_session_makers = {}


async def get_database_session(path: str):
    async_sessionmaker_obj = database_session_makers.get(path)

    if async_sessionmaker_obj is not None:
        return async_sessionmaker_obj
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}", echo=True)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_sessionmaker_obj = async_sessionmaker(engine)
    database_session_makers.update({path: async_sessionmaker_obj})

    return async_sessionmaker_obj
