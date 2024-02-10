from sqlalchemy import create_engine

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.schema import MetaData

from app.database.models import Base


async def get_database_session(path: str):
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}", echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    return async_sessionmaker(engine)
