from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.database.models import Base

database_session_makers = {}


async def get_database_session(path: str):
    if path in database_session_makers:
        return database_session_makers.get(path)
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}", echo=False)
    async_sessionmaker_obj = async_sessionmaker(engine)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    database_session_makers.update({path: async_sessionmaker_obj})

    return async_sessionmaker_obj
