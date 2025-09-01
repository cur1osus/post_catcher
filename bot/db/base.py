from typing import Any

from sqlalchemy.dialects.sqlite import INTEGER
from sqlalchemy.ext.asyncio import (
    AsyncAttrs,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from bot.settings import Settings


class Base(DeclarativeBase, AsyncAttrs):
    id: Mapped[int] = mapped_column(INTEGER, primary_key=True, autoincrement=True)

    repr_cols_num = 3
    repr_cols = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }

    def __repr__(self) -> str:
        cols = [
            f"{col}={getattr(self, col)}"
            for idx, col in enumerate(self.__table__.columns.keys())
            if col in self.repr_cols or idx < self.repr_cols_num
        ]
        return f"<{self.__class__.__name__} {', '.join(cols)}>"


async def create_db_session_pool(
    se: Settings,
) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    engine: AsyncEngine = create_async_engine(
        url=se.mysql_dsn(),
        max_overflow=10,
        pool_size=100,
        pool_pre_ping=True,
        pool_recycle=900,
    )

    return engine, async_sessionmaker(engine, expire_on_commit=False)


async def close_db(engine: AsyncEngine) -> None:
    await engine.dispose()
