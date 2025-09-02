import datetime
from typing import List

from sqlalchemy import (
    BigInteger,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.orm.properties import ForeignKey

from .base import Base


class Post(Base):
    __tablename__ = "posts"

    message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    channel_username: Mapped[str] = mapped_column(String(200), nullable=False)
    content: Mapped[str] = mapped_column(String(4096), nullable=False)


class UserDB(Base):
    __tablename__ = "users"

    user_id: Mapped[int] = mapped_column(BigInteger)
    name: Mapped[str] = mapped_column(String(100))
    username: Mapped[str] = mapped_column(String(100))

    is_admin: Mapped[bool] = mapped_column(nullable=False, default=False)
    receive_notifications: Mapped[bool] = mapped_column(nullable=False, default=False)

    date_sub_start: Mapped[datetime.datetime] = mapped_column(
        nullable=False,
        default=datetime.datetime.now(),
    )
    quantity_days_sub: Mapped[int] = mapped_column(default=0)

    triggers: Mapped[List["Trigger"]] = relationship(
        back_populates="user",
        lazy="selectin",
        cascade="all, delete-orphan",
    )
    ignores: Mapped[List["Ignore"]] = relationship(
        back_populates="user",
        lazy="selectin",
        cascade="all, delete-orphan",
    )


class Trigger(Base):
    __tablename__ = "triggers"

    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    user: Mapped[UserDB] = relationship(back_populates="triggers")

    content: Mapped[str] = mapped_column(String(100), nullable=False)


class Ignore(Base):
    __tablename__ = "ignores"

    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    user: Mapped[UserDB] = relationship(back_populates="ignores")

    content: Mapped[str] = mapped_column(String(100), nullable=False)


class Catcher(Base):
    __tablename__ = "catchers"

    name: Mapped[str] = mapped_column(String(50), nullable=True)
    phone: Mapped[str] = mapped_column(String(50))
    api_id: Mapped[int] = mapped_column(BigInteger)
    api_hash: Mapped[str] = mapped_column(String(100))
    path_session: Mapped[str] = mapped_column(String(100))

    is_connected: Mapped[bool] = mapped_column(default=False)


class MonitoringChannel(Base):
    __tablename__ = "monitoring_channels"

    channel_id: Mapped[int] = mapped_column(BigInteger, nullable=True)
    title: Mapped[str] = mapped_column(String(100), nullable=True)
    username: Mapped[str] = mapped_column(String(100))
