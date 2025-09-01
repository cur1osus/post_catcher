import logging
from typing import Final, cast
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telethon import TelegramClient
from telethon.tl.types import Channel

from bot.db.func import RedisStorage
from bot.db.models import Post
from bot.utils import fn

logger = logging.getLogger(__name__)
minute: Final[int] = 60


async def handling_difference_update_chanel(
    client: TelegramClient,
    sessionmaker: async_sessionmaker[AsyncSession],
    storage: RedisStorage,
) -> None:
    """
    Обрабатывает обновления из каналов и сохраняет данные в БД при необходимости.
    """
    async with sessionmaker() as session:
        channels_usernames: list[str] = await fn.get_channels_usernames(
            session, storage
        )
        if not channels_usernames:
            return

        for channel_username in channels_usernames:
            if not await fn.Sub.subscribe_to_channel(channel_username, client, storage):
                logger.info(f"Ошибка подписки на канал {channel_username}")
                continue

            r = await fn.safe_get_entity(client, channel_username)
            channel_entity: Channel = cast(Channel, r)
            if not channel_entity or not getattr(channel_entity, "broadcast", False):
                logger.info(f"Канал {channel_username} не является каналом")
                continue

            # Получаем разницу обновлений
            updates = await fn.get_difference_update_channel(
                client,
                storage,
                channel_entity,
                channel_username,
            )

            if not updates:
                logger.info(f"Нет обновлений для канала {channel_username}")
                continue

            # Обрабатываем каждое обновление
            for update in updates:
                msg_text = getattr(update, "message", "") or ""
                if not msg_text:
                    continue

                is_post_exists = await session.scalar(
                    select(Post).where(Post.message_id == update.id)
                )
                if is_post_exists:
                    continue

                post = Post(
                    message_id=update.id,
                    channel_username=channel_username,
                    content=msg_text,
                )
                session.add(post)

        await session.commit()
