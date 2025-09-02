import logging
from typing import Final, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telethon import TelegramClient
from telethon.tl.types import Message

from bot.db.func import RedisStorage
from bot.db.models import MonitoringChannel, Post
from bot.utils import TypeChatLike, fn

logger = logging.getLogger(__name__)
minute: Final[int] = 60


logger = logging.getLogger(__name__)


async def handle_updates_for_entities(
    client: TelegramClient,
    sessionmaker: async_sessionmaker[AsyncSession],
    storage: RedisStorage,
) -> None:
    """
    Унифицированная обработка обновлений для каналов и чатов.
    Автоматически определяет тип сущности и использует соответствующий механизм синхронизации.
    """
    async with sessionmaker() as session:
        channels: list[MonitoringChannel] = await fn.get_channels(session)

        if not channels:
            logger.info("Нет сущностей для обработки.")
            return

        for channel in channels:
            try:
                if channel.username.startswith("@") or channel.username.startswith("-"):
                    subscribed = await fn.Sub.subscribe_to_channel(
                        channel.username,
                        client,
                        storage,
                    )
                else:
                    subscribed = await fn.Sub.subscribe_by_invite_hash(
                        channel.username,
                        client,
                        storage,
                    )
                    new_username = await fn.Sub.fetch_id_from_chat_invite_request(
                        channel.username,
                        client,
                    )
                    if new_username:
                        channel.username = new_username
                    else:
                        logger.info(
                            "Не удалось получить id канала, помечаю канал для удаления"
                        )
                        await session.delete(channel)
                        continue

                if not subscribed:
                    logger.info(
                        f"Не удалось подписаться/присоединиться к {channel.username}"
                    )
                    continue

                entity = await fn.safe_get_entity(client, channel.username)
                if not entity:
                    logger.info(f"Сущность не найдена: {channel.username}")
                    continue

                # Приводим к Union-типу
                chat_like: TypeChatLike = cast(TypeChatLike, entity)

                # обновлеяем имя канала/чата если у него оно не указано еще
                if not channel.title:
                    channel.title = chat_like.title

                # Определяем тип сущности
                is_channel = getattr(chat_like, "broadcast", False)
                is_megagroup = getattr(chat_like, "megagroup", False)

                # Выбираем стратегию обработки
                if is_channel and not is_megagroup:
                    # Это обычный канал (broadcast)
                    updates = await fn.get_difference_update_channel(
                        client=client,
                        storage=storage,
                        channel=chat_like,
                        channel_username=channel.username,
                    )
                else:
                    # Это чат, супергруппа или мигрированная группа
                    updates = await fn.get_difference_update_chat(
                        client=client,
                        storage=storage,
                        chat=chat_like,
                        chat_username=channel.username,
                    )

                if not updates:
                    logger.info(f"Нет новых сообщений в {channel.username}")
                    continue

                # Обработка новых сообщений
                new_posts = []
                for update in updates:
                    if not isinstance(update, Message):
                        continue

                    msg_text = update.message or ""
                    if not msg_text.strip():
                        continue

                    # Проверка на дубликат
                    exists = await session.scalar(
                        select(Post).where(
                            (Post.message_id == update.id)
                            & (Post.channel_username == channel.username)
                        )
                    )
                    if exists:
                        continue

                    post = Post(
                        message_id=update.id,
                        channel_username=channel.username,
                        content=msg_text,
                    )
                    new_posts.append(post)

                if new_posts:
                    session.add_all(new_posts)
                    logger.info(
                        f"Сохранено {len(new_posts)} новых сообщений из {channel.username}"
                    )
                else:
                    logger.info(f"Новых уникальных сообщений в {channel.username} нет.")

            except Exception as e:
                logger.info(f"Ошибка при обработке сущности {channel.username}: {e}")
                # Не останавливаем обработку других сущностей
                continue
        await session.commit()
