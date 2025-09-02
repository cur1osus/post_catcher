import logging
from typing import Any, Union

import telethon
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    InviteHashExpiredError,
    UserAlreadyParticipantError,
)
from telethon.hints import Entity
from telethon.tl.functions.channels import (
    GetFullChannelRequest,
    GetParticipantRequest,
)
from telethon.tl.functions.messages import (
    CheckChatInviteRequest,
    GetHistoryRequest,
    ImportChatInviteRequest,
)
from telethon.tl.functions.updates import GetChannelDifferenceRequest
from telethon.tl.types import (
    Channel,
    ChannelMessagesFilter,
    Chat,
    ChatForbidden,
    ChatInvite,
    ChatInviteAlready,
    ChatInvitePeek,
    InputChannel,
    InputPeerChannel,
    InputPeerChat,
    Message,
    MessageRange,
    TypeChat,
    TypeMessage,
)
from telethon.tl.types.messages import ChatFull, Messages
from telethon.tl.types.updates import (
    ChannelDifference,
    ChannelDifferenceEmpty,
    ChannelDifferenceTooLong,
)

from bot.db.func import RedisStorage
from bot.db.models import MonitoringChannel

logger = logging.getLogger(__name__)

# Типы сущностей, которые мы можем отслеживать
TypeChatLike = Union[Channel, Chat]


class Function:
    @staticmethod
    async def get_channels(session: AsyncSession) -> list[MonitoringChannel]:
        logger.info("Загрузка идентификаторов каналов...")
        channels = (await session.scalars(select(MonitoringChannel))).all()
        return list(channels)

    @staticmethod
    async def safe_get_entity(
        client: TelegramClient,
        peer_id: Any,
    ) -> Entity | list[Entity] | None:
        try:
            # Сначала пробуем получить пользователя напрямую
            return await client.get_entity(peer_id)
        except ValueError:
            logger.info(
                f"Пользователь {peer_id} не найден в кэше, обновляем диалоги..."
            )

            try:
                # Обновляем кэш диалогов
                await client.get_dialogs()
                await client.catch_up()

                # Пробуем снова после обновления кэша
                return await client.get_entity(peer_id)
            except ValueError:
                logger.info(
                    f"Пользователь {peer_id} всё ещё недоступен после обновления кэша"
                )
                return None
            except Exception as e:
                logger.info(f"Ошибка при получении пользователя {peer_id}: {e}")
                return None

    @staticmethod
    async def get_difference_update_channel(
        client: TelegramClient,
        storage: RedisStorage,
        channel: TypeChatLike,
        channel_username: str,
    ) -> list[TypeMessage]:
        """Улучшенное получение обновлений для канала с максимальным охватом сообщений."""
        try:
            if not channel:
                return []

            if not channel.access_hash:
                return []

            input_channel = InputChannel(channel.id, channel.access_hash)
            chat_pts = await storage.get(channel_username)

            # Инициализация PTS через GetFullChannelRequest при первом запуске
            if not chat_pts:
                full_channel = await client(GetFullChannelRequest(input_channel))
                pts = full_channel.full_chat.pts  # pyright: ignore
                await storage.set(channel_username, pts)
                logger.info(f"Инициализирован PTS={pts} для канала {channel_username}")
            else:
                pts = int(chat_pts)

            # Запрос разницы БЕЗ фильтра (критически важно!)
            # Макс. значение для 32-bit signed int (Telegram использует 32-bit ID)
            MAX_MESSAGE_ID = 2**31 - 1
            filter = ChannelMessagesFilter(
                ranges=[MessageRange(0, MAX_MESSAGE_ID)],
                exclude_new_messages=False,
            )
            difference: ChannelDifference = await client(  # pyright: ignore
                GetChannelDifferenceRequest(
                    channel=input_channel,
                    filter=filter,
                    pts=pts,
                    limit=50,
                    force=True,  # Гарантируем получение изменений
                )
            )

            # Обработка случаев
            if isinstance(difference, ChannelDifferenceEmpty):
                logger.debug(
                    f"Канал {channel_username}: состояние актуально (PTS={pts})"
                )
                return []

            if isinstance(difference, ChannelDifferenceTooLong):
                logger.warning(
                    f"Канал {channel_username}: PTS={pts} сильно устарел. Получаем историю..."
                )
                return await Function._handle_too_long_state(
                    client,
                    input_channel,
                    storage,
                    channel_username,
                )

            # Сбор ВСЕХ сообщений (включая other_updates)
            updates = difference.new_messages.copy()
            if hasattr(difference, "other_updates") and difference.other_updates:
                updates.extend(
                    [  # pyright: ignore
                        update.message  # pyright: ignore
                        for update in difference.other_updates
                        if hasattr(update, "message") and update.message  # pyright: ignore
                    ]
                )

            # Обновление PTS только если есть изменения
            if difference.pts > pts:
                await storage.set(channel_username, difference.pts)
                logger.info(
                    f"Канал {channel_username}: получено {len(updates)} сообщений. PTS обновлен: {pts} → {difference.pts}"
                )
            else:
                logger.warning(
                    f"Канал {channel_username}: PTS не увеличился ({pts} → {difference.pts}). Возможно, ошибка синхронизации."
                )

            return updates

        except Exception as e:
            logger.exception(
                f"Критическая ошибка при обработке канала {channel_username}: {e}"
            )
            return []

    @staticmethod
    async def get_difference_update_chat(
        client: TelegramClient,
        storage: RedisStorage,
        chat: TypeChat,
        chat_username: str,
    ) -> list[Message]:
        """
        Получение новых сообщений из чата/группы с использованием эмуляции разностного обновления
        через хранение последнего известного max_id и поллинг истории.
        Подходит для групп и чатов, где нет ChannelDifference.
        """
        try:
            if not chat:
                return []

            # Поддержка Chat и ChatForbidden
            if isinstance(chat, ChatForbidden):
                logger.warning(
                    f"Доступ к чату {chat_username} запрещён (ChatForbidden)"
                )
                return []

            if not chat.id:
                return []

            input_chat = InputPeerChat(chat.id)  # Для старых чатов
            if hasattr(chat, "megagroup") and chat.megagroup:
                # Это супергруппа — используем InputPeerChannel
                if not chat.access_hash:
                    return []
                input_chat = InputPeerChannel(chat.id, chat.access_hash)

            # Получаем последний известный max_id (аналог PTS для чатов)
            last_max_id_str = await storage.get(f"chat_last_max_id:{chat_username}")
            last_max_id = int(last_max_id_str) if last_max_id_str else 0

            # Получаем последние сообщения
            messages = await client(
                GetHistoryRequest(
                    peer=input_chat,
                    limit=50,  # Максимум за один запрос
                    offset_date=None,
                    offset_id=0,
                    max_id=0,
                    min_id=last_max_id,
                    add_offset=0,
                    hash=0,
                )
            )

            new_messages = [
                msg
                for msg in messages.messages
                if isinstance(msg, Message) and msg.id > last_max_id
            ]

            if new_messages:
                # Сортируем по id, чтобы самые новые были в конце (как в каналах)
                new_messages.sort(key=lambda m: m.id)

                # Обновляем max_id
                current_max_id = max(msg.id for msg in new_messages)
                await storage.set(f"chat_last_max_id:{chat_username}", current_max_id)
                logger.info(
                    f"Чат {chat_username}: получено {len(new_messages)} новых сообщений. "
                    f"max_id обновлён: {last_max_id} → {current_max_id}"
                )
            else:
                logger.debug(
                    f"Чат {chat_username}: новых сообщений нет (last_max_id={last_max_id})"
                )

            return new_messages

        except Exception as e:
            logger.exception(
                f"Критическая ошибка при обработке чата {chat_username}: {e}"
            )
            return []

    @staticmethod
    async def _handle_too_long_state(
        client: TelegramClient,
        input_channel: InputChannel,
        storage: RedisStorage,
        channel_username: str,
    ) -> list[Any]:
        """Обработка устаревшего PTS через историю сообщений"""
        try:
            # Получаем последние 100 сообщений (максимум за один запрос)
            history: Messages = await client(
                GetHistoryRequest(
                    peer=input_channel,  # pyright: ignore
                    limit=100,
                    offset_date=None,
                    offset_id=0,
                    max_id=0,
                    min_id=0,
                    add_offset=0,
                    hash=0,
                )
            )

            # Обновляем PTS до актуального
            full_channel: ChatFull = await client(GetFullChannelRequest(input_channel))  # pyright: ignore
            new_pts = full_channel.full_chat.pts  # pyright: ignore
            await storage.set(channel_username, new_pts)

            logger.warning(
                f"Канал {channel_username}: восстановлено {len(history.messages)} сообщений "
                f"из истории после PTS устаревания (новый PTS={new_pts})"
            )
            return history.messages

        except Exception as e:
            logger.error(
                f"Ошибка при обработке ChannelDifferenceTooLong для {channel_username}: {e}"
            )
            return []

    class Sub:
        @staticmethod
        async def is_subscribed(channel_username: str, client: TelegramClient) -> bool:
            """Проверяет, подписан ли пользователь на канал."""
            try:
                await client(GetParticipantRequest(channel_username, "me"))  # pyright: ignore
                return True
            except Exception as e:
                if "USER_NOT_PARTICIPANT" in str(e):
                    return False
                else:
                    print(f"Ошибка при проверке подписки на {channel_username}: {e}")
                    return False

        @staticmethod
        async def subscribe_to_channel(
            channel_username: str,
            client: TelegramClient,
            storage: RedisStorage,
        ) -> bool:
            """Подписывается на канал, если ещё не подписан."""
            is_subscribed_cached = await storage.get(channel_username + ":subscribed")
            if is_subscribed_cached:
                return True
            try:
                if await Function.Sub.is_subscribed(channel_username, client):
                    print(f"✅ Уже подписан на {channel_username}")
                    await storage.set(channel_username + ":subscribed", True)
                    return True

                await client(
                    telethon.functions.channels.JoinChannelRequest(channel_username)  # pyright: ignore
                )
                print(f"✅ Подписался на {channel_username}")
                return True

            except UserAlreadyParticipantError:
                print(f"✅ Уже подписан на {channel_username}")
            except InviteHashExpiredError:
                print(f"❌ Ссылка-приглашение устарела для {channel_username}")
            except ChannelPrivateError:
                print(
                    f"❌ Канал {channel_username} недоступен (приватный или не существует)"
                )
            except Exception as e:
                print(f"❌ Ошибка при подписке на {channel_username}: {e}")
            return False

        @staticmethod
        async def subscribe_by_invite_hash(
            invite_hash: str,
            client: TelegramClient,
            storage: RedisStorage,
        ) -> bool:
            is_subscribed_cached = await storage.get(invite_hash + ":subscribed")
            if is_subscribed_cached:
                return True
            try:
                result = await client(ImportChatInviteRequest(invite_hash))
                print(f"Присоединились по invite-ссылке: {invite_hash}")
                await storage.set(invite_hash + ":subscribed", True)
                return True
            except UserAlreadyParticipantError:
                await storage.set(invite_hash + ":subscribed", True)
                print(f"✅ Уже подписан по ссылке {invite_hash}")
                return True
            except Exception as e:
                logger.info(f"Ошибка при подписке по ссылке: {e}")
                return False

        @staticmethod
        async def fetch_id_from_chat_invite_request(
            link: str,
            client: TelegramClient,
        ) -> str | None:
            result: ChatInviteAlready | ChatInvite | ChatInvitePeek = await client(  # pyright: ignore
                CheckChatInviteRequest(link)
            )

            try:
                if isinstance(result, ChatInvite):
                    # Уже состоишь в чате
                    return None
                elif isinstance(result, (ChatInviteAlready, ChatInvitePeek)):
                    # Приглашение активно, можно присоединиться
                    return str(result.chat.id)
                else:
                    logger.info(result, "Ссылка не действительна")
            except InviteHashExpiredError:
                return None
