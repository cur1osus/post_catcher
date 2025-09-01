import argparse
import asyncio
import logging
import sys

from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio.session import AsyncSession
from telethon import TelegramClient

from bot.background_jobs import handling_difference_update_chanel
from bot.db.base import create_db_session_pool
from bot.db.func import RedisStorage
from bot.scheduler import Scheduler
from bot.settings import se

# Создаём объект парсера аргументов
parser = argparse.ArgumentParser(description="Запуск Telegram-бота с аргументами")
parser.add_argument("path_session", type=str, help="Путь к сессии")
parser.add_argument("api_id", type=int, help="ID бота")
parser.add_argument("api_hash", type=str, help="Хэш бота")

# Парсим аргументы
args = parser.parse_args()
bot_path_session: str = args.path_session
bot_api_id: int = int(args.api_id)
bot_api_hash: str = args.api_hash


scheduler = Scheduler()


async def run_scheduler() -> None:
    while True:
        await scheduler.run_pending()
        await asyncio.sleep(1)


async def set_tasks(
    client: TelegramClient,
    sessionmaker: async_sessionmaker[AsyncSession],
    storage: RedisStorage,
):
    scheduler.every(10).seconds.do(
        handling_difference_update_chanel,
        client,
        sessionmaker,
        storage,
    )


async def init_telethon_client() -> TelegramClient | None:
    """Инициализация Telegram клиента"""
    try:
        client = TelegramClient(bot_path_session, bot_api_id, bot_api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            logger.info("Сессия не авторизована")
            return None
        else:
            logger.info("Клиент Telegram инициализирован")
            return client
    except Exception as e:
        logger.exception(f"Ошибка при инициализации клиента: {e}")
        return None


async def main() -> None:
    logger.info("Запуск...")

    # Инициализация redis
    redis = await se.redis_dsn()

    # Инициализация клиентов БД
    engine, sessionmaker = await create_db_session_pool(se)

    client = await init_telethon_client()
    if not client:
        logger.error("Ошибка при инициализации клиента Telegram")
        exit()

    storage = RedisStorage(redis=redis, client_hash=bot_api_hash)

    await set_tasks(client, sessionmaker, storage)

    # Запуск планировщика и клиента
    try:
        logger.info("Запуск планировщика и клиента")
        await asyncio.gather(
            client.start(),  # pyright: ignore
            run_scheduler(),
        )
        await client.run_until_disconnected()  # pyright: ignore
    except Exception as e:
        logger.exception(f"Ошибка при запуске Клиента: {e}")
    finally:
        await client.disconnect()  # pyright: ignore
        logger.info("Клиент отключен")


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.getLogger("schedule").setLevel(logging.WARNING)

    # Формат логов
    f = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Обработчик для вывода в консоль
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(f)
    logger.addHandler(console_handler)

    asyncio.run(main())
