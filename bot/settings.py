import os

from dotenv import load_dotenv
from redis.asyncio import Redis
from sqlalchemy import URL

load_dotenv()


class RedisSettings:
    def __init__(self) -> None:
        self.host = os.environ.get("REDIS_HOST", "localhost")
        self.port = int(os.environ.get("REDIS_PORT", 6379))
        self.db = os.environ.get("REDIS_DB", 0)


class DBSettings:
    def __init__(self, _env_prefix: str = "MYSQL_") -> None:
        self.host = os.environ.get(f"{_env_prefix}HOST", "localhost")
        self.port = os.environ.get(f"{_env_prefix}PORT", 3306)
        self.db = os.environ.get(f"{_env_prefix}DB", "database")
        self.username = os.environ.get(f"{_env_prefix}USERNAME", "user")
        self.password = os.environ.get(f"{_env_prefix}PASSWORD", "password")


class Settings:
    bot_token = os.environ.get("BOT_TOKEN", "")

    db: DBSettings = DBSettings()
    redis: RedisSettings = RedisSettings()

    def mysql_dsn(self) -> URL:
        return URL.create(
            drivername="mysql+aiomysql",
            database=self.db.db,
            username=self.db.username,
            password=self.db.password,
            host=self.db.host,
        )

    def mysql_dsn_string(self) -> str:
        return URL.create(
            drivername="mysql+aiomysql",
            database=self.db.db,
            username=self.db.username,
            password=self.db.password,
            host=self.db.host,
        ).render_as_string(hide_password=False)

    async def redis_dsn(self) -> Redis:
        return Redis(host=self.redis.host, port=self.redis.port, db=self.redis.db)


se = Settings()
