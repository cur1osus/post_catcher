from typing import Any

import msgspec
from redis.asyncio import Redis


class RedisStorage:
    def __init__(self, redis: Redis, client_hash: str):
        self._redis = redis
        self._client_hash = client_hash
        self.encoder = msgspec.json.Encoder()
        self.decoder = msgspec.json.Decoder()

    def build_key(self, key: str) -> str:
        return f"catcher:{self._client_hash}:{key}"

    async def get(self, key: Any) -> Any | None:
        """
        Извлекает данные из Redis и десериализует их с использованием msgspec.

        :param key: Ключ для извлечения данных.
        :return: Десериализованные данные, или None если ключ не найден.
        """
        if not self._redis:
            return None
        data = await self._redis.get(self.build_key(key))
        return self.decoder.decode(data) if data else None

    async def set(self, key: Any, value: Any, **kwargs) -> None:
        """
        Сохраняет данные в Redis с использованием msgspec для сериализации.

        :param key: Ключ для сохранения данных.
        :param value: Данные для сохранения.
        """
        serialized_data = self.encoder.encode(value)
        await self._redis.set(self.build_key(key), serialized_data, **kwargs)

    async def delete(self, *keys: Any) -> None:
        await self._redis.delete(*map(self.build_key, keys))
