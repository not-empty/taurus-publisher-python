from redis.asyncio import Redis, ConnectionPool
from functools import lru_cache


@lru_cache()
def get_connection_pool(
    host: str,
    port: int,
) -> ConnectionPool:
    return ConnectionPool(
        host=host,
        port=port,
    )


class RedisConnection:
    def __init__(self):
        ...

    async def create_connection(
        self,
        host: str,
        port: int,
    ) -> Redis:
        return await Redis(
            connection_pool=get_connection_pool(
                host=host,
                port=port,
            )
        )
