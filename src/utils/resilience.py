import asyncio
from typing import AsyncGenerator

_SSE_KEEPALIVE = ": keepalive\n\n"


async def keepalive_stream(
    source: AsyncGenerator[str, None],
    interval: float = 15.0,
) -> AsyncGenerator[str, None]:
    iterator = source.__aiter__()
    while True:
        try:
            chunk = await asyncio.wait_for(iterator.__anext__(), timeout=interval)
            yield chunk
        except asyncio.TimeoutError:
            yield _SSE_KEEPALIVE
        except StopAsyncIteration:
            break
