import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import StreamableHTTPTransport, streamable_http_client

from src.settings import settings

_INIT_TIMEOUT = 30.0


# Datadog MCP suporta apenas POST (JSON-RPC). Não suporta GET para SSE de
# notificações server-initiated. O streamable_http_client 1.27.x inicia um
# background task que faz GET após o initialize — Datadog retorna 405.
# Sobrescrevemos handle_get_stream na subclasse para evitar o GET.
class _PostOnlyTransport(StreamableHTTPTransport):
    async def handle_get_stream(self, *args, **kwargs) -> None:
        return


# Monkey-patch pontual: substitui StreamableHTTPTransport pelo nosso no módulo
# para que streamable_http_client use a subclasse ao criar a instância interna.
import mcp.client.streamable_http as _shttp  # noqa: E402

_shttp.StreamableHTTPTransport = _PostOnlyTransport  # type: ignore[attr-defined, misc]


@asynccontextmanager
async def datadog_mcp_session(
    dd_api_key: str,
    dd_app_key: str,
    site: str = "datadoghq.com",
) -> AsyncIterator[ClientSession]:
    url = settings.datadog_mcp_url or f"https://mcp.{site}/api/unstable/mcp-server/mcp"
    if not dd_api_key:
        raise ValueError("DD-API-KEY ausente — configure as credenciais Datadog em Configurações → Integrações")
    http_client = httpx.AsyncClient(
        headers={"DD-API-KEY": dd_api_key, **({"DD-APPLICATION-KEY": dd_app_key} if dd_app_key else {})},
        timeout=httpx.Timeout(30.0, read=300.0),
    )
    async with http_client:
        async with streamable_http_client(url, http_client=http_client) as (read, write, _):
            async with ClientSession(read, write) as session:
                await asyncio.wait_for(session.initialize(), timeout=_INIT_TIMEOUT)
                yield session
