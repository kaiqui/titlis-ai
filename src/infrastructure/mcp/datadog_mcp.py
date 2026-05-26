from contextlib import asynccontextmanager
from typing import AsyncIterator

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from src.settings import settings


# Conecta ao MCP remoto do Datadog via streamable HTTP (POST).
# URL oficial: https://mcp.{site}/api/unstable/mcp-server/mcp
# Configurável via DATADOG_MCP_URL; padrão cobre datadoghq.com e datadoghq.eu via site.
@asynccontextmanager
async def datadog_mcp_session(
    dd_api_key: str,
    dd_app_key: str,
    site: str = "datadoghq.com",
) -> AsyncIterator[ClientSession]:
    url = settings.datadog_mcp_url or f"https://mcp.{site}/api/unstable/mcp-server/mcp"
    if not dd_api_key:
        raise ValueError("DD-API-KEY ausente — configure as credenciais Datadog em Configurações → Integrações")
    headers: dict = {"DD-API-KEY": dd_api_key}
    if dd_app_key:
        headers["DD-APPLICATION-KEY"] = dd_app_key
    async with streamablehttp_client(url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session
