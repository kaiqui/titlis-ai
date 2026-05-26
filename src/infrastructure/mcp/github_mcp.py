import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


# Conecta ao github-mcp-server via stdio.
# Suporta dois modos:
#   PAT: GITHUB_TOKEN passado via github_token
#   GitHub App: GITHUB_APP_ID + GITHUB_APP_PRIVATE_KEY + GITHUB_APP_INSTALLATION_ID
@asynccontextmanager
async def github_mcp_session(
    github_token: Optional[str] = None,
    github_app_id: Optional[str] = None,
    github_app_private_key: Optional[str] = None,
    github_app_installation_id: Optional[str] = None,
) -> AsyncIterator[ClientSession]:
    if github_app_id and github_app_private_key and github_app_installation_id:
        env = {
            **os.environ,
            "GITHUB_APP_ID": github_app_id,
            "GITHUB_APP_PRIVATE_KEY": github_app_private_key,
            "GITHUB_APP_INSTALLATION_ID": github_app_installation_id,
        }
    elif github_token:
        env = {**os.environ, "GITHUB_PERSONAL_ACCESS_TOKEN": github_token}
    else:
        raise ValueError(
            "GitHub auth não configurado — configure um Personal Access Token "
            "ou as credenciais de GitHub App em Configurações → Integrações"
        )

    server_params = StdioServerParameters(
        command="github-mcp-server",
        args=["stdio", "--toolsets", "all"],
        env=env,
    )
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session
