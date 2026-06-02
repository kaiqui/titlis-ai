import asyncio
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from src.observability.metrics import mcp_init_failed_total
from src.utils.logger import get_logger

logger = get_logger(__name__)

_INIT_TIMEOUT = 30.0
_MAX_INIT_RETRIES = 3
_INIT_RETRY_DELAYS = [1.0, 2.0]
_MCP_CALL_TIMEOUT = 30.0


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

    last_exc: Optional[Exception] = None
    for attempt in range(_MAX_INIT_RETRIES):
        if attempt > 0:
            delay = _INIT_RETRY_DELAYS[attempt - 1]
            logger.warning(
                "GitHub MCP init falhou, tentando novamente",
                extra={"attempt": attempt + 1, "delay": delay, "error": str(last_exc)[:120]},
            )
            await asyncio.sleep(delay)

        _initialized = False
        try:
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await asyncio.wait_for(session.initialize(), timeout=_INIT_TIMEOUT)
                    _initialized = True
                    yield session
                    return
        except Exception as exc:
            if _initialized:
                raise  # Exception do caller, não do init — propaga imediatamente
            last_exc = exc

    mcp_init_failed_total.labels(provider="github").inc()
    logger.critical(
        "GitHub MCP init falhou após todos os retries — github-mcp-server pode estar morto",
        extra={"attempts": _MAX_INIT_RETRIES, "error": str(last_exc)[:200]},
    )
    raise last_exc  # type: ignore[misc]
