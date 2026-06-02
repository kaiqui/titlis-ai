import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import StreamableHTTPTransport, streamable_http_client

from src.observability.metrics import mcp_init_failed_total
from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_INIT_TIMEOUT = 30.0
_MAX_INIT_RETRIES = 3
_INIT_RETRY_DELAYS = [1.0, 2.0]


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

    last_exc: Optional[Exception] = None
    for attempt in range(_MAX_INIT_RETRIES):
        if attempt > 0:
            delay = _INIT_RETRY_DELAYS[attempt - 1]
            logger.warning(
                "Datadog MCP init falhou, tentando novamente",
                extra={"attempt": attempt + 1, "delay": delay, "error": str(last_exc)[:120]},
            )
            await asyncio.sleep(delay)

        _initialized = False
        http_client = httpx.AsyncClient(
            headers={"DD-API-KEY": dd_api_key, **({"DD-APPLICATION-KEY": dd_app_key} if dd_app_key else {})},
            timeout=httpx.Timeout(30.0, read=float(settings.datadog_mcp_read_timeout)),
        )
        try:
            async with http_client:
                async with streamable_http_client(url, http_client=http_client) as (read, write, _):
                    async with ClientSession(read, write) as session:
                        await asyncio.wait_for(session.initialize(), timeout=_INIT_TIMEOUT)
                        _initialized = True
                        yield session
                        return
        except Exception as exc:
            if _initialized:
                raise  # Exception do caller, não do init — propaga imediatamente
            last_exc = exc

    mcp_init_failed_total.labels(provider="datadog").inc()
    logger.critical(
        "Datadog MCP init falhou após todos os retries",
        extra={"attempts": _MAX_INIT_RETRIES, "error": str(last_exc)[:200]},
    )
    raise last_exc  # type: ignore[misc]
