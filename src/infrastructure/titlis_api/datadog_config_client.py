from typing import Any, Dict, Optional

import httpx

from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_POOL = httpx.Limits(max_connections=5, max_keepalive_connections=3, keepalive_expiry=30)


class DatadogConfigClient:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=settings.titlis_api_url,
            headers={"X-Internal-Secret": settings.internal_secret},
            timeout=10.0,
            limits=_POOL,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def get_dd_config(self, tenant_id: int) -> Optional[Dict[str, Any]]:
        try:
            resp = await self._client.get(
                "/v1/internal/ai/datadog-config",
                params={"tenantId": tenant_id},
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception:
            logger.warning("Falha ao buscar credenciais Datadog", extra={"tenant_id": tenant_id})
            return None
