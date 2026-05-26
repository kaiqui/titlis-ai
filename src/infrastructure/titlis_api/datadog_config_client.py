from typing import Any, Dict, Optional

import httpx

from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DatadogConfigClient:
    async def get_dd_config(self, tenant_id: int) -> Optional[Dict[str, Any]]:
        url = f"{settings.titlis_api_url}/v1/internal/ai/datadog-config"
        headers = {"X-Internal-Secret": settings.internal_secret}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, params={"tenantId": tenant_id}, headers=headers)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp.json()
        except Exception:
            logger.warning("Falha ao buscar credenciais Datadog", extra={"tenant_id": tenant_id})
            return None
