import json
from typing import List, Optional

import httpx

from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_POOL = httpx.Limits(max_connections=10, max_keepalive_connections=5, keepalive_expiry=30)


class KnowledgeClient:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=settings.titlis_api_url,
            headers={"X-Internal-Secret": settings.internal_secret},
            timeout=30.0,
            limits=_POOL,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def index_chunk(
        self,
        tenant_id: Optional[int],
        source_type: str,
        source_id: str,
        chunk_text: str,
        embedding: List[float],
        metadata: Optional[dict] = None,
    ) -> str:
        payload = {
            "tenantId": tenant_id,
            "sourceType": source_type,
            "sourceId": source_id,
            "chunkText": chunk_text,
            "embedding": embedding,
            "metadata": json.dumps(metadata) if metadata else None,
        }
        resp = await self._client.post("/v1/internal/rag/chunks", json=payload)
        resp.raise_for_status()
        return resp.json()["id"]

    async def search_similar(
        self,
        tenant_id: int,
        embedding: List[float],
        limit: int = 5,
    ) -> List[dict]:
        resp = await self._client.post(
            "/v1/internal/rag/search",
            json={"tenantId": tenant_id, "embedding": embedding, "limit": limit},
        )
        resp.raise_for_status()
        result: List[dict] = resp.json()
        return result
