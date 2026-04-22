import json
from typing import List, Optional

import httpx

from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class KnowledgeClient:
    def __init__(self) -> None:
        self._base = settings.titlis_api_url
        self._secret = settings.internal_secret

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
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self._base}/v1/internal/rag/chunks",
                json=payload,
                headers={"X-Internal-Secret": self._secret},
                timeout=30.0,
            )
            resp.raise_for_status()
            return resp.json()["id"]

    async def search_similar(
        self,
        tenant_id: int,
        embedding: List[float],
        limit: int = 5,
    ) -> List[dict]:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self._base}/v1/internal/rag/search",
                json={"tenantId": tenant_id, "embedding": embedding, "limit": limit},
                headers={"X-Internal-Secret": self._secret},
                timeout=30.0,
            )
            resp.raise_for_status()
            result: List[dict] = resp.json()
            return result
