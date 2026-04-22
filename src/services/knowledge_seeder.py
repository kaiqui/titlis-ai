from typing import Optional

from src.infrastructure.titlis_api.knowledge_client import KnowledgeClient
from src.services.embedding_service import EmbeddingService
from src.services.prompt_builder import _RULE_CONTEXT
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _build_chunk_text(rule_id: str, ctx: dict) -> str:
    return (
        f"Regra: {rule_id} — {ctx['title']}\n"
        f"Pilar: {ctx['pillar']}\n"
        f"Por que importa: {ctx['why']}\n"
        f"Como corrigir: {ctx['fix_hint']}"
    )


class KnowledgeSeeder:
    def __init__(
        self,
        embedding_service: EmbeddingService,
        knowledge_client: KnowledgeClient,
    ) -> None:
        self._embedder = embedding_service
        self._client = knowledge_client

    async def seed_global_rules(
        self,
        provider: str,
        api_key: str,
        tenant_id: Optional[int] = None,
    ) -> int:
        seeded = 0
        for rule_id, ctx in _RULE_CONTEXT.items():
            try:
                chunk_text = _build_chunk_text(rule_id, ctx)
                embedding = await self._embedder.embed(chunk_text, provider, api_key)
                await self._client.index_chunk(
                    tenant_id=tenant_id,
                    source_type="global_rule_doc",
                    source_id=rule_id,
                    chunk_text=chunk_text,
                    embedding=embedding,
                    metadata={
                        "rule_id": rule_id,
                        "title": ctx["title"],
                        "pillar": ctx["pillar"],
                    },
                )
                seeded += 1
                logger.info("Chunk global semeado", extra={"rule_id": rule_id})
            except Exception:
                logger.exception("Erro ao semear chunk global", extra={"rule_id": rule_id})
        logger.info("Seed global concluído", extra={"total": seeded})
        return seeded
