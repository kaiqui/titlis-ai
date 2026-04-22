import json
import time
from typing import AsyncGenerator, List

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from src.bootstrap.dependencies import (
    get_embedding_service,
    get_knowledge_client,
    get_llm_service,
    get_prompt_builder,
)
from src.domain.models import ExplainRequest, SseChunk
from src.infrastructure.titlis_api.knowledge_client import KnowledgeClient
from src.observability.metrics import ai_latency_seconds, ai_requests_total
from src.services.embedding_service import EmbeddingService
from src.services.llm_service import LLMService, QuotaExceededError
from src.services.prompt_builder import PromptBuilder
from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


def _verify_internal_secret(request: Request) -> None:
    secret = request.headers.get("X-Internal-Secret", "")
    if secret != settings.internal_secret:
        raise HTTPException(status_code=403, detail="internal_secret_invalid")


async def _retrieve_chunks(
    body: ExplainRequest,
    embedding_service: EmbeddingService,
    knowledge_client: KnowledgeClient,
) -> List[dict]:
    try:
        query = (
            f"{body.finding.rule_id} {body.finding.pillar} "
            f"{body.finding.actual_value or ''} {body.finding.deployment_name}"
        ).strip()
        embedding = await embedding_service.embed(
            text=query,
            provider=body.ai_config.provider,
            api_key=body.ai_config.api_key,
        )
        return await knowledge_client.search_similar(
            tenant_id=body.tenant_id,
            embedding=embedding,
            limit=settings.rag_top_k,
        )
    except Exception:
        logger.warning(
            "RAG retrieval falhou — continuando sem chunks",
            extra={"tenant_id": body.tenant_id, "rule_id": body.finding.rule_id},
        )
        return []


@router.post("/explain")
async def explain_finding(
    body: ExplainRequest,
    request: Request,
    llm_service: LLMService = Depends(get_llm_service),
    prompt_builder: PromptBuilder = Depends(get_prompt_builder),
    embedding_service: EmbeddingService = Depends(get_embedding_service),
    knowledge_client: KnowledgeClient = Depends(get_knowledge_client),
) -> StreamingResponse:
    _verify_internal_secret(request)

    chunks: List[dict] = []
    if settings.rag_enabled:
        chunks = await _retrieve_chunks(body, embedding_service, knowledge_client)

    messages = prompt_builder.build_explain_messages(body.finding, chunks=chunks)

    tenant_label = str(body.tenant_id)
    provider_label = body.ai_config.provider
    model_label = body.ai_config.model
    rule_label = body.finding.rule_id

    async def generate() -> AsyncGenerator[str, None]:
        start = time.monotonic()
        status = "success"
        try:
            async for chunk in llm_service.chat_stream(
                messages=messages,
                config=body.ai_config,
                tenant_id=body.tenant_id,
                trace_metadata={"rule_id": body.finding.rule_id, "phase": "explain"},
            ):
                event = SseChunk(type="chunk", content=chunk)
                yield f"data: {json.dumps(event.model_dump(exclude_none=True))}\n\n"
            done = SseChunk(type="done")
            yield f"data: {json.dumps(done.model_dump(exclude_none=True))}\n\n"
        except QuotaExceededError as exc:
            status = "quota_exceeded"
            error = SseChunk(type="error", error=f"quota_exceeded:{exc.budget}")
            yield f"data: {json.dumps(error.model_dump(exclude_none=True))}\n\n"
        except Exception as exc:
            status = "error"
            logger.exception(
                "Erro inesperado no explain_finding",
                extra={"tenant_id": body.tenant_id, "rule_id": body.finding.rule_id},
            )
            error = SseChunk(type="error", error=str(exc))
            yield f"data: {json.dumps(error.model_dump(exclude_none=True))}\n\n"
        finally:
            elapsed = time.monotonic() - start
            ai_requests_total.labels(
                tenant_id=tenant_label,
                provider=provider_label,
                model=model_label,
                rule_id=rule_label,
                status=status,
            ).inc()
            ai_latency_seconds.labels(
                tenant_id=tenant_label,
                provider=provider_label,
                model=model_label,
                phase="explain",
            ).observe(elapsed)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
