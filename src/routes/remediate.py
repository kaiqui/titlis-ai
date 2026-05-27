import json
import time
import uuid
from typing import AsyncGenerator

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from langgraph.types import Command

from src.bootstrap.dependencies import get_remediation_graph
from src.domain.models import ConfirmRemediationRequest, RemediateRequest, SetManifestPathRequest
from src.observability.langfuse_handler import get_langfuse_callbacks
from src.observability.metrics import (
    ai_latency_seconds,
    ai_pr_created_total,
    ai_pr_user_rejected_total,
    ai_requests_total,
)
from src.settings import settings
from src.utils.logger import get_logger
from src.utils.resilience import keepalive_stream

logger = get_logger(__name__)
router = APIRouter()

_MAX_RETRIES = 3


def _verify_internal_secret(request: Request) -> None:
    secret = request.headers.get("X-Internal-Secret", "")
    if secret != settings.internal_secret:
        raise HTTPException(status_code=403, detail="internal_secret_invalid")


def _sse(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


def _build_graph_config(thread_id: str, tenant_id: int, rule_ids: str, provider: str) -> dict:
    config: dict = {"configurable": {"thread_id": thread_id}}
    callbacks = get_langfuse_callbacks(tenant_id, rule_ids, provider)
    if callbacks:
        config["callbacks"] = callbacks
    return config


@router.post("/remediate")
async def remediate(body: RemediateRequest, request: Request) -> StreamingResponse:
    _verify_internal_secret(request)

    graph = get_remediation_graph()
    thread_id = str(uuid.uuid4())
    rule_ids = ",".join(body.finding_ids)
    config = _build_graph_config(thread_id, body.tenant_id, rule_ids, body.ai_config.provider)

    tenant_label = str(body.tenant_id)
    provider_label = body.ai_config.provider
    model_label = body.ai_config.model

    initial_state = {
        "tenant_id": body.tenant_id,
        "workload_id": body.workload_id,
        "finding_ids": body.finding_ids,
        "repo_url": body.repo_url,
        "deploy_manifest_path": body.deploy_manifest_path,
        "ai_config": body.ai_config.model_dump(),
        "retry_count": 0,
        "validation_errors": [],
    }

    async def _inner() -> AsyncGenerator[str, None]:
        start = time.monotonic()
        status = "success"
        try:
            async for event in graph.compiled.astream(initial_state, config, stream_mode="updates"):
                if "__interrupt__" in event:
                    interrupt_val = event["__interrupt__"][0].value
                    if interrupt_val.get("type") == "manifest_path_required":
                        yield _sse(
                            {
                                "type": "path_required",
                                "thread_id": thread_id,
                                "detected_environment": interrupt_val.get("detected_environment"),
                                "suggested_path": interrupt_val.get("suggested_path"),
                                "deployment_name": interrupt_val.get("deployment_name"),
                                "namespace": interrupt_val.get("namespace"),
                            }
                        )
                    else:
                        yield _sse(
                            {
                                "type": "fix_ready",
                                "thread_id": thread_id,
                                "patched_manifest": interrupt_val.get("patched_manifest"),
                                "current_manifest": interrupt_val.get("current_manifest"),
                                "findings": interrupt_val.get("findings", []),
                                "deployment_name": interrupt_val.get("deployment_name"),
                                "namespace": interrupt_val.get("namespace"),
                            }
                        )
                    yield _sse({"type": "done"})
                    return

                for node_name, node_output in event.items():
                    if node_name.startswith("__"):
                        continue
                    if node_name == "check_existing_pr":
                        existing = (node_output or {}).get("existing_pr")
                        if existing:
                            yield _sse({"type": "existing_pr", "pr_url": existing["pr_url"]})
                    yield _sse({"type": "progress", "node": node_name})

            final = graph.compiled.get_state(config)
            state_vals = final.values if final else {}
            if state_vals.get("validation_errors") and state_vals.get("retry_count", 0) >= _MAX_RETRIES:
                status = "validation_failed"
                yield _sse({"type": "error", "error": "patch_validation_failed_max_retries"})
            yield _sse({"type": "done"})

        except Exception as exc:
            status = "error"
            logger.exception(
                "Erro no pipeline de remediação",
                extra={"tenant_id": body.tenant_id, "workload_id": body.workload_id},
            )
            yield _sse({"type": "error", "error": str(exc)})
            yield _sse({"type": "done"})
        finally:
            elapsed = time.monotonic() - start
            ai_requests_total.labels(
                tenant_id=tenant_label,
                provider=provider_label,
                model=model_label,
                rule_id=rule_ids,
                status=status,
            ).inc()
            ai_latency_seconds.labels(
                tenant_id=tenant_label,
                provider=provider_label,
                model=model_label,
                phase="remediate",
            ).observe(elapsed)

    async def generate() -> AsyncGenerator[str, None]:
        async for chunk in keepalive_stream(_inner()):
            if await request.is_disconnected():
                logger.info(
                    "Cliente desconectou durante remediação",
                    extra={"tenant_id": body.tenant_id, "workload_id": body.workload_id},
                )
                break
            yield chunk

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/remediate/{thread_id}/confirm")
async def confirm_remediation(
    thread_id: str,
    body: ConfirmRemediationRequest,
    request: Request,
) -> StreamingResponse:
    _verify_internal_secret(request)

    graph = get_remediation_graph()
    # Reconstrói config sem callbacks — thread já está no checkpointer
    config: dict = {"configurable": {"thread_id": thread_id}}

    async def _inner() -> AsyncGenerator[str, None]:
        try:
            async for event in graph.compiled.astream(Command(resume=body.approved), config, stream_mode="updates"):
                for node_name, node_output in event.items():
                    if node_name.startswith("__"):
                        continue
                    if node_name == "create_remediation_pr":
                        pr = (node_output or {}).get("pr_result", {})
                        if pr:
                            yield _sse(
                                {
                                    "type": "pr_created",
                                    "pr_url": pr.get("pr_url"),
                                    "pr_number": pr.get("pr_number"),
                                    "branch": pr.get("branch"),
                                }
                            )
                            ai_pr_created_total.labels(
                                tenant_id=str(body.approved),
                                pillar="unknown",
                            ).inc()
                    elif node_name == "notify_api" and not body.approved:
                        ai_pr_user_rejected_total.labels(
                            tenant_id="unknown",
                            rule_id="unknown",
                        ).inc()
                    yield _sse({"type": "progress", "node": node_name})

            yield _sse({"type": "done"})

        except Exception as exc:
            logger.exception("Erro ao confirmar remediação", extra={"thread_id": thread_id})
            yield _sse({"type": "error", "error": str(exc)})
            yield _sse({"type": "done"})

    async def generate() -> AsyncGenerator[str, None]:
        async for chunk in keepalive_stream(_inner()):
            if await request.is_disconnected():
                logger.info("Cliente desconectou durante confirmação", extra={"thread_id": thread_id})
                break
            yield chunk

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/remediate/{thread_id}/set-path")
async def set_manifest_path(
    thread_id: str,
    body: SetManifestPathRequest,
    request: Request,
) -> StreamingResponse:
    _verify_internal_secret(request)

    graph = get_remediation_graph()
    config: dict = {"configurable": {"thread_id": thread_id}}

    async def _inner() -> AsyncGenerator[str, None]:
        try:
            async for event in graph.compiled.astream(
                Command(resume=body.manifest_path), config, stream_mode="updates"
            ):
                if "__interrupt__" in event:
                    interrupt_val = event["__interrupt__"][0].value
                    yield _sse(
                        {
                            "type": "fix_ready",
                            "thread_id": thread_id,
                            "patched_manifest": interrupt_val.get("patched_manifest"),
                            "current_manifest": interrupt_val.get("current_manifest"),
                            "findings": interrupt_val.get("findings", []),
                            "deployment_name": interrupt_val.get("deployment_name"),
                            "namespace": interrupt_val.get("namespace"),
                        }
                    )
                    yield _sse({"type": "done"})
                    return

                for node_name, node_output in event.items():
                    if node_name.startswith("__"):
                        continue
                    if node_name == "check_existing_pr":
                        existing = (node_output or {}).get("existing_pr")
                        if existing:
                            yield _sse({"type": "existing_pr", "pr_url": existing["pr_url"]})
                    yield _sse({"type": "progress", "node": node_name})

            final = graph.compiled.get_state(config)
            state_vals = final.values if final else {}
            if state_vals.get("validation_errors") and state_vals.get("retry_count", 0) >= _MAX_RETRIES:
                yield _sse({"type": "error", "error": "patch_validation_failed_max_retries"})
            yield _sse({"type": "done"})

        except Exception as exc:
            logger.exception("Erro ao definir path do manifest", extra={"thread_id": thread_id})
            yield _sse({"type": "error", "error": str(exc)})
            yield _sse({"type": "done"})

    async def generate() -> AsyncGenerator[str, None]:
        async for chunk in keepalive_stream(_inner()):
            if await request.is_disconnected():
                logger.info("Cliente desconectou (set-path)", extra={"thread_id": thread_id})
                break
            yield chunk

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
