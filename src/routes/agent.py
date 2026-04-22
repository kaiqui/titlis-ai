from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from src.bootstrap.dependencies import get_agent_service, get_session_store
from src.domain.models import AgentChatRequest, AgentToolsRespondRequest
from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


def _verify_internal_secret(request: Request) -> None:
    secret = request.headers.get("X-Internal-Secret", "")
    if secret != settings.internal_secret:
        raise HTTPException(status_code=403, detail="internal_secret_invalid")


@router.post("/agent/chat")
async def agent_chat(body: AgentChatRequest, request: Request) -> StreamingResponse:
    _verify_internal_secret(request)

    service = get_agent_service()
    store = get_session_store()

    session = store.get_or_create(body.session_id, body.tenant_id, body.ai_config.model_dump())
    store.cleanup_expired()

    async def generate():
        try:
            async for chunk in service.run_turn(session, body.message):
                yield chunk
        except Exception as exc:
            import json
            logger.exception("Erro no agente", extra={"session_id": body.session_id})
            yield f"data: {json.dumps({'type': 'error', 'error': str(exc)})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/agent/{session_id}/tools/respond")
async def agent_tools_respond(
    session_id: str,
    body: AgentToolsRespondRequest,
    request: Request,
) -> StreamingResponse:
    _verify_internal_secret(request)

    store = get_session_store()
    session = store.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session_not_found")

    service = get_agent_service()

    async def generate():
        try:
            async for chunk in service.run_tool_responses(session, body.decisions):
                yield chunk
        except Exception as exc:
            import json
            logger.exception("Erro ao responder tools", extra={"session_id": session_id})
            yield f"data: {json.dumps({'type': 'error', 'error': str(exc)})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/agent/{session_id}/audit")
async def agent_audit_log(session_id: str, request: Request):
    _verify_internal_secret(request)

    store = get_session_store()
    session = store.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session_not_found")

    return {"session_id": session_id, "audit_log": session.audit_log}
