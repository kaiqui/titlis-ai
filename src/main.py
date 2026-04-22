import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import litellm
from fastapi import FastAPI

from src.routes.agent import router as agent_router
from src.routes.explain import router as explain_router
from src.routes.feedback import router as feedback_router
from src.routes.health import router as health_router
from src.routes.knowledge import router as knowledge_router
from src.routes.metrics import router as metrics_router
from src.routes.remediate import router as remediate_router
from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _setup_litellm_callbacks() -> None:
    if not settings.langfuse_enabled:
        return
    os.environ.setdefault("LANGFUSE_PUBLIC_KEY", settings.langfuse_public_key)
    os.environ.setdefault("LANGFUSE_SECRET_KEY", settings.langfuse_secret_key)
    os.environ.setdefault("LANGFUSE_HOST", settings.langfuse_base_url)
    litellm.success_callback = ["langfuse"]
    litellm.failure_callback = ["langfuse"]
    logger.info("LiteLLM → Langfuse callback habilitado", extra={"host": settings.langfuse_base_url})


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    _setup_litellm_callbacks()
    logger.info(
        "titlis-ai iniciando",
        extra={
            "port": settings.port,
            "log_level": settings.log_level,
            "langfuse_enabled": settings.langfuse_enabled,
        },
    )
    yield
    logger.info("titlis-ai encerrando")


app = FastAPI(
    title="titlis-ai",
    description="Assistente de IA para findings de scorecard Kubernetes",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(metrics_router)
app.include_router(explain_router, prefix="/v1")
app.include_router(remediate_router, prefix="/v1")
app.include_router(agent_router, prefix="/v1")
app.include_router(knowledge_router, prefix="/v1")
app.include_router(feedback_router, prefix="/v1")
