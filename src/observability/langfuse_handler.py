from typing import List

from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


def get_langfuse_callbacks(tenant_id: int, rule_ids: str, provider: str) -> List:
    if not settings.langfuse_enabled:
        return []
    try:
        from langfuse.callback import CallbackHandler  # type: ignore[import]

        handler = CallbackHandler(
            public_key=settings.langfuse_public_key,
            secret_key=settings.langfuse_secret_key,
            host=settings.langfuse_base_url,
            tags=[f"tenant_id:{tenant_id}", f"rule_id:{rule_ids}", f"provider:{provider}"],
        )
        return [handler]
    except ImportError:
        logger.warning("langfuse não instalado — tracing desativado")
        return []
    except Exception:
        logger.warning("Falha ao inicializar Langfuse callback — tracing desativado")
        return []
