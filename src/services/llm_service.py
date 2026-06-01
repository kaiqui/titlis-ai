import asyncio
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx
import litellm

from src.domain.models import TenantAiConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)

BUDGET_WARNING_THRESHOLD = 0.80

_PROVIDER_ALIASES = {
    "google": "gemini",
}

# Erros transitórios que justificam retry — excluem erros de auth/quota/payload.
_RETRYABLE_HTTP = (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.ConnectTimeout)


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, _RETRYABLE_HTTP):
        return True
    status = getattr(exc, "status_code", None)
    return status in (429, 502, 503, 504)


def build_litellm_model_id(provider: str, model: str) -> str:
    if "/" in model:
        return model
    normalized = _PROVIDER_ALIASES.get(provider, provider)
    return f"{normalized}/{model}"


class QuotaExceededError(Exception):
    def __init__(self, tenant_id: int, budget: int) -> None:
        self.tenant_id = tenant_id
        self.budget = budget
        super().__init__(f"Cota mensal de tokens atingida para tenant {tenant_id}. Budget={budget}")


class LLMService:
    def _check_quota(self, config: TenantAiConfig, tenant_id: int) -> None:
        if config.monthly_token_budget is None:
            return
        if config.tokens_used_month >= config.monthly_token_budget:
            raise QuotaExceededError(tenant_id, config.monthly_token_budget)
        ratio = config.tokens_used_month / config.monthly_token_budget
        if ratio >= BUDGET_WARNING_THRESHOLD:
            logger.warning(
                "Token budget acima de 80% para tenant",
                extra={
                    "tenant_id": tenant_id,
                    "used": config.tokens_used_month,
                    "budget": config.monthly_token_budget,
                    "ratio_pct": round(ratio * 100, 1),
                },
            )

    def _langfuse_metadata(
        self,
        tenant_id: int,
        model_id: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        meta: Dict[str, Any] = {
            "trace_user_id": f"tenant_{tenant_id}",
            "session_id": f"tenant_{tenant_id}",
            "tags": [f"tenant_id:{tenant_id}", f"model:{model_id}"],
        }
        if extra:
            rule_id = extra.get("rule_id")
            phase = extra.get("phase", "llm")
            if rule_id:
                meta["tags"].append(f"rule_id:{rule_id}")
            meta["tags"].append(f"phase:{phase}")
        return meta

    async def chat(
        self,
        messages: List[dict],
        config: TenantAiConfig,
        tenant_id: int = 0,
        trace_metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        self._check_quota(config, tenant_id)
        model_id = build_litellm_model_id(config.provider, config.model)
        metadata = self._langfuse_metadata(tenant_id, model_id, trace_metadata)

        last_exc: Exception = RuntimeError("nenhuma tentativa executada")
        for attempt in range(3):
            try:
                response = await litellm.acompletion(
                    model=model_id,
                    messages=messages,
                    api_key=config.api_key,
                    timeout=90,
                    metadata=metadata,
                )
                content: str = response.choices[0].message.content or ""
                logger.info(
                    "LLM completion concluída",
                    extra={
                        "tenant_id": tenant_id,
                        "model": model_id,
                        "prompt_tokens": getattr(response.usage, "prompt_tokens", 0),
                        "completion_tokens": getattr(response.usage, "completion_tokens", 0),
                    },
                )
                return content
            except Exception as exc:
                last_exc = exc
                if not _is_retryable(exc) or attempt == 2:
                    raise
                delay = 2.0**attempt
                logger.warning(
                    "LLM erro transitório, retentando",
                    extra={"attempt": attempt + 1, "delay": delay, "error": str(exc)},
                )
                await asyncio.sleep(delay)
        raise last_exc

    async def chat_stream(
        self,
        messages: List[dict],
        config: TenantAiConfig,
        tenant_id: int = 0,
        trace_metadata: Optional[Dict[str, Any]] = None,
    ) -> AsyncGenerator[str, None]:
        self._check_quota(config, tenant_id)
        model_id = build_litellm_model_id(config.provider, config.model)
        metadata = self._langfuse_metadata(tenant_id, model_id, trace_metadata)

        last_exc: Exception = RuntimeError("nenhuma tentativa executada")
        for attempt in range(3):
            try:
                response = await litellm.acompletion(
                    model=model_id,
                    messages=messages,
                    api_key=config.api_key,
                    stream=True,
                    timeout=120,
                    metadata=metadata,
                )
                # Se chegou aqui, conexão estabelecida — itera o stream
                async for chunk in response:
                    delta = chunk.choices[0].delta.content
                    if delta:
                        yield delta
                return
            except Exception as exc:
                last_exc = exc
                if not _is_retryable(exc) or attempt == 2:
                    raise
                delay = 2.0**attempt
                logger.warning(
                    "LLM stream erro transitório, retentando",
                    extra={"attempt": attempt + 1, "delay": delay, "error": str(exc)},
                )
                await asyncio.sleep(delay)
        raise last_exc
