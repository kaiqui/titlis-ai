import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.domain.models import TenantAiConfig
from src.services.llm_service import LLMService, QuotaExceededError


def _config(budget: int = None, used: int = 0) -> TenantAiConfig:
    return TenantAiConfig(
        provider="openai",
        model="gpt-4o",
        api_key="sk-test",
        monthly_token_budget=budget,
        tokens_used_month=used,
    )


def _mock_completion(content: str = "resposta do LLM") -> MagicMock:
    choice = MagicMock()
    choice.message.content = content
    choice.delta.content = content
    response = MagicMock()
    response.choices = [choice]
    response.usage.prompt_tokens = 10
    response.usage.completion_tokens = 20
    return response


class TestLLMServiceChat:
    @pytest.mark.asyncio
    async def test_chat_happy_path(self) -> None:
        service = LLMService()
        config = _config()
        mock_response = _mock_completion("Explicação da regra RES-001")

        with patch("litellm.acompletion", new=AsyncMock(return_value=mock_response)):
            result = await service.chat([{"role": "user", "content": "explique"}], config, tenant_id=1)

        assert result == "Explicação da regra RES-001"

    @pytest.mark.asyncio
    async def test_chat_quota_exceeded_raises(self) -> None:
        service = LLMService()
        config = _config(budget=1000, used=1000)

        with pytest.raises(QuotaExceededError) as exc_info:
            await service.chat([{"role": "user", "content": "x"}], config, tenant_id=7)

        assert exc_info.value.tenant_id == 7
        assert exc_info.value.budget == 1000

    @pytest.mark.asyncio
    async def test_chat_no_budget_passes_without_check(self) -> None:
        service = LLMService()
        config = _config(budget=None, used=99999)
        mock_response = _mock_completion("ok")

        with patch("litellm.acompletion", new=AsyncMock(return_value=mock_response)):
            result = await service.chat([{"role": "user", "content": "x"}], config)

        assert result == "ok"

    @pytest.mark.asyncio
    async def test_chat_litellm_provider_error_propagates(self) -> None:
        service = LLMService()
        config = _config()

        with patch("litellm.acompletion", new=AsyncMock(side_effect=Exception("invalid api key"))):
            with pytest.raises(Exception, match="invalid api key"):
                await service.chat([{"role": "user", "content": "x"}], config)


class TestLLMServiceTokenBudget:
    @pytest.mark.asyncio
    async def test_budget_80_percent_emits_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        service = LLMService()
        config = _config(budget=1000, used=800)
        mock_response = _mock_completion("ok")

        with patch("litellm.acompletion", new=AsyncMock(return_value=mock_response)):
            with caplog.at_level(logging.WARNING):
                await service.chat([{"role": "user", "content": "x"}], config, tenant_id=5)

        assert any("80%" in r.message or "budget" in r.message.lower() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_budget_100_percent_raises_quota_error(self) -> None:
        service = LLMService()
        config = _config(budget=500, used=500)

        with pytest.raises(QuotaExceededError):
            await service.chat([{"role": "user", "content": "x"}], config, tenant_id=3)

    @pytest.mark.asyncio
    async def test_budget_reset_allows_chat(self) -> None:
        service = LLMService()
        config_before = _config(budget=500, used=500)
        config_after = _config(budget=500, used=0)
        mock_response = _mock_completion("ok")

        with pytest.raises(QuotaExceededError):
            await service.chat([], config_before, tenant_id=1)

        with patch("litellm.acompletion", new=AsyncMock(return_value=mock_response)):
            result = await service.chat([], config_after, tenant_id=1)

        assert result == "ok"


class TestLLMServiceChatStream:
    @pytest.mark.asyncio
    async def test_stream_yields_chunks(self) -> None:
        service = LLMService()
        config = _config()

        chunk1, chunk2, chunk3 = MagicMock(), MagicMock(), MagicMock()
        chunk1.choices[0].delta.content = "Olá "
        chunk2.choices[0].delta.content = "mundo"
        chunk3.choices[0].delta.content = None

        async def fake_stream(*args, **kwargs):
            for c in [chunk1, chunk2, chunk3]:
                yield c

        with patch("litellm.acompletion", new=AsyncMock(return_value=fake_stream())):
            chunks = []
            async for c in service.chat_stream([], config, tenant_id=1):
                chunks.append(c)

        assert chunks == ["Olá ", "mundo"]

    @pytest.mark.asyncio
    async def test_stream_raises_quota_exceeded_before_llm_call(self) -> None:
        service = LLMService()
        config = _config(budget=100, used=100)

        with pytest.raises(QuotaExceededError):
            async for _ in service.chat_stream([], config, tenant_id=2):
                pass
