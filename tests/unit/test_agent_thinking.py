import pytest
from unittest.mock import MagicMock, patch

from src.domain.models import TenantAiConfig
from src.services.agent_service import _extract_thinking, _needs_thinking_param


# ── _extract_thinking ─────────────────────────────────────────────────────────


class TestExtractThinking:
    def test_returns_empty_when_no_thinking_blocks(self):
        delta = MagicMock(spec=[])
        assert _extract_thinking(delta) == ""

    def _delta(self, thinking_blocks=None, reasoning_content=None):
        delta = MagicMock()
        delta.thinking_blocks = thinking_blocks
        delta.reasoning_content = reasoning_content
        return delta

    def test_returns_empty_when_thinking_blocks_is_none(self):
        assert _extract_thinking(self._delta(thinking_blocks=None)) == ""

    def test_returns_empty_when_thinking_blocks_is_empty_list(self):
        assert _extract_thinking(self._delta(thinking_blocks=[])) == ""

    def test_extracts_from_dict_block(self):
        delta = self._delta(thinking_blocks=[{"type": "thinking", "thinking": "passo 1: analisar"}])
        assert _extract_thinking(delta) == "passo 1: analisar"

    def test_extracts_from_object_block(self):
        block = MagicMock()
        block.thinking = "raciocínio do modelo"
        assert _extract_thinking(self._delta(thinking_blocks=[block])) == "raciocínio do modelo"

    def test_concatenates_multiple_blocks(self):
        delta = self._delta(thinking_blocks=[{"thinking": "parte 1"}, {"thinking": " parte 2"}])
        assert _extract_thinking(delta) == "parte 1 parte 2"

    def test_skips_dict_block_without_thinking_key(self):
        assert _extract_thinking(self._delta(thinking_blocks=[{"type": "thinking"}])) == ""

    def test_skips_object_block_with_none_thinking(self):
        block = MagicMock()
        block.thinking = None
        assert _extract_thinking(self._delta(thinking_blocks=[block])) == ""

    def test_extracts_reasoning_content_openai_style(self):
        delta = self._delta(reasoning_content="passo a passo: verificar recursos")
        assert _extract_thinking(delta) == "passo a passo: verificar recursos"

    def test_reasoning_content_takes_lower_priority_than_thinking_blocks(self):
        block = MagicMock()
        block.thinking = "bloco anthropic"
        delta = self._delta(thinking_blocks=[block], reasoning_content="raciocínio openai")
        assert _extract_thinking(delta) == "bloco anthropic"

    def test_returns_empty_when_neither_field_present(self):
        delta = MagicMock(spec=[])
        assert _extract_thinking(delta) == ""


# ── _stream_llm thinking kwargs ───────────────────────────────────────────────


def _make_config(provider: str, model: str) -> TenantAiConfig:
    return TenantAiConfig(provider=provider, model=model, api_key="sk-test")


def _make_chunk(content: str = "", thinking_blocks=None, finish_reason=None, reasoning_content=None):
    delta = MagicMock()
    delta.content = content
    delta.tool_calls = None
    delta.thinking_blocks = thinking_blocks
    delta.reasoning_content = reasoning_content
    choice = MagicMock()
    choice.delta = delta
    choice.finish_reason = finish_reason
    chunk = MagicMock()
    chunk.choices = [choice]
    return chunk


async def _collect_stream(coro):
    chunks = []
    async for item in coro:
        chunks.append(item)
    return chunks


def _fake_acompletion(chunks, captured_kwargs=None):
    async def acompletion(**kwargs):
        if captured_kwargs is not None:
            captured_kwargs.update(kwargs)

        async def gen():
            for c in chunks:
                yield c

        return gen()

    return acompletion


# ── _needs_thinking_param ─────────────────────────────────────────────────────


class TestNeedsThinkingParam:
    def test_anthropic_always_true(self):
        assert _needs_thinking_param("anthropic", "claude-sonnet-4-5-20251001") is True
        assert _needs_thinking_param("anthropic", "claude-3-haiku-20240307") is True
        assert _needs_thinking_param("anthropic", "claude-future-99") is True

    def test_gemini_2_5_flash_true(self):
        assert _needs_thinking_param("google", "gemini-2.5-flash") is True
        assert _needs_thinking_param("gemini", "gemini-2.5-flash") is True

    def test_gemini_2_5_pro_true(self):
        assert _needs_thinking_param("google", "gemini-2.5-pro") is True

    def test_gemini_2_5_flash_lite_true(self):
        assert _needs_thinking_param("google", "gemini-2.5-flash-lite") is True

    def test_gemini_3_true(self):
        assert _needs_thinking_param("google", "gemini-3-flash-preview") is True

    def test_gemini_2_0_false(self):
        assert _needs_thinking_param("google", "gemini-2.0-flash") is False

    def test_gemini_1_5_false(self):
        assert _needs_thinking_param("google", "gemini-1.5-pro") is False

    def test_openai_false(self):
        assert _needs_thinking_param("openai", "o4-mini") is False
        assert _needs_thinking_param("openai", "gpt-4o") is False

    def test_unknown_provider_false(self):
        assert _needs_thinking_param("mistral", "mistral-large") is False


class TestStreamLlmThinking:
    @pytest.mark.asyncio
    async def test_any_anthropic_model_adds_thinking_kwarg(self):
        from src.services.agent_service import AgentService
        from src.pipeline.session import AgentSession

        for model in ["claude-sonnet-4-5-20251001", "claude-3-haiku-20240307", "claude-future-99"]:
            session = MagicMock(spec=AgentSession)
            session.tenant_id = 1
            config = _make_config("anthropic", model)
            captured_kwargs = {}

            service = AgentService.__new__(AgentService)
            result = {}

            with patch(
                "litellm.acompletion",
                side_effect=_fake_acompletion([_make_chunk(finish_reason="end_turn")], captured_kwargs),
            ):
                async for _ in service._stream_llm([], [], f"anthropic/{model}", config, session, result):
                    pass

            assert "thinking" in captured_kwargs, f"thinking missing for model {model}"
            assert captured_kwargs["thinking"] == {"type": "enabled", "budget_tokens": 8000}

    @pytest.mark.asyncio
    async def test_non_anthropic_does_not_add_thinking_kwarg(self):
        from src.services.agent_service import AgentService
        from src.pipeline.session import AgentSession

        session = MagicMock(spec=AgentSession)
        session.tenant_id = 1
        config = _make_config("openai", "gpt-4o")
        captured_kwargs = {}

        service = AgentService.__new__(AgentService)
        result = {}

        with patch(
            "litellm.acompletion", side_effect=_fake_acompletion([_make_chunk(finish_reason="stop")], captured_kwargs)
        ):
            async for _ in service._stream_llm([], [], "openai/gpt-4o", config, session, result):
                pass

        assert "thinking" not in captured_kwargs

    @pytest.mark.asyncio
    async def test_thinking_chunks_are_yielded_for_anthropic(self):
        from src.services.agent_service import AgentService
        from src.pipeline.session import AgentSession

        session = MagicMock(spec=AgentSession)
        session.tenant_id = 1
        config = _make_config("anthropic", "claude-sonnet-4-5-20251001")
        chunks = [
            _make_chunk(thinking_blocks=[{"thinking": "analisando deployment"}]),
            _make_chunk(finish_reason="end_turn"),
        ]

        service = AgentService.__new__(AgentService)
        result = {}

        with patch("litellm.acompletion", side_effect=_fake_acompletion(chunks)):
            yielded = await _collect_stream(
                service._stream_llm([], [], "anthropic/claude-sonnet-4-5-20251001", config, session, result)
            )

        assert "analisando deployment" in yielded
        assert result["thinking_acc"] == "analisando deployment"

    @pytest.mark.asyncio
    async def test_text_chunks_not_yielded_for_anthropic_thinking_model(self):
        from src.services.agent_service import AgentService
        from src.pipeline.session import AgentSession

        session = MagicMock(spec=AgentSession)
        session.tenant_id = 1
        config = _make_config("anthropic", "claude-sonnet-4-5-20251001")
        chunks = [_make_chunk(content="resposta final"), _make_chunk(finish_reason="end_turn")]

        service = AgentService.__new__(AgentService)
        result = {}

        with patch("litellm.acompletion", side_effect=_fake_acompletion(chunks)):
            yielded = await _collect_stream(
                service._stream_llm([], [], "anthropic/claude-sonnet-4-5-20251001", config, session, result)
            )

        assert "resposta final" not in yielded
        assert result["text_acc"] == "resposta final"

    @pytest.mark.asyncio
    async def test_text_chunks_never_yielded_for_any_provider(self):
        from src.services.agent_service import AgentService
        from src.pipeline.session import AgentSession

        for provider, model, model_id in [
            ("openai", "gpt-4o", "openai/gpt-4o"),
            ("anthropic", "claude-sonnet-4-5-20251001", "anthropic/claude-sonnet-4-5-20251001"),
            ("google", "gemini-2.0-flash", "gemini/gemini-2.0-flash"),
        ]:
            session = MagicMock(spec=AgentSession)
            session.tenant_id = 1
            config = _make_config(provider, model)
            finish = "stop" if provider != "anthropic" else "end_turn"
            chunks = [_make_chunk(content="resposta do modelo"), _make_chunk(finish_reason=finish)]

            service = AgentService.__new__(AgentService)
            result = {}

            with patch("litellm.acompletion", side_effect=_fake_acompletion(chunks)):
                yielded = await _collect_stream(service._stream_llm([], [], model_id, config, session, result))

            assert "resposta do modelo" not in yielded, f"texto vazou como thinking para {provider}"
            assert result["text_acc"] == "resposta do modelo"

    @pytest.mark.asyncio
    async def test_reasoning_content_yielded_for_openai_o_series(self):
        from src.services.agent_service import AgentService
        from src.pipeline.session import AgentSession

        session = MagicMock(spec=AgentSession)
        session.tenant_id = 1
        config = _make_config("openai", "o4-mini")
        reasoning_chunk = _make_chunk()
        reasoning_chunk.choices[0].delta.reasoning_content = "verificando limits do container"
        reasoning_chunk.choices[0].delta.thinking_blocks = None
        finish_chunk = _make_chunk(finish_reason="stop")

        service = AgentService.__new__(AgentService)
        result = {}

        with patch("litellm.acompletion", side_effect=_fake_acompletion([reasoning_chunk, finish_chunk])):
            yielded = await _collect_stream(service._stream_llm([], [], "openai/o4-mini", config, session, result))

        assert "verificando limits do container" in yielded
        assert result["thinking_acc"] == "verificando limits do container"

    @pytest.mark.asyncio
    async def test_gemini_2_5_adds_thinking_kwarg(self):
        from src.services.agent_service import AgentService
        from src.pipeline.session import AgentSession

        for model in ["gemini-2.5-flash", "gemini-2.5-pro", "gemini-2.5-flash-lite"]:
            session = MagicMock(spec=AgentSession)
            session.tenant_id = 1
            config = _make_config("google", model)
            captured_kwargs = {}

            service = AgentService.__new__(AgentService)
            result = {}

            with patch(
                "litellm.acompletion",
                side_effect=_fake_acompletion([_make_chunk(finish_reason="stop")], captured_kwargs),
            ):
                async for _ in service._stream_llm([], [], f"gemini/{model}", config, session, result):
                    pass

            assert "thinking" in captured_kwargs, f"thinking kwarg ausente para {model}"
            assert captured_kwargs["thinking"] == {"type": "enabled", "budget_tokens": 8000}

    @pytest.mark.asyncio
    async def test_gemini_2_0_does_not_add_thinking_kwarg(self):
        from src.services.agent_service import AgentService
        from src.pipeline.session import AgentSession

        session = MagicMock(spec=AgentSession)
        session.tenant_id = 1
        config = _make_config("google", "gemini-2.0-flash")
        captured_kwargs = {}

        service = AgentService.__new__(AgentService)
        result = {}

        with patch(
            "litellm.acompletion",
            side_effect=_fake_acompletion([_make_chunk(finish_reason="stop")], captured_kwargs),
        ):
            async for _ in service._stream_llm([], [], "gemini/gemini-2.0-flash", config, session, result):
                pass

        assert "thinking" not in captured_kwargs

    @pytest.mark.asyncio
    async def test_gemini_2_5_thinking_blocks_yielded(self):
        from src.services.agent_service import AgentService
        from src.pipeline.session import AgentSession

        session = MagicMock(spec=AgentSession)
        session.tenant_id = 1
        config = _make_config("google", "gemini-2.5-flash")
        chunks = [
            _make_chunk(thinking_blocks=[{"thinking": "analisando HPA do workload"}]),
            _make_chunk(finish_reason="stop"),
        ]

        service = AgentService.__new__(AgentService)
        result = {}

        with patch("litellm.acompletion", side_effect=_fake_acompletion(chunks)):
            yielded = await _collect_stream(
                service._stream_llm([], [], "gemini/gemini-2.5-flash", config, session, result)
            )

        assert "analisando HPA do workload" in yielded
        assert result["thinking_acc"] == "analisando HPA do workload"
