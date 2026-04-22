import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.embedding_service import EmbeddingService


class TestEmbeddingService:
    @pytest.mark.asyncio
    async def test_embed_returns_vector(self) -> None:
        fake_vector = [0.1] * 1536
        fake_response = MagicMock()
        fake_response.data = [{"embedding": fake_vector}]

        with patch("litellm.aembedding", new=AsyncMock(return_value=fake_response)):
            service = EmbeddingService()
            result = await service.embed("CPU request not set", "openai", "sk-test")

        assert len(result) == 1536
        assert result[0] == pytest.approx(0.1)

    @pytest.mark.asyncio
    async def test_embed_uses_correct_model_for_openai(self) -> None:
        fake_response = MagicMock()
        fake_response.data = [{"embedding": [0.0] * 1536}]

        with patch("litellm.aembedding", new=AsyncMock(return_value=fake_response)) as mock_embed:
            service = EmbeddingService()
            await service.embed("text", "openai", "sk-test")
            called_model = mock_embed.call_args.kwargs.get("model") or mock_embed.call_args.args[0]
            assert "text-embedding-3-small" in called_model

    @pytest.mark.asyncio
    async def test_embed_uses_cohere_model_for_cohere(self) -> None:
        fake_response = MagicMock()
        fake_response.data = [{"embedding": [0.0] * 1536}]

        with patch("litellm.aembedding", new=AsyncMock(return_value=fake_response)) as mock_embed:
            service = EmbeddingService()
            await service.embed("text", "cohere", "ck-test")
            call_kwargs = mock_embed.call_args.kwargs
            assert "cohere" in call_kwargs.get("model", "")

    @pytest.mark.asyncio
    async def test_embed_falls_back_to_openai_for_unknown_provider(self) -> None:
        fake_response = MagicMock()
        fake_response.data = [{"embedding": [0.0] * 1536}]

        with patch("litellm.aembedding", new=AsyncMock(return_value=fake_response)) as mock_embed:
            service = EmbeddingService()
            await service.embed("text", "unknown_provider", "key")
            call_kwargs = mock_embed.call_args.kwargs
            assert "text-embedding-3-small" in call_kwargs.get("model", "")
