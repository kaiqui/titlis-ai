from typing import List

import litellm

from src.utils.logger import get_logger

logger = get_logger(__name__)

_PROVIDER_MODEL: dict = {
    "openai": "openai/text-embedding-3-small",
    "cohere": "cohere/embed-english-v3",
    "ollama": "ollama/nomic-embed-text",
    "anthropic": "openai/text-embedding-3-small",
    "google": "gemini/text-embedding-004",
    "gemini": "gemini/text-embedding-004",
    "mistral": "openai/text-embedding-3-small",
    "azure": "openai/text-embedding-3-small",
}

_EMBEDDING_DIMS = 1536


class EmbeddingService:
    async def embed(self, text: str, provider: str, api_key: str) -> List[float]:
        model = _PROVIDER_MODEL.get(provider, "openai/text-embedding-3-small")
        response = await litellm.aembedding(model=model, input=[text], api_key=api_key)
        embedding: List[float] = response.data[0]["embedding"]
        logger.info(
            "Embedding gerado",
            extra={"provider": provider, "model": model, "dims": len(embedding)},
        )
        return embedding
