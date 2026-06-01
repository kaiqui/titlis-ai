import pytest
from unittest.mock import AsyncMock, patch

import httpx

from src.infrastructure.titlis_api.knowledge_client import KnowledgeClient


def _make_response(status: int, body=None, method: str = "POST") -> httpx.Response:
    import json

    content = json.dumps(body or {}).encode()
    request = httpx.Request(method, "http://titlis-api/test")
    return httpx.Response(status, content=content, headers={"content-type": "application/json"}, request=request)


@pytest.fixture
def client():
    with patch("src.infrastructure.titlis_api.knowledge_client.settings") as s:
        s.titlis_api_url = "http://titlis-api"
        s.internal_secret = "secret"
        c = KnowledgeClient()
        yield c


class TestIndexChunk:
    @pytest.mark.asyncio
    async def test_returns_chunk_id(self, client):
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=_make_response(200, {"id": "chunk-42"}))
        result = await client.index_chunk(
            tenant_id=1,
            source_type="rule_doc",
            source_id="RES-003",
            chunk_text="Always set cpu limits.",
            embedding=[0.1, 0.2, 0.3],
        )
        assert result == "chunk-42"

    @pytest.mark.asyncio
    async def test_passes_metadata_as_json_string(self, client):
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=_make_response(200, {"id": "chunk-1"}))
        await client.index_chunk(
            tenant_id=None,
            source_type="k8s_best_practice",
            source_id="hpa-doc",
            chunk_text="HPA requires resource requests.",
            embedding=[0.1],
            metadata={"pillar": "resilience"},
        )
        call_json = client._client.post.call_args[1]["json"]
        import json

        assert json.loads(call_json["metadata"]) == {"pillar": "resilience"}

    @pytest.mark.asyncio
    async def test_raises_on_error(self, client):
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=_make_response(500))
        with pytest.raises(httpx.HTTPStatusError):
            await client.index_chunk(1, "rule_doc", "RES-003", "text", [0.1])


class TestSearchSimilar:
    @pytest.mark.asyncio
    async def test_returns_chunks(self, client):
        data = [{"chunkText": "Use cpu limits", "score": 0.95}]
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=_make_response(200, data))
        result = await client.search_similar(tenant_id=1, embedding=[0.1, 0.2])
        assert result == data

    @pytest.mark.asyncio
    async def test_passes_correct_payload(self, client):
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=_make_response(200, []))
        await client.search_similar(tenant_id=1, embedding=[0.5], limit=3)
        call_json = client._client.post.call_args[1]["json"]
        assert call_json["tenantId"] == 1
        assert call_json["limit"] == 3

    @pytest.mark.asyncio
    async def test_raises_on_error(self, client):
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=_make_response(503))
        with pytest.raises(httpx.HTTPStatusError):
            await client.search_similar(tenant_id=1, embedding=[0.1])
