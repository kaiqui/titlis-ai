from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

from src.main import app
from src.settings import settings


def _explain_payload(budget: int = None, used: int = 0) -> dict:
    return {
        "tenant_id": 1,
        "workload_id": 10,
        "finding": {
            "rule_id": "RES-003",
            "pillar": "resilience",
            "severity": "error",
            "actual_value": None,
            "expected_value": "100m",
            "deployment_name": "payment-api",
            "namespace": "production",
        },
        "ai_config": {
            "provider": "openai",
            "model": "gpt-4o",
            "api_key": "sk-test",
            "monthly_token_budget": budget,
            "tokens_used_month": used,
        },
    }


def _internal_headers() -> dict:
    return {"X-Internal-Secret": settings.internal_secret}


# Patches the RAG retrieval path so tests remain focused on LLM streaming behaviour.
_patch_rag = patch("src.routes.explain._retrieve_chunks", new=AsyncMock(return_value=[]))


class TestExplainRoute:
    def test_explain_returns_sse_chunks(self) -> None:
        client = TestClient(app, raise_server_exceptions=False)

        async def fake_stream(*args, **kwargs):
            for text in ["Explicação ", "da regra"]:
                c = MagicMock()
                c.choices[0].delta.content = text
                yield c

        with _patch_rag, patch("litellm.acompletion", new=AsyncMock(return_value=fake_stream())):
            response = client.post(
                "/v1/explain",
                json=_explain_payload(),
                headers=_internal_headers(),
            )

        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]
        body = response.text
        assert "chunk" in body
        assert "Explicação" in body or "regra" in body
        assert '"type": "done"' in body or '"type":"done"' in body

    def test_explain_returns_403_without_internal_secret(self) -> None:
        client = TestClient(app)
        response = client.post("/v1/explain", json=_explain_payload())
        assert response.status_code == 403

    def test_explain_streams_quota_exceeded_error(self) -> None:
        client = TestClient(app, raise_server_exceptions=False)

        with _patch_rag:
            response = client.post(
                "/v1/explain",
                json=_explain_payload(budget=100, used=100),
                headers=_internal_headers(),
            )

        assert response.status_code == 200
        body = response.text
        assert "quota_exceeded" in body

    def test_explain_returns_422_on_missing_field(self) -> None:
        client = TestClient(app)
        response = client.post(
            "/v1/explain",
            json={"tenant_id": 1},
            headers=_internal_headers(),
        )
        assert response.status_code == 422

    def test_explain_injects_rag_chunks_into_prompt(self) -> None:
        client = TestClient(app, raise_server_exceptions=False)

        chunks = [{"chunkText": "Liveness probe deve apontar para /healthz"}]

        async def fake_stream(*args, **kwargs):
            c = MagicMock()
            c.choices[0].delta.content = "ok"
            yield c

        mock_acompletion = AsyncMock(return_value=fake_stream())
        with (
            patch("src.routes.explain._retrieve_chunks", new=AsyncMock(return_value=chunks)),
            patch("litellm.acompletion", new=mock_acompletion),
        ):
            client.post(
                "/v1/explain",
                json=_explain_payload(),
                headers=_internal_headers(),
            )

        messages = mock_acompletion.call_args.kwargs.get("messages", [])
        user_msg = next((m for m in messages if m.get("role") == "user"), None)
        assert user_msg is not None
        assert "Liveness probe" in user_msg["content"]

    def test_explain_continues_when_rag_returns_empty(self) -> None:
        client = TestClient(app, raise_server_exceptions=False)

        async def fake_stream(*args, **kwargs):
            c = MagicMock()
            c.choices[0].delta.content = "resposta"
            yield c

        with (
            patch("src.routes.explain._retrieve_chunks", new=AsyncMock(return_value=[])),
            patch("litellm.acompletion", new=AsyncMock(return_value=fake_stream())),
        ):
            response = client.post(
                "/v1/explain",
                json=_explain_payload(),
                headers=_internal_headers(),
            )

        assert response.status_code == 200
        assert "resposta" in response.text
