from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.observability.metrics import ai_requests_total
from src.settings import settings

_patch_rag = patch("src.routes.explain._retrieve_chunks", new=AsyncMock(return_value=[]))


def _explain_payload(provider: str = "openai", model: str = "gpt-4o", rule_id: str = "RES-003") -> dict:
    return {
        "tenant_id": 1,
        "workload_id": 42,
        "finding": {
            "rule_id": rule_id,
            "pillar": "resources",
            "severity": "high",
            "actual_value": None,
            "expected_value": "100m",
            "deployment_name": "payment-api",
            "namespace": "production",
        },
        "ai_config": {
            "provider": provider,
            "model": model,
            "api_key": "sk-test",
        },
    }


def _fake_stream():
    async def _gen(*args, **kwargs):
        c = MagicMock()
        c.choices[0].delta.content = "ok"
        yield c

    return _gen()


def _counter_value(counter, **labels) -> float:
    try:
        return counter.labels(**labels)._value.get()
    except Exception:
        return 0.0


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def auth_headers():
    return {"X-Internal-Secret": settings.internal_secret}


class TestPrometheusMetrics:
    def test_metrics_endpoint_returns_prometheus_format(self, client):
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "titlis_ai_requests_total" in resp.text

    def test_ai_requests_total_incremented_after_explain(self, client, auth_headers):
        before = _counter_value(
            ai_requests_total,
            tenant_id="1",
            provider="openai",
            model="gpt-4o",
            rule_id="RES-003",
            status="success",
        )

        with _patch_rag, patch("litellm.acompletion", new=AsyncMock(return_value=_fake_stream())):
            resp = client.post(
                "/v1/explain",
                json=_explain_payload(),
                headers=auth_headers,
            )
        _ = resp.content

        after = _counter_value(
            ai_requests_total,
            tenant_id="1",
            provider="openai",
            model="gpt-4o",
            rule_id="RES-003",
            status="success",
        )
        assert after - before == 1.0

    def test_ai_requests_total_labels_correct(self, client, auth_headers):
        before = _counter_value(
            ai_requests_total,
            tenant_id="1",
            provider="anthropic",
            model="claude-3-5-sonnet",
            rule_id="SEC-001",
            status="success",
        )

        with _patch_rag, patch("litellm.acompletion", new=AsyncMock(return_value=_fake_stream())):
            resp = client.post(
                "/v1/explain",
                json=_explain_payload(provider="anthropic", model="claude-3-5-sonnet", rule_id="SEC-001"),
                headers=auth_headers,
            )
        _ = resp.content

        after = _counter_value(
            ai_requests_total,
            tenant_id="1",
            provider="anthropic",
            model="claude-3-5-sonnet",
            rule_id="SEC-001",
            status="success",
        )
        assert after - before == 1.0
