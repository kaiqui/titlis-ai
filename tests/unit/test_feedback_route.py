import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.observability.metrics import ai_feedback_alerts_total, ai_user_feedback_total
from src.routes.feedback import _sentiment_counts
from src.settings import settings


@pytest.fixture(autouse=True)
def reset_sentiment_counts():
    _sentiment_counts.clear()
    yield
    _sentiment_counts.clear()


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def auth_headers():
    return {"X-Internal-Secret": settings.internal_secret}


def _counter_value(counter, **labels) -> float:
    try:
        return counter.labels(**labels)._value.get()
    except Exception:
        return 0.0


class TestFeedbackRoute:
    def test_thumbsdown_persiste_em_ai_feedback(self, client, auth_headers, mocker):
        mock_client = mocker.AsyncMock()
        mock_client.store_feedback = mocker.AsyncMock()
        mocker.patch("src.routes.feedback.get_scorecard_client", return_value=mock_client)

        resp = client.post(
            "/v1/feedback",
            json={
                "tenant_id": 1,
                "response_id": "resp-abc-123",
                "rule_id": "RES-003",
                "sentiment": "negative",
                "comment": "Explicação não ajudou",
                "provider": "openai",
                "model": "gpt-4o",
            },
            headers=auth_headers,
        )

        assert resp.status_code == 200
        mock_client.store_feedback.assert_called_once_with(
            tenant_id=1,
            response_id="resp-abc-123",
            rule_id="RES-003",
            sentiment="negative",
            comment="Explicação não ajudou",
        )

    def test_prometheus_counter_incrementado_no_feedback(self, client, auth_headers, mocker):
        mocker.patch(
            "src.routes.feedback.get_scorecard_client",
            return_value=mocker.AsyncMock(store_feedback=mocker.AsyncMock()),
        )

        before = _counter_value(
            ai_user_feedback_total,
            rule_id="RES-007",
            provider="anthropic",
            sentiment="positive",
        )

        client.post(
            "/v1/feedback",
            json={
                "tenant_id": 2,
                "response_id": "resp-xyz",
                "rule_id": "RES-007",
                "sentiment": "positive",
                "provider": "anthropic",
                "model": "claude-3-5-sonnet",
            },
            headers=auth_headers,
        )

        after = _counter_value(
            ai_user_feedback_total,
            rule_id="RES-007",
            provider="anthropic",
            sentiment="positive",
        )
        assert after - before == 1.0

    def test_alta_taxa_negativa_dispara_alerta(self, client, auth_headers, mocker):
        mocker.patch(
            "src.routes.feedback.get_scorecard_client",
            return_value=mocker.AsyncMock(store_feedback=mocker.AsyncMock()),
        )

        before_alert = _counter_value(ai_feedback_alerts_total, rule_id="SEC-001")

        # 1 positivo, 5 negativos → 83% negativo (> 30%)
        for _ in range(1):
            client.post(
                "/v1/feedback",
                json={"tenant_id": 1, "response_id": "r1", "rule_id": "SEC-001", "sentiment": "positive"},
                headers=auth_headers,
            )
        for i in range(5):
            client.post(
                "/v1/feedback",
                json={"tenant_id": 1, "response_id": f"r{i+2}", "rule_id": "SEC-001", "sentiment": "negative"},
                headers=auth_headers,
            )

        after_alert = _counter_value(ai_feedback_alerts_total, rule_id="SEC-001")
        assert after_alert > before_alert

    def test_sentiment_invalido_retorna_422(self, client, auth_headers):
        resp = client.post(
            "/v1/feedback",
            json={
                "tenant_id": 1,
                "response_id": "r1",
                "rule_id": "RES-001",
                "sentiment": "meh",
            },
            headers=auth_headers,
        )
        assert resp.status_code == 422

    def test_sem_secret_retorna_403(self, client):
        resp = client.post(
            "/v1/feedback",
            json={
                "tenant_id": 1,
                "response_id": "r1",
                "rule_id": "RES-001",
                "sentiment": "positive",
            },
        )
        assert resp.status_code == 403

    def test_falha_no_titlis_api_nao_quebra_response(self, client, auth_headers, mocker):
        mock_client = mocker.AsyncMock()
        mock_client.store_feedback = mocker.AsyncMock(side_effect=Exception("conexão recusada"))
        mocker.patch("src.routes.feedback.get_scorecard_client", return_value=mock_client)

        resp = client.post(
            "/v1/feedback",
            json={
                "tenant_id": 1,
                "response_id": "r1",
                "rule_id": "RES-003",
                "sentiment": "positive",
            },
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
