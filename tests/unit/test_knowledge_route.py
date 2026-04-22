from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient

from src.main import app
from src.settings import settings


def _headers() -> dict:
    return {"X-Internal-Secret": settings.internal_secret}


class TestKnowledgeSeedRoute:
    def test_seed_returns_count(self) -> None:
        client = TestClient(app)
        with patch("src.routes.knowledge.get_knowledge_seeder") as mock_get_seeder:
            seeder = AsyncMock()
            seeder.seed_global_rules = AsyncMock(return_value=27)
            mock_get_seeder.return_value = seeder

            response = client.post(
                "/v1/knowledge/seed",
                json={"provider": "openai", "api_key": "sk-test"},
                headers=_headers(),
            )

        assert response.status_code == 200
        assert response.json()["seeded"] == 27

    def test_seed_returns_403_without_secret(self) -> None:
        client = TestClient(app)
        response = client.post(
            "/v1/knowledge/seed",
            json={"provider": "openai", "api_key": "sk-test"},
        )
        assert response.status_code == 403

    def test_seed_with_tenant_id(self) -> None:
        client = TestClient(app)
        with patch("src.routes.knowledge.get_knowledge_seeder") as mock_get_seeder:
            seeder = AsyncMock()
            seeder.seed_global_rules = AsyncMock(return_value=5)
            mock_get_seeder.return_value = seeder

            response = client.post(
                "/v1/knowledge/seed",
                json={"provider": "openai", "api_key": "sk-test", "tenant_id": 42},
                headers=_headers(),
            )

        assert response.status_code == 200
        seeder.seed_global_rules.assert_called_once_with(provider="openai", api_key="sk-test", tenant_id=42)
