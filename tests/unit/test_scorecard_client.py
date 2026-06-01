import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from src.infrastructure.titlis_api.scorecard_client import ScorecardClient


def _make_response(status: int, body: dict | list | None = None, method: str = "GET") -> httpx.Response:
    import json

    content = json.dumps(body or {}).encode()
    request = httpx.Request(method, "http://titlis-api/test")
    return httpx.Response(status, content=content, headers={"content-type": "application/json"}, request=request)


@pytest.fixture
def client():
    with patch("src.infrastructure.titlis_api.scorecard_client.settings") as s:
        s.titlis_api_url = "http://titlis-api"
        s.internal_secret = "secret"
        c = ScorecardClient()
        yield c


class TestGetScorecardByUid:
    @pytest.mark.asyncio
    async def test_returns_scorecard_on_200(self, client):
        data = {"workload_id": "uid-1", "overall_score": 80}
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=_make_response(200, data))
        result = await client.get_scorecard_by_uid(1, "uid-1")
        assert result == data

    @pytest.mark.asyncio
    async def test_returns_none_on_404(self, client):
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=_make_response(404))
        result = await client.get_scorecard_by_uid(1, "missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_raises_on_500(self, client):
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=_make_response(500))
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_scorecard_by_uid(1, "uid-1")


class TestGetScorecardByName:
    @pytest.mark.asyncio
    async def test_returns_scorecard(self, client):
        data = {"workload_id": "uid-1"}
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=_make_response(200, data))
        result = await client.get_scorecard_by_name(1, "payment-api", "production")
        assert result == data

    @pytest.mark.asyncio
    async def test_returns_none_on_404(self, client):
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=_make_response(404))
        result = await client.get_scorecard_by_name(1, "missing", "ns")
        assert result is None


class TestGetDashboard:
    @pytest.mark.asyncio
    async def test_returns_list(self, client):
        data = [{"workload_id": "uid-1"}]
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=_make_response(200, data))
        result = await client.get_dashboard(1)
        assert result == data

    @pytest.mark.asyncio
    async def test_passes_cluster_param(self, client):
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=_make_response(200, []))
        await client.get_dashboard(1, cluster="prod-cluster")
        call_params = client._client.get.call_args[1]["params"]
        assert call_params["cluster"] == "prod-cluster"


class TestGetSimilarResolved:
    @pytest.mark.asyncio
    async def test_returns_list(self, client):
        data = [{"workload_id": "uid-2"}]
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=_make_response(200, data))
        result = await client.get_similar_resolved(1, "RES-003")
        assert result == data


class TestGetSlos:
    @pytest.mark.asyncio
    async def test_returns_slos(self, client):
        data = [{"id": 1, "target": 0.99}]
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=_make_response(200, data))
        result = await client.get_slos(1)
        assert result == data

    @pytest.mark.asyncio
    async def test_passes_namespace_filter(self, client):
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=_make_response(200, []))
        await client.get_slos(1, namespace="production")
        call_params = client._client.get.call_args[1]["params"]
        assert call_params["namespace"] == "production"


class TestGetRemediationHistory:
    @pytest.mark.asyncio
    async def test_returns_history(self, client):
        data = [{"pr_number": 42}]
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=_make_response(200, data))
        result = await client.get_remediation_history(1, "uid-1")
        assert result == data


class TestStoreFeedback:
    @pytest.mark.asyncio
    async def test_posts_feedback(self, client):
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=_make_response(200, method="POST"))
        await client.store_feedback(1, "resp-1", "RES-003", "positive", "great")
        client._client.post.assert_called_once()
        call_json = client._client.post.call_args[1]["json"]
        assert call_json["sentiment"] == "positive"
        assert call_json["ruleId"] == "RES-003"


class TestNotifyRemediationStarted:
    @pytest.mark.asyncio
    async def test_posts_remediation_event(self, client):
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=_make_response(200, method="POST"))
        await client.notify_remediation_started(
            tenant_id=1,
            workload_id="uid-1",
            pr_url="https://github.com/org/repo/pull/42",
            pr_number=42,
            github_branch="fix/auto",
            repo_url="https://github.com/org/repo",
            finding_ids=["RES-003"],
        )
        client._client.post.assert_called_once()
        call_json = client._client.post.call_args[1]["json"]
        assert call_json["workloadId"] == "uid-1"
        assert call_json["prNumber"] == 42
        assert "RES-003" in call_json["findingIds"]


class TestProposeSloChange:
    @pytest.mark.asyncio
    async def test_returns_response(self, client):
        data = {"id": 5, "status": "pending"}
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=_make_response(200, data, method="POST"))
        result = await client.propose_slo_change(1, 5, "target", "0.95", "0.99")
        assert result == data


class TestRetryLogic:
    @pytest.mark.asyncio
    async def test_retries_on_connect_error(self, client):
        good_response = _make_response(200, {"ok": True})
        client._client = AsyncMock()
        client._client.get = AsyncMock(
            side_effect=[httpx.ConnectError("timeout"), good_response]
        )
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await client.get_scorecard_by_uid(1, "uid-1")
        assert result == {"ok": True}
        assert client._client.get.call_count == 2

    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self, client):
        client._client = AsyncMock()
        client._client.get = AsyncMock(side_effect=httpx.ConnectError("timeout"))
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(httpx.ConnectError):
                await client.get_scorecard_by_uid(1, "uid-1")
        assert client._client.get.call_count == 3
