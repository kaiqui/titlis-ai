import pytest
from unittest.mock import AsyncMock

from src.tools.read_tools import build_read_tools


def _client(*, by_name=None, by_uid=None, similar=None, dashboard=None, history=None):
    c = AsyncMock()
    c.get_scorecard_by_name = AsyncMock(return_value=by_name)
    c.get_scorecard_by_uid = AsyncMock(return_value=by_uid)
    c.get_similar_resolved = AsyncMock(return_value=similar or [])
    c.get_dashboard = AsyncMock(return_value=dashboard or [])
    c.get_remediation_history = AsyncMock(return_value=history or [])
    return c


TENANT = 1
SCORECARD = {
    "workload_id": "uid-abc",
    "workload": "payment-api",
    "namespace": "production",
    "overall_score": 75,
    "validation_results": [],
}


class TestListAllWorkloads:
    @pytest.mark.asyncio
    async def test_returns_all_workloads(self):
        dashboard = [
            {"workload": "api", "namespace": "production", "overall_score": 45},
            {"workload": "worker", "namespace": "staging", "overall_score": 90},
        ]
        client = _client(dashboard=dashboard)
        registry = build_read_tools(client, TENANT)
        tool = registry.get("list_all_workloads")
        result = await tool.handler()
        assert len(result) == 2
        client.get_dashboard.assert_called_once_with(TENANT)

    @pytest.mark.asyncio
    async def test_tenant_isolation(self):
        client = _client(dashboard=[])
        registry = build_read_tools(client, tenant_id=42)
        tool = registry.get("list_all_workloads")
        await tool.handler()
        client.get_dashboard.assert_called_once_with(42)


class TestGetDeploymentSpec:
    @pytest.mark.asyncio
    async def test_returns_scorecard_data(self):
        client = _client(by_name=SCORECARD)
        registry = build_read_tools(client, TENANT)
        tool = registry.get("get_deployment_spec")
        result = await tool.handler(namespace="production", name="payment-api")
        assert result["workload"] == "payment-api"
        client.get_scorecard_by_name.assert_called_once_with(TENANT, "payment-api", "production")

    @pytest.mark.asyncio
    async def test_workload_not_found_returns_error(self):
        client = _client(by_name=None)
        registry = build_read_tools(client, TENANT)
        tool = registry.get("get_deployment_spec")
        result = await tool.handler(namespace="production", name="unknown")
        assert result["error"] == "workload_not_found"

    @pytest.mark.asyncio
    async def test_tenant_isolation(self):
        client = _client(by_name=None)
        registry = build_read_tools(client, tenant_id=99)
        tool = registry.get("get_deployment_spec")
        await tool.handler(namespace="ns", name="app")
        client.get_scorecard_by_name.assert_called_once_with(99, "app", "ns")


class TestGetCurrentScorecard:
    @pytest.mark.asyncio
    async def test_returns_scorecard(self):
        client = _client(by_uid=SCORECARD)
        registry = build_read_tools(client, TENANT)
        tool = registry.get("get_current_scorecard")
        result = await tool.handler(workload_id="uid-abc")
        assert result["workload_id"] == "uid-abc"

    @pytest.mark.asyncio
    async def test_not_found_returns_error(self):
        client = _client(by_uid=None)
        registry = build_read_tools(client, TENANT)
        tool = registry.get("get_current_scorecard")
        result = await tool.handler(workload_id="missing")
        assert result["error"] == "workload_not_found"


class TestGetHpaConfig:
    @pytest.mark.asyncio
    async def test_returns_null_hpa(self):
        client = _client()
        registry = build_read_tools(client, TENANT)
        tool = registry.get("get_hpa_config")
        result = await tool.handler(namespace="ns", name="app")
        assert result["hpa"] is None
        assert result["namespace"] == "ns"


class TestGetSimilarResolved:
    @pytest.mark.asyncio
    async def test_returns_similar_workloads(self):
        similar = [{"workload": "other-api", "rule_id": "RES-003"}]
        client = _client(similar=similar)
        registry = build_read_tools(client, TENANT)
        tool = registry.get("get_similar_resolved")
        result = await tool.handler(rule_id="RES-003", pillar="resilience")
        assert len(result) == 1
        assert result[0]["workload"] == "other-api"


class TestGetNamespaceInventory:
    @pytest.mark.asyncio
    async def test_filters_by_namespace(self):
        dashboard = [
            {"workload": "api", "namespace": "production"},
            {"workload": "worker", "namespace": "staging"},
        ]
        client = _client(dashboard=dashboard)
        registry = build_read_tools(client, TENANT)
        tool = registry.get("get_namespace_inventory")
        result = await tool.handler(namespace="production")
        assert len(result) == 1
        assert result[0]["workload"] == "api"


class TestGetRemediationHistory:
    @pytest.mark.asyncio
    async def test_returns_history(self):
        history = [{"status": "MERGED", "github_pr_url": "https://github.com/org/repo/pull/1"}]
        client = _client(history=history)
        registry = build_read_tools(client, TENANT)
        tool = registry.get("get_remediation_history")
        result = await tool.handler(workload_id="uid-abc")
        assert len(result) == 1
        assert result[0]["status"] == "MERGED"
