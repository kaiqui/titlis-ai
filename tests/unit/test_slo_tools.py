import pytest
from unittest.mock import AsyncMock

from src.tools.slo_tools import SloValidationError, build_slo_tools

TENANT = 1

SLO_LIST = [
    {
        "slo_config_id": 10,
        "name": "payment-slo",
        "namespace": "production",
        "target": "99.9",
        "warning": "99.5",
        "timeframe": "30d",
        "datadog_slo_state": "OK",
    }
]


def _client(slos=None, propose_result=None):
    c = AsyncMock()
    c.get_slos = AsyncMock(return_value=slos if slos is not None else SLO_LIST)
    c.propose_slo_change = AsyncMock(return_value=propose_result or {"id": "uuid-1", "status": "pending"})
    return c


class TestGetSloStatus:
    @pytest.mark.asyncio
    async def test_returns_slo_by_config_id(self):
        client = _client()
        registry = build_slo_tools(client, TENANT)
        tool = registry.get("get_slo_status")
        result = await tool.handler(workload_id="10")
        assert result["slo_config_id"] == 10

    @pytest.mark.asyncio
    async def test_not_found_returns_error(self):
        client = _client(slos=[])
        registry = build_slo_tools(client, TENANT)
        tool = registry.get("get_slo_status")
        result = await tool.handler(workload_id="999")
        assert result["error"] == "slo_not_found"


class TestListAutoCreatedSlos:
    @pytest.mark.asyncio
    async def test_returns_all_slos(self):
        client = _client()
        registry = build_slo_tools(client, TENANT)
        tool = registry.get("list_auto_created_slos")
        result = await tool.handler()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_passes_namespace_filter(self):
        client = _client()
        registry = build_slo_tools(client, TENANT)
        tool = registry.get("list_auto_created_slos")
        await tool.handler(namespace="production")
        client.get_slos.assert_called_once_with(TENANT, "production")


class TestUpdateSloThresholds:
    @pytest.mark.asyncio
    async def test_happy_path_proposes_changes(self):
        client = _client()
        registry = build_slo_tools(client, TENANT)
        tool = registry.get("update_slo_thresholds")
        result = await tool.handler(slo_config_id=10, target="99.95")
        assert len(result["proposed_changes"]) == 1
        client.propose_slo_change.assert_called_once()

    @pytest.mark.asyncio
    async def test_target_le_warning_raises(self):
        client = _client()
        registry = build_slo_tools(client, TENANT)
        tool = registry.get("update_slo_thresholds")
        with pytest.raises(SloValidationError, match="target"):
            await tool.handler(slo_config_id=10, target="99.0", warning="99.5")

    @pytest.mark.asyncio
    async def test_404_propagated(self):
        import httpx

        client = _client()
        client.propose_slo_change = AsyncMock(
            side_effect=httpx.HTTPStatusError("Not Found", request=None, response=AsyncMock(status_code=404))
        )
        registry = build_slo_tools(client, TENANT)
        tool = registry.get("update_slo_thresholds")
        with pytest.raises(httpx.HTTPStatusError):
            await tool.handler(slo_config_id=999, target="99.9")
