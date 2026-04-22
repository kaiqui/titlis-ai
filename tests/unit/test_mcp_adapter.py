import pytest
from unittest.mock import AsyncMock

from src.services.mcp_adapter import McpAdapter
from src.tools.base import ToolDefinition, ToolRegistry


def _registry_with(name: str, result: any) -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(
        ToolDefinition(
            name=name,
            description=f"tool {name}",
            parameters={"type": "object", "properties": {}, "required": []},
            handler=AsyncMock(return_value=result),
        )
    )
    return reg


class TestMcpAdapterToOpenaiTools:
    def test_produces_function_type_entries(self):
        reg = _registry_with("get_deployment_spec", {})
        adapter = McpAdapter(reg)
        tools = adapter.to_openai_tools()
        assert len(tools) == 1
        assert tools[0]["type"] == "function"
        assert tools[0]["function"]["name"] == "get_deployment_spec"

    def test_merges_multiple_registries(self):
        reg1 = _registry_with("tool_a", {})
        reg2 = _registry_with("tool_b", {})
        adapter = McpAdapter(reg1, reg2)
        names = {t["function"]["name"] for t in adapter.to_openai_tools()}
        assert names == {"tool_a", "tool_b"}


class TestMcpAdapterToAnthropicTools:
    def test_produces_input_schema_entries(self):
        reg = _registry_with("get_current_scorecard", {})
        adapter = McpAdapter(reg)
        tools = adapter.to_anthropic_tools()
        assert len(tools) == 1
        assert "input_schema" in tools[0]
        assert tools[0]["name"] == "get_current_scorecard"


class TestMcpAdapterExecute:
    @pytest.mark.asyncio
    async def test_executes_registered_tool(self):
        handler = AsyncMock(return_value={"score": 95})
        reg = ToolRegistry()
        reg.register(
            ToolDefinition(
                name="get_deployment_spec",
                description="",
                parameters={},
                handler=handler,
            )
        )
        adapter = McpAdapter(reg)
        result = await adapter.execute("get_deployment_spec", {"namespace": "ns", "name": "app"})
        assert result["score"] == 95
        handler.assert_called_once_with(namespace="ns", name="app")

    @pytest.mark.asyncio
    async def test_unknown_tool_raises(self):
        adapter = McpAdapter(ToolRegistry())
        with pytest.raises(ValueError, match="Tool não registrada"):
            await adapter.execute("unknown_tool", {})
