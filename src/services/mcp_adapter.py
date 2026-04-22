from typing import Any, Dict, List

from src.tools.base import ToolRegistry
from src.utils.logger import get_logger

logger = get_logger(__name__)


class McpAdapter:
    def __init__(self, *registries: ToolRegistry) -> None:
        from src.tools.base import ToolRegistry as _Reg

        self._registry = _Reg()
        for reg in registries:
            for tool in reg.all():
                self._registry.register(tool)

    def to_openai_tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters,
                },
            }
            for tool in self._registry.all()
        ]

    def to_anthropic_tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.parameters,
            }
            for tool in self._registry.all()
        ]

    async def execute(self, tool_name: str, args: Dict[str, Any]) -> Any:
        tool = self._registry.get(tool_name)
        if tool is None:
            raise ValueError(f"Tool não registrada: {tool_name}")
        logger.info("Executando tool MCP", extra={"tool": tool_name, "args_keys": list(args.keys())})
        return await tool.handler(**args)
