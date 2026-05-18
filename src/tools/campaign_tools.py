from typing import Any, Optional

from src.infrastructure.prbot_client import PrbotClient
from src.tools.base import ToolDefinition, ToolRegistry


def build_campaign_tools(tenant_id: int, actor_email: Optional[str] = None) -> ToolRegistry:
    registry = ToolRegistry()
    client = PrbotClient()

    async def trigger_bulk_pr_campaign(
        title: str,
        workload_uids: list[str],
        description: str = "",
        rule_id: str = "PERF-004",
        cascade_up_to: str = "dev",
    ) -> dict[str, Any]:
        payload = {
            "title": title,
            "description": description,
            "workload_uids": workload_uids,
            "rule_id": rule_id,
            "cascade_up_to": cascade_up_to,
            "actor_email": actor_email,
            "tenant_id": tenant_id,
            "trigger_source": "manual",
        }
        return await client.create_campaign(payload)

    registry.register(
        ToolDefinition(
            name="trigger_bulk_pr_campaign",
            description=(
                "Dispara uma campanha de PRs em lote para ajustar HPA de múltiplos workloads. "
                "Use quando o usuário quiser corrigir vários workloads de uma vez. "
                "Parâmetro cascade_up_to controla até qual ambiente a campanha avança automaticamente; "
                "padrão é 'dev' (mais conservador). Nunca use 'prd' sem confirmação explícita do usuário."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "title": {"type": "string", "description": "Título da campanha"},
                    "workload_uids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "UIDs K8s dos workloads a incluir na campanha",
                    },
                    "description": {"type": "string", "description": "Descrição opcional"},
                    "rule_id": {"type": "string", "default": "PERF-004"},
                    "cascade_up_to": {
                        "type": "string",
                        "enum": ["dev", "hml", "prd"],
                        "default": "dev",
                    },
                },
                "required": ["title", "workload_uids"],
            },
            handler=trigger_bulk_pr_campaign,
        )
    )

    return registry
