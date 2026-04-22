from typing import Any, Dict, List, Optional

from src.infrastructure.titlis_api.scorecard_client import ScorecardClient
from src.tools.base import ToolDefinition, ToolRegistry
from src.utils.logger import get_logger

logger = get_logger(__name__)

_VALID_FIELDS = {"target", "warning", "timeframe"}


class SloValidationError(ValueError):
    pass


def build_slo_tools(scorecard_client: ScorecardClient, tenant_id: int) -> ToolRegistry:
    registry = ToolRegistry()

    async def get_slo_status(workload_id: str) -> Dict[str, Any]:
        slos = await scorecard_client.get_slos(tenant_id)
        matched = [
            s for s in slos if s.get("k8s_resource_uid") == workload_id or str(s.get("slo_config_id")) == workload_id
        ]
        if not matched:
            return {"error": "slo_not_found", "workload_id": workload_id}
        return matched[0]

    async def list_auto_created_slos(namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        return await scorecard_client.get_slos(tenant_id, namespace)

    async def update_slo_thresholds(
        slo_config_id: int,
        target: Optional[str] = None,
        warning: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> Dict[str, Any]:
        if target is not None and warning is not None:
            try:
                t_val, w_val = float(target), float(warning)
            except (ValueError, TypeError):
                t_val, w_val = None, None
            if t_val is not None and w_val is not None and t_val <= w_val:
                raise SloValidationError(f"target ({target}) deve ser maior que warning ({warning})")

        results = []
        slos = await scorecard_client.get_slos(tenant_id)
        current = next((s for s in slos if s.get("slo_config_id") == slo_config_id), None)

        for field, new_val in [("target", target), ("warning", warning), ("timeframe", timeframe)]:
            if new_val is None:
                continue
            old_val = str(current.get(field, "")) if current else ""
            change = await scorecard_client.propose_slo_change(
                tenant_id=tenant_id,
                slo_config_id=slo_config_id,
                field=field,
                old_value=old_val,
                new_value=new_val,
            )
            results.append(change)

        return {"proposed_changes": results, "slo_config_id": slo_config_id}

    registry.register(
        ToolDefinition(
            name="get_slo_status",
            description="Retorna o status atual do SLO de um workload.",
            parameters={
                "type": "object",
                "properties": {
                    "workload_id": {"type": "string", "description": "k8s_uid ou slo_config_id"},
                },
                "required": ["workload_id"],
            },
            handler=get_slo_status,
        )
    )

    registry.register(
        ToolDefinition(
            name="list_auto_created_slos",
            description="Lista os SLOs criados automaticamente pelo operator para o tenant.",
            parameters={
                "type": "object",
                "properties": {
                    "namespace": {"type": "string", "description": "Filtra por namespace (opcional)"},
                },
                "required": [],
            },
            handler=list_auto_created_slos,
        )
    )

    registry.register(
        ToolDefinition(
            name="update_slo_thresholds",
            description=(
                "Propõe alteração de thresholds do SLO via titlis-api. "
                "O operator aplica a mudança no CRD em até 30s."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "slo_config_id": {"type": "integer"},
                    "target": {"type": "string", "description": "Ex: '99.9'"},
                    "warning": {"type": "string", "description": "Ex: '99.5'"},
                    "timeframe": {"type": "string", "description": "Ex: '30d'"},
                },
                "required": ["slo_config_id"],
            },
            handler=update_slo_thresholds,
        )
    )

    return registry
