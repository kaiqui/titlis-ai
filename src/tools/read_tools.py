from typing import Any, Dict, List

from src.infrastructure.titlis_api.scorecard_client import ScorecardClient
from src.tools.base import ToolDefinition, ToolRegistry
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _workload_not_found(namespace: str, name: str) -> Dict[str, Any]:
    return {"error": "workload_not_found", "namespace": namespace, "name": name}


def build_read_tools(scorecard_client: ScorecardClient, tenant_id: int) -> ToolRegistry:
    registry = ToolRegistry()

    async def get_deployment_spec(namespace: str, name: str) -> Dict[str, Any]:
        result = await scorecard_client.get_scorecard_by_name(tenant_id, name, namespace)
        if result is None:
            return _workload_not_found(namespace, name)
        return result

    async def get_current_scorecard(workload_id: str) -> Dict[str, Any]:
        result = await scorecard_client.get_scorecard_by_uid(tenant_id, workload_id)
        if result is None:
            return {"error": "workload_not_found", "workload_id": workload_id}
        return result

    async def get_hpa_config(namespace: str, name: str) -> Dict[str, Any]:
        # HPA data is not currently stored in titlis-api; returns null to signal no HPA configured.
        return {"namespace": namespace, "name": name, "hpa": None}

    async def get_similar_resolved(rule_id: str, pillar: str) -> List[Dict[str, Any]]:
        return await scorecard_client.get_similar_resolved(tenant_id, rule_id)

    async def get_namespace_inventory(namespace: str) -> List[Dict[str, Any]]:
        all_workloads = await scorecard_client.get_dashboard(tenant_id)
        return [w for w in all_workloads if w.get("namespace") == namespace]

    async def list_all_workloads() -> List[Dict[str, Any]]:
        return await scorecard_client.get_dashboard(tenant_id)

    async def get_remediation_history(workload_id: str) -> List[Dict[str, Any]]:
        return await scorecard_client.get_remediation_history(tenant_id, workload_id)

    registry.register(
        ToolDefinition(
            name="list_all_workloads",
            description="Lista todos os workloads do tenant com namespace, score e findings. Use antes de pedir namespace ao usuário.",
            parameters={
                "type": "object",
                "properties": {},
                "required": [],
            },
            handler=list_all_workloads,
        )
    )

    registry.register(
        ToolDefinition(
            name="get_deployment_spec",
            description="Retorna dados do Deployment (scorecard + metadata) a partir do namespace e nome.",
            parameters={
                "type": "object",
                "properties": {
                    "namespace": {"type": "string", "description": "Namespace do Deployment"},
                    "name": {"type": "string", "description": "Nome do Deployment"},
                },
                "required": ["namespace", "name"],
            },
            handler=get_deployment_spec,
        )
    )

    registry.register(
        ToolDefinition(
            name="get_current_scorecard",
            description="Retorna o scorecard atual de um workload pelo k8s_uid.",
            parameters={
                "type": "object",
                "properties": {
                    "workload_id": {"type": "string", "description": "k8s_uid do workload"},
                },
                "required": ["workload_id"],
            },
            handler=get_current_scorecard,
        )
    )

    registry.register(
        ToolDefinition(
            name="get_hpa_config",
            description="Retorna configuração de HPA do Deployment, ou null se não configurado.",
            parameters={
                "type": "object",
                "properties": {
                    "namespace": {"type": "string"},
                    "name": {"type": "string"},
                },
                "required": ["namespace", "name"],
            },
            handler=get_hpa_config,
        )
    )

    registry.register(
        ToolDefinition(
            name="get_similar_resolved",
            description="Lista workloads do mesmo tenant que já resolveram a regra informada.",
            parameters={
                "type": "object",
                "properties": {
                    "rule_id": {"type": "string", "description": "Ex: RES-003"},
                    "pillar": {"type": "string", "description": "Ex: resilience"},
                },
                "required": ["rule_id", "pillar"],
            },
            handler=get_similar_resolved,
        )
    )

    registry.register(
        ToolDefinition(
            name="get_namespace_inventory",
            description="Lista todos os Deployments de um namespace com seus scores.",
            parameters={
                "type": "object",
                "properties": {
                    "namespace": {"type": "string"},
                },
                "required": ["namespace"],
            },
            handler=get_namespace_inventory,
        )
    )

    registry.register(
        ToolDefinition(
            name="get_remediation_history",
            description="Retorna o histórico de PRs de remediação de um workload.",
            parameters={
                "type": "object",
                "properties": {
                    "workload_id": {"type": "string", "description": "k8s_uid do workload"},
                },
                "required": ["workload_id"],
            },
            handler=get_remediation_history,
        )
    )

    return registry
