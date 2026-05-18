import re
from typing import Any, Dict, List, Optional, Tuple

import yaml

from src.infrastructure.github.client import GitHubAPIClient
from src.infrastructure.github.repository import GitHubRepository
from src.infrastructure.prbot_client import PrbotClient
from src.infrastructure.titlis_api.scorecard_client import ScorecardClient
from src.tools.base import ToolDefinition, ToolRegistry
from src.utils.logger import get_logger

logger = get_logger(__name__)

_RESOURCE_RE = re.compile(r"(\d+(?:\.\d+)?)(m|Mi|Gi|Ki|Ti|Pi|)?$")
_MILLI_UNITS = {"m"}
_MEM_UNITS = {"Mi": 1, "Gi": 1024, "Ki": 1 / 1024, "Ti": 1024 * 1024, "Pi": 1024 * 1024 * 1024}


def _parse_cpu_millicores(value: str) -> float:
    m = _RESOURCE_RE.match(value.strip())
    if not m:
        return 0.0
    num, unit = float(m.group(1)), m.group(2) or ""
    return num if unit == "m" else num * 1000


def _parse_mem_mebibytes(value: str) -> float:
    m = _RESOURCE_RE.match(value.strip())
    if not m:
        return 0.0
    num, unit = float(m.group(1)), m.group(2) or ""
    if not unit:
        return num / (1024 * 1024)
    return num * _MEM_UNITS.get(unit, 1)


def _is_cpu(value: str) -> bool:
    return value.strip().endswith("m") or value.strip().replace(".", "").isdigit()


def _never_reduce_violated(current: str, suggested: str) -> bool:
    if not current or not suggested:
        return False
    try:
        if _is_cpu(current):
            return _parse_cpu_millicores(suggested) < _parse_cpu_millicores(current)
        return _parse_mem_mebibytes(suggested) < _parse_mem_mebibytes(current)
    except Exception:
        return False


def _extract_container_resources(yaml_text: str) -> Dict[Tuple[str, str], str]:
    """Parse YAML and return {(requests|limits, cpu|memory): value} for all containers."""
    result: Dict[Tuple[str, str], str] = {}
    try:
        for doc in yaml.safe_load_all(yaml_text):
            if not isinstance(doc, dict) or doc.get("kind") != "Deployment":
                continue
            containers = (
                (doc.get("spec") or {})
                .get("template", {})
                .get("spec", {})
                .get("containers") or []
            )
            for container in containers:
                resources = (container or {}).get("resources") or {}
                for section in ("requests", "limits"):
                    for key, val in (resources.get(section) or {}).items():
                        result[(section, key)] = str(val)
    except Exception:
        pass
    return result


def _check_never_reduce(current_yaml: str, patched_yaml: str) -> Optional[str]:
    """Returns a violation message if any resource section+key is reduced, else None."""
    current_res = _extract_container_resources(current_yaml)
    patched_res = _extract_container_resources(patched_yaml)
    for (section, key), patched_val in patched_res.items():
        current_val = current_res.get((section, key))
        if current_val and _never_reduce_violated(current_val, patched_val):
            return (
                f"never-reduce violado: tentou reduzir '{section}.{key}' "
                f"de {current_val} para {patched_val}"
            )
    return None


def _parse_repo(repo_url: str):
    clean = repo_url.rstrip("/").removeprefix("https://github.com/").removeprefix("http://github.com/")
    parts = clean.split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"repo_url inválido: {repo_url}")
    return parts[0], parts[1]


def build_github_tools(
    github_token: str,
    base_branch: str,
    tenant_id: int,
    scorecard_client: Optional[ScorecardClient] = None,
) -> ToolRegistry:
    registry = ToolRegistry()
    client = GitHubAPIClient(token=github_token)
    repo = GitHubRepository(client=client)

    async def read_deploy_manifest(repo_url: str, branch: str, path: str) -> Dict[str, Any]:
        owner, name = _parse_repo(repo_url)
        content = await repo.get_file_content(owner, name, path, branch)
        if content is None:
            return {"error": "file_not_found", "path": path, "branch": branch}
        return {"content": content, "path": path, "branch": branch}

    async def check_existing_pr(repo_url: str, namespace: str, deployment: str) -> Optional[Dict[str, Any]]:
        owner, name = _parse_repo(repo_url)
        pr = await repo.find_open_remediation_pr(owner, name, namespace, deployment, base_branch)
        if pr is None:
            return None
        return {"pr_url": pr.url, "pr_number": pr.number, "branch": pr.branch}

    async def create_remediation_pr(
        repo_url: str,
        path: str,
        patched_yaml: str,
        current_yaml: str,
        findings: List[str],
        namespace: str,
        deployment_name: str,
        workload_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        violation = _check_never_reduce(current_yaml, patched_yaml)
        if violation:
            raise ValueError(violation)

        payload = {
            "title": f"fix(titlis): auto-remediation {deployment_name} [{', '.join(findings[:3])}]",
            "description": (
                f"Remediação automática — {deployment_name}\n"
                f"Namespace: {namespace}\n"
                f"Findings corrigidos: {', '.join(findings)}"
            ),
            "workload_uids": [workload_id] if workload_id else [],
            "rule_id": findings[0] if findings else "",
            "cascade_up_to": "dev",
            "tenant_id": tenant_id,
            "trigger_source": "agent",
            "repo_url": repo_url,
            "path": path,
            "patched_yaml": patched_yaml,
            "namespace": namespace,
            "deployment_name": deployment_name,
            "findings": findings,
        }
        result = await PrbotClient().create_campaign(payload)
        return {"campaign_id": result.get("id"), "status": result.get("status")}

    registry.register(
        ToolDefinition(
            name="read_deploy_manifest",
            description="Lê o conteúdo atual do deploy.yaml do repositório GitHub.",
            parameters={
                "type": "object",
                "properties": {
                    "repo_url": {"type": "string"},
                    "branch": {"type": "string"},
                    "path": {"type": "string"},
                },
                "required": ["repo_url", "branch", "path"],
            },
            handler=read_deploy_manifest,
        )
    )

    registry.register(
        ToolDefinition(
            name="check_existing_pr",
            description="Verifica se já existe um PR de remediação aberto para o Deployment.",
            parameters={
                "type": "object",
                "properties": {
                    "repo_url": {"type": "string"},
                    "namespace": {"type": "string"},
                    "deployment": {"type": "string"},
                },
                "required": ["repo_url", "namespace", "deployment"],
            },
            handler=check_existing_pr,
        )
    )

    registry.register(
        ToolDefinition(
            name="create_remediation_pr",
            description=(
                "Cria branch, commit e PR no GitHub com o deploy.yaml corrigido. "
                "Valida never-reduce: resources nunca são reduzidos."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "repo_url": {"type": "string"},
                    "path": {"type": "string"},
                    "patched_yaml": {"type": "string"},
                    "current_yaml": {"type": "string"},
                    "findings": {"type": "array", "items": {"type": "string"}},
                    "namespace": {"type": "string"},
                    "deployment_name": {"type": "string"},
                    "workload_id": {"type": "string", "description": "k8s_uid do workload (workload_id do list_all_workloads)"},
                },
                "required": [
                    "repo_url",
                    "path",
                    "patched_yaml",
                    "current_yaml",
                    "findings",
                    "namespace",
                    "deployment_name",
                ],
            },
            handler=create_remediation_pr,
        )
    )

    return registry
