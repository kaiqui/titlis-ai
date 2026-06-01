from typing import Any, Dict, List, Optional
from typing_extensions import TypedDict


class ScorecardRemediationState(TypedDict, total=False):
    # ── inputs ───────────────────────────────────────────────────────────────
    tenant_id: int
    workload_id: str  # k8s_uid
    finding_ids: List[str]  # rule IDs requested for remediation
    repo_url: str
    deploy_manifest_path: str
    ai_config: Dict[str, Any]

    # ── populated by classify_findings ───────────────────────────────────────
    findings: List[Dict[str, Any]]  # full finding detail from scorecard
    namespace: str
    deployment_name: str

    # ── populated by fetch_context ────────────────────────────────────────────
    rag_context: List[Dict[str, Any]]
    current_manifest: Optional[str]
    live_deployment: Optional[Dict[str, Any]]

    # ── populated by check_existing_pr ───────────────────────────────────────
    existing_pr: Optional[Dict[str, Any]]

    # ── populated by analyze / generate / validate ───────────────────────────
    analysis: Optional[str]
    patched_manifest: Optional[str]
    validation_errors: List[str]
    retry_count: int

    # ── populated by resolve_manifest_path ───────────────────────────────────
    detected_environment: Optional[str]
    service_definition: Optional[Dict[str, Any]]  # conteúdo de .titlis/service.yaml
    effective_base_branch: Optional[str]  # branch resolvido via service.yaml

    # ── human-in-the-loop ────────────────────────────────────────────────────
    approved: Optional[bool]

    # ── populated by create_remediation_pr ───────────────────────────────────
    pr_result: Optional[Dict[str, Any]]
