from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class FindingContext(BaseModel):
    rule_id: str
    pillar: str
    severity: str
    actual_value: Optional[str] = None
    expected_value: Optional[str] = None
    deployment_name: str
    namespace: str
    container_name: Optional[str] = None
    is_remediable: bool = False


class TenantAiConfig(BaseModel):
    provider: str
    model: str
    api_key: str
    github_token: Optional[str] = None
    github_base_branch: str = "main"
    monthly_token_budget: Optional[int] = None
    tokens_used_month: int = 0


class ExplainRequest(BaseModel):
    tenant_id: int
    workload_id: int
    finding: FindingContext
    ai_config: TenantAiConfig


class RemediateRequest(BaseModel):
    tenant_id: int
    workload_id: str  # k8s_uid do workload
    finding_ids: List[str]  # rule IDs a remediar (vazio = todos os falhos)
    repo_url: str
    deploy_manifest_path: str = "manifests/kubernetes/main/deploy.yaml"
    ai_config: TenantAiConfig


class ConfirmRemediationRequest(BaseModel):
    approved: bool


class SetManifestPathRequest(BaseModel):
    manifest_path: str


class SseChunk(BaseModel):
    type: str
    content: Optional[str] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class FeedbackRequest(BaseModel):
    tenant_id: int
    response_id: str
    rule_id: str
    sentiment: str  # "positive" | "negative"
    comment: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None


class ToolProposal(BaseModel):
    proposal_id: str
    tool_name: str
    description: str
    args: Dict[str, Any]
    is_write: bool = False


class AgentChatRequest(BaseModel):
    tenant_id: int
    session_id: str
    message: str
    ai_config: TenantAiConfig
    workload_id: Optional[str] = None


class AgentToolDecision(BaseModel):
    proposal_id: str
    approved: bool
    edited_args: Optional[Dict[str, Any]] = None


class AgentToolsRespondRequest(BaseModel):
    decisions: List[AgentToolDecision]
