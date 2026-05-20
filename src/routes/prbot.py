import json
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from src.bootstrap.dependencies import get_llm_service
from src.domain.models import TenantAiConfig
from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


class WorkloadFinding(BaseModel):
    rule_id: str
    severity: str = "medium"


class AiConfigInput(BaseModel):
    api_key: str
    provider: str = "openai"
    model: str = "gpt-4o"


class ManifestPatchRequest(BaseModel):
    tenant_id: int
    manifest: str
    findings: List[WorkloadFinding]
    workload_name: str
    namespace: str
    cluster_name: str = ""
    environment: str = ""
    criticality: str = ""
    ai_config: Optional[AiConfigInput] = None


class AppliedFix(BaseModel):
    rule_id: str
    summary: str


class SkippedFix(BaseModel):
    rule_id: str
    reason: str


class ManifestPatchResponse(BaseModel):
    corrected_manifest: str
    applied: List[AppliedFix]
    skipped: List[SkippedFix]


_RULE_DESCRIPTIONS: Dict[str, str] = {
    "SEC-002": "Adicionar securityContext com allowPrivilegeEscalation: false",
    "SEC-003": "Adicionar securityContext com readOnlyRootFilesystem: true",
    "SEC-004": "Adicionar securityContext com runAsNonRoot: true e runAsUser > 0",
    "RES-001": "Definir resources.requests.cpu para todos os containers",
    "RES-002": "Definir resources.requests.memory para todos os containers",
    "RES-003": "Definir resources.limits.cpu para todos os containers",
    "RES-004": "Definir resources.limits.memory para todos os containers",
    "RES-005": "Adicionar readinessProbe em todos os containers",
    "RES-006": "Adicionar livenessProbe em todos os containers",
    "RES-007": "Adicionar startupProbe em todos os containers",
    "RES-008": "Definir minReplicas >= 2 no HPA (alta disponibilidade)",
    "RES-009": "Definir maxReplicas no HPA",
    "RES-010": "Configurar PodDisruptionBudget",
    "RES-011": "Definir topologySpreadConstraints ou affinity para distribuição de pods",
    "RES-012": "Adicionar labels de versão (app.kubernetes.io/version)",
    "RES-013": "Definir terminationGracePeriodSeconds adequado (>= 30s)",
    "RES-014": "Adicionar preStop hook no container lifecycle",
    "RES-016": "Configurar política de reinicialização (restartPolicy)",
    "RES-017": "Definir podAntiAffinity para alta disponibilidade",
    "RES-018": "Adicionar recursos de QoS Guaranteed (requests == limits)",
    "RES-019": "Configurar HPA com métricas adequadas",
    "PERF-001": "Definir targetCPUUtilizationPercentage no HPA (50-70%)",
    "PERF-002": "Ajustar cpu requests para evitar over/under-provisioning",
    "PERF-003": "Ajustar memory requests com base no uso real",
    "PERF-004": "Calibrar HPA minReplicas e maxReplicas com base em métricas",
    "OPS-001": "Adicionar annotations de monitoring/alerting obrigatórias",
}

_SYSTEM_PROMPT = """Você é um especialista em Kubernetes e SRE. Sua tarefa é corrigir um manifesto YAML de Deployment para resolver findings de compliance listados.

REGRAS OBRIGATÓRIAS:
1. Retorne APENAS o manifesto YAML completo e corrigido — sem markdown fences, sem explicações, apenas o YAML puro.
2. Após o YAML, adicione uma seção JSON no formato exato:
   ---APPLIED---
   [{"rule_id": "...", "summary": "..."}, ...]
   ---SKIPPED---
   [{"rule_id": "...", "reason": "..."}, ...]
3. NUNCA reduza valores existentes de cpu, memory, minReplicas, maxReplicas — apenas adicione ou aumente.
4. Para cada finding, aplique a correção correspondente se possível. Se não for possível (ex: requer dados externos como métricas reais), inclua no SKIPPED.
5. Preserve TODOS os outros campos do manifesto original sem alteração.
6. Mantenha indentação YAML consistente com o manifesto original.
7. Para recursos não definidos: cpu requests=100m, memory requests=128Mi, cpu limits=500m, memory limits=256Mi como padrão seguro.
"""


def _build_user_prompt(req: ManifestPatchRequest) -> str:
    findings_text = "\n".join(
        f"- {f.rule_id} [{f.severity}]: {_RULE_DESCRIPTIONS.get(f.rule_id, f.rule_id)}"
        for f in req.findings
    )
    return f"""Workload: {req.workload_name} (namespace: {req.namespace}, ambiente: {req.environment or "N/A"}, criticidade: {req.criticality or "N/A"})

FINDINGS A CORRIGIR:
{findings_text}

MANIFESTO ATUAL:
{req.manifest}

Corrija o manifesto para resolver os findings acima e retorne no formato especificado."""


def _parse_llm_response(raw: str, finding_ids: List[str]) -> ManifestPatchResponse:
    applied_marker = "---APPLIED---"
    skipped_marker = "---SKIPPED---"

    applied: List[AppliedFix] = []
    skipped: List[SkippedFix] = []
    corrected_manifest = raw

    if applied_marker in raw:
        parts = raw.split(applied_marker)
        corrected_manifest = parts[0].strip()
        rest = parts[1] if len(parts) > 1 else ""

        if skipped_marker in rest:
            applied_part, skipped_part = rest.split(skipped_marker, 1)
        else:
            applied_part = rest
            skipped_part = ""

        try:
            applied_json = re.search(r"\[.*?\]", applied_part, re.DOTALL)
            if applied_json:
                for item in json.loads(applied_json.group()):
                    applied.append(AppliedFix(rule_id=item.get("rule_id", ""), summary=item.get("summary", "")))
        except Exception:
            pass

        try:
            skipped_json = re.search(r"\[.*?\]", skipped_part, re.DOTALL)
            if skipped_json:
                for item in json.loads(skipped_json.group()):
                    skipped.append(SkippedFix(rule_id=item.get("rule_id", ""), reason=item.get("reason", "")))
        except Exception:
            pass

    corrected_manifest = corrected_manifest.strip()
    if corrected_manifest.startswith("```"):
        corrected_manifest = re.sub(r"^```[a-z]*\n?", "", corrected_manifest)
        corrected_manifest = re.sub(r"\n?```$", "", corrected_manifest).strip()

    applied_ids = {f.rule_id for f in applied}
    skipped_ids = {f.rule_id for f in skipped}
    for fid in finding_ids:
        if fid not in applied_ids and fid not in skipped_ids:
            skipped.append(SkippedFix(rule_id=fid, reason="not_processed_by_llm"))

    return ManifestPatchResponse(
        corrected_manifest=corrected_manifest,
        applied=applied,
        skipped=skipped,
    )


@router.post("/prbot/generate-manifest-patch", response_model=ManifestPatchResponse)
async def generate_manifest_patch(body: ManifestPatchRequest, request: Request) -> ManifestPatchResponse:
    secret = request.headers.get("X-Internal-Secret", "")
    if secret != settings.internal_secret:
        raise HTTPException(status_code=403, detail="internal_secret_invalid")

    if not body.findings:
        return ManifestPatchResponse(corrected_manifest=body.manifest, applied=[], skipped=[])

    if body.ai_config is None:
        raise HTTPException(status_code=422, detail="ai_config required")

    ai_cfg = TenantAiConfig(
        provider=body.ai_config.provider,
        model=body.ai_config.model,
        api_key=body.ai_config.api_key,
    )

    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": _build_user_prompt(body)},
    ]

    llm = get_llm_service()
    raw = await llm.chat(messages, ai_cfg, tenant_id=body.tenant_id, trace_metadata={"phase": "manifest_patch"})

    finding_ids = [f.rule_id for f in body.findings]
    result = _parse_llm_response(raw, finding_ids)

    logger.info(
        "manifest patch gerado",
        extra={
            "tenant_id": body.tenant_id,
            "workload": body.workload_name,
            "namespace": body.namespace,
            "applied_count": len(result.applied),
            "skipped_count": len(result.skipped),
        },
    )
    return result
