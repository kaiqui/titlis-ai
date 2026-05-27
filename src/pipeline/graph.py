import asyncio
import json
from datetime import datetime
from typing import Any, Dict

import yaml
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from src.infrastructure.mcp.github_mcp import github_mcp_session
from src.infrastructure.titlis_api.scorecard_client import ScorecardClient
from src.infrastructure.titlis_api.knowledge_client import KnowledgeClient
from src.pipeline.state import ScorecardRemediationState
from src.services.embedding_service import EmbeddingService
from src.services.llm_service import LLMService
from src.tools.github_tools import _never_reduce_violated, _parse_repo
from src.utils.logger import get_logger

logger = get_logger(__name__)

_MAX_RETRIES = 3


def _github_session_kwargs(ai_config: dict) -> dict:
    auth_mode = ai_config.get("github_auth_mode", "pat")
    if auth_mode == "github_app":
        app_id = ai_config.get("github_app_id")
        priv_key = ai_config.get("github_app_private_key")
        install_id = ai_config.get("github_app_installation_id")
        if app_id and priv_key and install_id:
            return {
                "github_app_id": app_id,
                "github_app_private_key": priv_key,
                "github_app_installation_id": install_id,
            }
    token = ai_config.get("github_token")
    if token:
        return {"github_token": token}
    return {}


def _mcp_text(result) -> str:
    if result and result.content:
        item = result.content[0]
        if hasattr(item, "text"):
            return item.text
    return ""


def _detect_env_from_cluster(cluster_name: str) -> str:
    name = cluster_name.lower()
    for kw in ("prod", "production"):
        if kw in name:
            return "prod"
    for kw in ("hml", "homolog", "staging", "stg"):
        if kw in name:
            return "hml"
    for kw in ("dev", "development"):
        if kw in name:
            return "dev"
    return ""


def _detect_env_from_namespace(namespace: str) -> str:
    ns = namespace.lower()
    if any(kw in ns for kw in ("prod", "production")):
        return "prd"
    if any(kw in ns for kw in ("hml", "homolog", "staging", "stg")):
        return "hml"
    if any(kw in ns for kw in ("dev", "develop")):
        return "dev"
    return ""


class RemediationGraph:
    def __init__(
        self,
        llm_service: LLMService,
        scorecard_client: ScorecardClient,
        knowledge_client: KnowledgeClient,
        embedding_service: EmbeddingService,
    ) -> None:
        self._llm = llm_service
        self._scorecard = scorecard_client
        self._knowledge = knowledge_client
        self._embedding = embedding_service
        self._graph = self._build()

    # ── nodes ─────────────────────────────────────────────────────────────────

    async def _classify_findings(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        scorecard = await self._scorecard.get_scorecard_by_uid(state["tenant_id"], state["workload_id"])
        if not scorecard:
            logger.warning("Scorecard não encontrado", extra={"workload_id": state["workload_id"]})
            return {
                "findings": [],
                "namespace": "",
                "deployment_name": state["workload_id"],
            }

        requested = set(state.get("finding_ids", []))
        all_results = scorecard.get("validation_results", [])
        findings = [r for r in all_results if not r.get("passed") and (not requested or r.get("rule_id") in requested)]

        return {
            "findings": findings,
            "namespace": scorecard.get("namespace", ""),
            "deployment_name": scorecard.get("workload", state["workload_id"]),
            "live_deployment": scorecard,
        }

    async def _read_service_yaml(self, repo_url: str, ai_config: dict) -> Dict[str, Any] | None:
        kwargs = _github_session_kwargs(ai_config)
        if not kwargs:
            return None
        try:
            owner, name = _parse_repo(repo_url)
            async with github_mcp_session(**kwargs) as session:
                result = await session.call_tool(
                    "get_file_contents",
                    {"owner": owner, "repo": name, "path": ".titlis/service.yaml"},
                )
                if getattr(result, "isError", False):
                    content = getattr(result, "content", None)
                    err_text = getattr(content[0], "text", "") if content else ""
                    if "not found" in err_text.lower() or "404" in err_text:
                        logger.warning(
                            "Repo não acessível — PAT pode não ter scope 'repo' para repo privado",
                            extra={"repo": f"{owner}/{name}", "error": err_text[:120]},
                        )
                    else:
                        logger.debug(".titlis/service.yaml não existe no repo", extra={"repo": f"{owner}/{name}"})
                    return None
                text = _mcp_text(result)
                if text:
                    return yaml.safe_load(text)
        except Exception as exc:
            logger.debug(
                ".titlis/service.yaml não acessível",
                extra={"repo_url": repo_url, "error": str(exc)[:120]},
            )
        return None

    async def _resolve_manifest_path(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        live = state.get("live_deployment") or {}
        labels = live.get("labels") or {}
        namespace = state.get("namespace", "")
        ai_config = state.get("ai_config", {})

        # repo_url pode vir da request ou ser inferido dos labels do workload
        # titlis.io/github-owner + titlis.io/repo formam "owner/repo"
        repo_url = state.get("repo_url", "")
        if not repo_url:
            gh_owner = labels.get("titlis.io/github-owner", "")
            gh_repo = labels.get("titlis.io/repo", "")
            if gh_owner and gh_repo:
                repo_url = f"https://github.com/{gh_owner}/{gh_repo}"

        env = (
            _detect_env_from_namespace(namespace)
            or labels.get("env")
            or labels.get("environment")
            or live.get("environment")
            or _detect_env_from_cluster(live.get("cluster", ""))
            or "unknown"
        )

        # Tenta ler .titlis/service.yaml automaticamente
        if repo_url:
            svc = await self._read_service_yaml(repo_url, ai_config)
            if svc:
                gitops_paths = svc.get("spec", {}).get("gitops", {}).get("paths", {})
                path_cfg = gitops_paths.get(env) or gitops_paths.get(next(iter(gitops_paths), ""), {})
                if path_cfg and path_cfg.get("path"):
                    logger.info(
                        ".titlis/service.yaml resolveu caminho do manifest",
                        extra={"env": env, "path": path_cfg["path"]},
                    )
                    return {
                        "deploy_manifest_path": path_cfg["path"],
                        "effective_base_branch": path_cfg.get("base_branch")
                        or ai_config.get("github_base_branch", "main"),
                        "detected_environment": env,
                        "service_definition": svc,
                    }

        # Fallback: pede confirmação ao usuário
        confirmed_path = interrupt(
            {
                "type": "manifest_path_required",
                "detected_environment": env,
                "suggested_path": state.get("deploy_manifest_path", ""),
                "deployment_name": state.get("deployment_name", ""),
                "namespace": namespace,
                "hint": (
                    "Não foi possível ler .titlis/service.yaml automaticamente. "
                    "Se o repositório for privado, verifique se o PAT tem o scope 'repo' "
                    "(e 'read:org' para repos de organizações) em Configurações → Integrações. "
                    "Caso as credenciais estejam corretas, adicione .titlis/service.yaml ao repo."
                ),
            }
        )

        return {
            "deploy_manifest_path": str(confirmed_path),
            "detected_environment": env,
        }

    async def _fetch_context(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        ai_config = state.get("ai_config", {})
        findings = state.get("findings", [])
        repo_url = state.get("repo_url", "")
        path = state.get("deploy_manifest_path", "")
        base_branch = state.get("effective_base_branch") or ai_config.get("github_base_branch", "main")

        rag_task = self._fetch_rag_context(findings, ai_config)
        manifest_task = self._fetch_manifest(repo_url, base_branch, path, ai_config)

        rag_chunks, current_manifest = await asyncio.gather(rag_task, manifest_task)

        return {
            "rag_context": rag_chunks,
            "current_manifest": current_manifest,
        }

    async def _fetch_rag_context(self, findings, ai_config):
        if not findings or not ai_config.get("api_key"):
            return []
        try:
            rule_ids = " ".join(f.get("rule_id", "") for f in findings)
            embedding = await self._embedding.embed(
                text=rule_ids,
                provider=ai_config.get("provider", "openai"),
                api_key=ai_config["api_key"],
            )
            return await self._knowledge.search_similar(tenant_id=0, embedding=embedding, limit=3)
        except Exception:
            logger.warning("RAG falhou no pipeline de remediação")
            return []

    async def _fetch_manifest(self, repo_url, branch, path, ai_config):
        kwargs = _github_session_kwargs(ai_config)
        if not kwargs or not repo_url:
            return None
        try:
            owner, name = _parse_repo(repo_url)
            async with github_mcp_session(**kwargs) as session:
                result = await session.call_tool(
                    "get_file_contents", {"owner": owner, "repo": name, "path": path, "ref": branch}
                )
                return _mcp_text(result) or None
        except Exception:
            logger.warning("Falha ao ler manifest via MCP GitHub", extra={"path": path})
            return None

    async def _check_existing_pr(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        ai_config = state.get("ai_config", {})
        kwargs = _github_session_kwargs(ai_config)
        repo_url = state.get("repo_url", "")
        namespace = state.get("namespace", "")
        deployment_name = state.get("deployment_name", "")
        base_branch = ai_config.get("github_base_branch", "main")

        if not kwargs or not repo_url:
            return {"existing_pr": None}

        safe_name = deployment_name.replace("/", "-")
        branch_prefix = f"fix/auto-remediation-{namespace}-{safe_name}-"

        try:
            owner, name = _parse_repo(repo_url)
            async with github_mcp_session(**kwargs) as session:
                result = await session.call_tool(
                    "list_pull_requests",
                    {"owner": owner, "repo": name, "state": "open", "base": base_branch},
                )
                prs = json.loads(_mcp_text(result) or "[]")
                for pr in prs:
                    head_ref = pr.get("head", {}).get("ref", "")
                    if head_ref.startswith(branch_prefix):
                        return {
                            "existing_pr": {
                                "pr_url": pr["html_url"],
                                "pr_number": pr["number"],
                                "branch": head_ref,
                            }
                        }
        except Exception:
            logger.warning("Falha ao verificar PR existente via MCP GitHub")

        return {"existing_pr": None}

    async def _analyze_findings(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        ai_config = state.get("ai_config", {})
        findings = state.get("findings", [])
        rag_context = state.get("rag_context", [])
        current_manifest = state.get("current_manifest", "")

        rag_section = ""
        if rag_context:
            rag_section = "\n\n**Contexto de base de conhecimento:**\n"
            for i, c in enumerate(rag_context, 1):
                rag_section += f"{i}. {c.get('chunkText', '')}\n"

        findings_str = "\n".join(
            f"- {f.get('rule_id')}: {f.get('message', '')} (actual={f.get('actual_value', 'N/A')})" for f in findings
        )

        messages = [
            {
                "role": "system",
                "content": (
                    "Você é um especialista em SRE e Kubernetes. Analise os findings de compliance "
                    "e explique o que precisa ser corrigido no deploy.yaml. Seja objetivo e técnico."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Deployment: {state.get('deployment_name')} (namespace: {state.get('namespace')})\n\n"
                    f"**Findings a corrigir:**\n{findings_str}\n\n"
                    f"**Manifest atual:**\n```yaml\n{current_manifest or 'não disponível'}\n```"
                    f"{rag_section}\n\n"
                    "Explique o que precisa ser alterado e por quê."
                ),
            },
        ]

        analysis = await self._llm.chat(messages, _to_ai_config(ai_config), state.get("tenant_id", 0))
        return {"analysis": analysis}

    async def _generate_yaml_patch(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        ai_config = state.get("ai_config", {})
        analysis = state.get("analysis", "")
        current_manifest = state.get("current_manifest", "")
        findings = state.get("findings", [])
        retry_count = state.get("retry_count", 0)
        validation_errors = state.get("validation_errors", [])

        error_feedback = ""
        if validation_errors:
            error_feedback = "\n\n**Erros da tentativa anterior (corrija):**\n" + "\n".join(validation_errors)

        findings_str = "\n".join(
            f"- {f.get('rule_id')}: {f.get('message', '')} (actual={f.get('actual_value', 'N/A')})" for f in findings
        )

        messages = [
            {
                "role": "system",
                "content": (
                    "Você é um especialista em Kubernetes. Gere APENAS o conteúdo YAML corrigido do deploy.yaml. "
                    "Não adicione explicações, comentários ou blocos de código markdown. "
                    "Retorne SOMENTE o YAML válido. "
                    "NUNCA reduza valores de cpu ou memory — apenas aumente ou adicione."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"**Análise:**\n{analysis}\n\n"
                    f"**Findings a corrigir:**\n{findings_str}\n\n"
                    f"**Manifest atual:**\n{current_manifest or 'não disponível'}"
                    f"{error_feedback}\n\n"
                    "Retorne o deploy.yaml completo e corrigido."
                ),
            },
        ]

        patched = await self._llm.chat(messages, _to_ai_config(ai_config), state.get("tenant_id", 0))
        patched = patched.strip()
        if patched.startswith("```"):
            lines = patched.splitlines()
            patched = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])

        return {
            "patched_manifest": patched.strip(),
            "retry_count": retry_count + 1,
            "validation_errors": [],
        }

    async def _validate_patch(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        patched = state.get("patched_manifest", "")
        current = state.get("current_manifest", "")
        errors = []

        try:
            yaml.safe_load(patched)
        except yaml.YAMLError as exc:
            errors.append(f"YAML inválido: {exc}")
            return {"validation_errors": errors}

        if current:
            current_lines = {
                line.strip().split(":")[0]: line.split(":", 1)[-1].strip().strip('"').strip("'")
                for line in current.splitlines()
                if ":" in line and line.strip().startswith(("cpu:", "memory:"))
            }
            for line in patched.splitlines():
                if ":" not in line:
                    continue
                key = line.strip().split(":")[0]
                if key in ("cpu", "memory"):
                    new_val = line.split(":", 1)[-1].strip().strip('"').strip("'")
                    old_val = current_lines.get(key, "")
                    if old_val and new_val and _never_reduce_violated(old_val, new_val):
                        errors.append(f"never-reduce violado: {key} {old_val} → {new_val}")

        return {"validation_errors": errors}

    async def _await_user_confirmation(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        approved = interrupt(
            {
                "patched_manifest": state.get("patched_manifest"),
                "current_manifest": state.get("current_manifest"),
                "findings": [f.get("rule_id") for f in state.get("findings", [])],
                "deployment_name": state.get("deployment_name"),
                "namespace": state.get("namespace"),
            }
        )
        return {"approved": bool(approved)}

    async def _create_remediation_pr(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        ai_config = state.get("ai_config", {})
        kwargs = _github_session_kwargs(ai_config)
        base_branch = state.get("effective_base_branch") or ai_config.get("github_base_branch", "main")
        repo_url = state.get("repo_url", "")
        path = state.get("deploy_manifest_path", "deploy.yaml")
        patched = state.get("patched_manifest", "")
        findings = [f.get("rule_id", "") for f in state.get("findings", [])]
        namespace = state.get("namespace", "")
        deployment_name = state.get("deployment_name", "")

        owner, name = _parse_repo(repo_url)
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        branch_name = f"fix/auto-remediation-{namespace}-{deployment_name}-{ts}"
        commit_msg = f"fix(titlis-ai): auto-remediation {deployment_name} [{', '.join(findings)}]"
        pr_body = (
            f"## Remediação automática — {deployment_name}\n\n"
            f"**Namespace:** {namespace}\n\n"
            f"**Findings corrigidos:**\n"
            + "\n".join(f"- {f}" for f in findings)
            + "\n\n*Gerado pelo Titlis AI Assistant*"
        )

        async with github_mcp_session(**kwargs) as session:
            await session.call_tool(
                "create_branch",
                {"owner": owner, "repo": name, "branch": branch_name, "from_branch": base_branch},
            )
            await session.call_tool(
                "push_files",
                {
                    "owner": owner,
                    "repo": name,
                    "branch": branch_name,
                    "message": commit_msg,
                    "files": [{"path": path, "content": patched}],
                },
            )
            result = await session.call_tool(
                "create_pull_request",
                {
                    "owner": owner,
                    "repo": name,
                    "title": f"fix(titlis): auto-remediation {deployment_name} [{', '.join(findings[:3])}]",
                    "body": pr_body,
                    "head": branch_name,
                    "base": base_branch,
                    "draft": False,
                },
            )

        pr_data = json.loads(_mcp_text(result) or "{}")
        return {
            "pr_result": {
                "pr_url": pr_data.get("html_url", ""),
                "pr_number": pr_data.get("number", 0),
                "branch": branch_name,
            }
        }

    async def _notify_api(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        pr = state.get("pr_result", {}) or {}
        try:
            await self._scorecard.notify_remediation_started(
                tenant_id=state.get("tenant_id", 0),
                workload_id=state.get("workload_id", ""),
                pr_url=pr.get("pr_url"),
                pr_number=pr.get("pr_number"),
                github_branch=pr.get("branch"),
                repo_url=state.get("repo_url"),
                finding_ids=[f.get("rule_id") for f in state.get("findings", [])],
            )
        except Exception:
            logger.exception(
                "Falha ao notificar API sobre remediação iniciada",
                extra={"workload_id": state.get("workload_id"), "tenant_id": state.get("tenant_id")},
            )
        return {}

    # ── routing ───────────────────────────────────────────────────────────────

    @staticmethod
    def _route_after_check_pr(state: ScorecardRemediationState) -> str:
        if state.get("existing_pr"):
            return END
        return "analyze_findings"

    @staticmethod
    def _route_after_validate(state: ScorecardRemediationState) -> str:
        errors = state.get("validation_errors", [])
        retry = state.get("retry_count", 0)
        if errors and retry < _MAX_RETRIES:
            return "generate_yaml_patch"
        if errors:
            return END
        return "await_user_confirmation"

    @staticmethod
    def _route_after_confirmation(state: ScorecardRemediationState) -> str:
        if state.get("approved"):
            return "create_remediation_pr"
        return END

    # ── build ─────────────────────────────────────────────────────────────────

    def _build(self):
        builder = StateGraph(ScorecardRemediationState)

        builder.add_node("classify_findings", self._classify_findings)
        builder.add_node("resolve_manifest_path", self._resolve_manifest_path)
        builder.add_node("fetch_context", self._fetch_context)
        builder.add_node("check_existing_pr", self._check_existing_pr)
        builder.add_node("analyze_findings", self._analyze_findings)
        builder.add_node("generate_yaml_patch", self._generate_yaml_patch)
        builder.add_node("validate_patch", self._validate_patch)
        builder.add_node("await_user_confirmation", self._await_user_confirmation)
        builder.add_node("create_remediation_pr", self._create_remediation_pr)
        builder.add_node("notify_api", self._notify_api)

        builder.add_edge(START, "classify_findings")
        builder.add_edge("classify_findings", "resolve_manifest_path")
        builder.add_edge("resolve_manifest_path", "fetch_context")
        builder.add_edge("fetch_context", "check_existing_pr")
        builder.add_conditional_edges("check_existing_pr", self._route_after_check_pr)
        builder.add_edge("analyze_findings", "generate_yaml_patch")
        builder.add_edge("generate_yaml_patch", "validate_patch")
        builder.add_conditional_edges("validate_patch", self._route_after_validate)
        builder.add_conditional_edges("await_user_confirmation", self._route_after_confirmation)
        builder.add_edge("create_remediation_pr", "notify_api")
        builder.add_edge("notify_api", END)

        return builder.compile(checkpointer=MemorySaver())

    @property
    def compiled(self):
        return self._graph


def _to_ai_config(ai_config: dict):
    from src.domain.models import TenantAiConfig

    return TenantAiConfig(
        provider=ai_config.get("provider", "openai"),
        model=ai_config.get("model", "gpt-4o"),
        api_key=ai_config.get("api_key", ""),
        github_token=ai_config.get("github_token"),
        github_base_branch=ai_config.get("github_base_branch", "main"),
        monthly_token_budget=ai_config.get("monthly_token_budget"),
        tokens_used_month=ai_config.get("tokens_used_month", 0),
    )
