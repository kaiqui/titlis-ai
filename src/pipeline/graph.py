import asyncio
import base64 as _b64
import json
import re
from datetime import datetime
from typing import Any, Dict, Optional

import yaml  # type: ignore[import-untyped]
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from src.infrastructure.github_app_client import resolve_installation_id
from src.infrastructure.mcp.github_mcp import github_mcp_session
from src.infrastructure.titlis_api.scorecard_client import ScorecardClient
from src.infrastructure.titlis_api.knowledge_client import KnowledgeClient
from src.pipeline.state import ScorecardRemediationState
from src.services.embedding_service import EmbeddingService
from src.services.llm_service import LLMService
from src.tools.github_tools import _check_never_reduce, _never_reduce_violated, _parse_repo
from src.utils.logger import get_logger

logger = get_logger(__name__)

_MAX_RETRIES = 3
_MCP_CALL_TIMEOUT = 30.0
_LLM_CALL_TIMEOUT = 120.0


async def _call_tool(session: Any, tool_name: str, args: dict) -> Any:
    return await asyncio.wait_for(session.call_tool(tool_name, args), timeout=_MCP_CALL_TIMEOUT)


async def _call_tool_strict(session: Any, tool_name: str, args: dict) -> Any:
    result = await _call_tool(session, tool_name, args)
    if getattr(result, "isError", False):
        text = ""
        content = getattr(result, "content", None)
        if content:
            text = getattr(content[0], "text", "")
        raise RuntimeError(f"GitHub MCP '{tool_name}' retornou erro: {text[:400]}")
    return result


async def _github_session_kwargs(ai_config: dict) -> dict:
    auth_mode = ai_config.get("github_auth_mode", "pat")
    if auth_mode == "github_app":
        app_id = ai_config.get("github_app_id")
        priv_key = ai_config.get("github_app_private_key")
        if app_id and priv_key:
            install_id = ai_config.get("github_app_installation_id") or await resolve_installation_id(app_id, priv_key)
            if install_id:
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


def _github_file_text(result) -> str:
    raw = _mcp_text(result)
    if not raw:
        return ""
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict) and parsed.get("encoding") == "base64" and parsed.get("content"):
            content = parsed["content"].replace("\n", "")
            return _b64.b64decode(content).decode("utf-8")
    except Exception:
        pass
    return raw


def _extract_file_content(result) -> str:
    if not result or not result.content:
        return ""

    for item in result.content:
        # EmbeddedResource — github-mcp-server v1.0.5 retorna o conteúdo aqui
        resource = getattr(item, "resource", None)
        if resource is not None:
            # TextResourceContents
            text = getattr(resource, "text", None)
            if text:
                return _decode_if_base64(text)
            # BlobResourceContents
            blob = getattr(resource, "blob", None)
            if blob:
                try:
                    return _b64.b64decode(blob).decode("utf-8")
                except Exception:
                    pass

        # TextContent com JSON wrapper (GitHub API raw)
        text = getattr(item, "text", None)
        if text:
            decoded = _github_file_text_from_str(text)
            # Se não é a mensagem de status e parece conteúdo real
            if decoded and not decoded.startswith("successfully"):
                return decoded

    return ""


def _decode_if_base64(text: str) -> str:
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict) and parsed.get("encoding") == "base64" and parsed.get("content"):
            return _b64.b64decode(parsed["content"].replace("\n", "")).decode("utf-8")
    except Exception:
        pass
    return text


def _github_file_text_from_str(raw: str) -> str:
    if not raw:
        return ""
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict) and parsed.get("encoding") == "base64" and parsed.get("content"):
            return _b64.b64decode(parsed["content"].replace("\n", "")).decode("utf-8")
    except Exception:
        pass
    return raw


def _detect_env_from_cluster(cluster_name: str) -> str:
    name = cluster_name.lower()
    # Verifica preprod/staging antes de prod para evitar falso positivo ("preprod" contém "prod")
    if any(kw in name for kw in ("preprod", "pre-prod", "staging", "stg")):
        return "hml"
    _exact_prod = name == "prod" or name.endswith("-prod") or name.startswith("prod-") or "production" in name
    if _exact_prod:
        return "prd"
    if "prod" in name:
        return "prd"
    if any(kw in name for kw in ("hml", "homolog")):
        return "hml"
    if any(kw in name for kw in ("dev", "development")):
        return "dev"
    return ""


def _detect_env_from_namespace(namespace: str) -> str:
    ns = namespace.lower()
    # Verifica preprod/staging antes de "prod" para evitar falso positivo
    if any(kw in ns for kw in ("preprod", "pre-prod", "staging", "stg", "hml", "homolog")):
        return "hml"
    if any(kw in ns for kw in ("prod", "production")):
        return "prd"
    if any(kw in ns for kw in ("dev", "develop", "sandbox")):
        return "dev"
    return ""


def _render_service_yaml(form: dict) -> str:
    env = form.get("env") or "dev"
    path_entry: dict = {"path": form.get("path", ""), "base_branch": form.get("base_branch", "main")}
    paths: dict = {env: path_entry}
    if form.get("extra_paths"):
        paths.update(form["extra_paths"])

    owner_block: dict = {"team": form.get("team", "")}
    if form.get("contacts"):
        owner_block["contacts"] = form["contacts"]

    doc = {
        "metadata": {
            "name": form.get("name", ""),
            "workload_match": {
                "namespaces": form.get("namespaces") or [],
                "name_pattern": form.get("name_pattern", ""),
            },
        },
        "spec": {
            "owner": owner_block,
            "gitops": {"paths": paths},
            "remediation": {"enabled": True},
        },
    }
    return yaml.safe_dump(doc, sort_keys=False, allow_unicode=True)


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

    async def _read_service_yaml(
        self,
        repo_url: str,
        ai_config: dict,
        path: str = ".titlis/service.yaml",
    ) -> Dict[str, Any] | None:
        kwargs = await _github_session_kwargs(ai_config)
        logger.info(
            "read_service_yaml: auth_mode=%s has_token=%s has_kwargs=%s path=%s",
            ai_config.get("github_auth_mode", "?"),
            bool(ai_config.get("github_token")),
            bool(kwargs),
            path,
        )
        if not kwargs:
            logger.warning("read_service_yaml: sem credenciais GitHub — token não configurado ou vazio")
            return None
        try:
            owner, name = _parse_repo(repo_url)
            async with github_mcp_session(**kwargs) as session:
                result = await _call_tool(
                    session,
                    "get_file_contents",
                    {"owner": owner, "repo": name, "path": path},
                )
                is_err = getattr(result, "isError", False)
                logger.info("read_service_yaml: get_file_contents isError=%s", is_err)
                if is_err:
                    content = getattr(result, "content", None)
                    err_text = getattr(content[0], "text", "") if content else ""
                    logger.warning(
                        "read_service_yaml: MCP error — %s",
                        err_text[:200],
                        extra={"repo": f"{owner}/{name}", "path": path},
                    )
                    return None
                text = _extract_file_content(result)
                logger.info("read_service_yaml: content_len=%d", len(text))
                if text:
                    parsed = yaml.safe_load(text)
                    has_spec = isinstance(parsed, dict) and "spec" in parsed
                    logger.info("read_service_yaml: parsed OK has_spec=%s", has_spec)
                    return parsed
                logger.warning("read_service_yaml: conteúdo não encontrado no resultado do MCP")
        except Exception as exc:
            logger.warning(
                "read_service_yaml: exception — %s",
                str(exc)[:200],
                extra={"repo_url": repo_url, "path": path},
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

        # Tenta ler o service.yaml (caminho configurado no vínculo; default raiz do repo)
        svc_yaml_path = (state.get("service_yaml_path") or ".titlis/service.yaml").strip().lstrip("/")
        if repo_url:
            svc = await self._read_service_yaml(repo_url, ai_config, svc_yaml_path)
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

        # Fallback: service.yaml ausente — pede ao dev para criar via formulário
        deployment_name = state.get("deployment_name", "")
        base_branch = ai_config.get("github_base_branch") or "main"
        suggested_path = state.get("deploy_manifest_path", "")
        prefill = {
            "name": deployment_name,
            "team": labels.get("titlis.io/team") or labels.get("team") or "",
            "name_pattern": f"^{deployment_name}$" if deployment_name else "",
            "namespaces": [namespace] if namespace else [],
            "env": env if env and env != "unknown" else "dev",
            "path": suggested_path,
            "base_branch": base_branch,
        }
        form = interrupt(
            {
                "type": "service_yaml_required",
                "detected_environment": env,
                "deployment_name": deployment_name,
                "namespace": namespace,
                "suggested_path": suggested_path,
                "prefill": prefill,
            }
        )

        # LangGraph 0.3.5: Command(resume=dict) pode vazar para interrupts subsequentes.
        # Por isso o resume value é sempre uma JSON string — desserializamos aqui.
        form_data: Optional[dict] = None
        if isinstance(form, str):
            try:
                parsed = json.loads(form)
                if isinstance(parsed, dict):
                    form_data = parsed
            except (json.JSONDecodeError, ValueError):
                pass
        elif isinstance(form, dict):
            # compat defensivo: se vier dict mesmo assim, aceita
            form_data = form

        if form_data is not None:
            generated_yaml = _render_service_yaml(form_data)
            return {
                "deploy_manifest_path": form_data.get("path", suggested_path),
                "effective_base_branch": form_data.get("base_branch") or base_branch,
                "detected_environment": env,
                "generated_service_yaml": generated_yaml,
                "service_yaml_missing": True,
                "service_yaml_path": svc_yaml_path,
                "service_yaml_prefill": prefill,
            }

        # fallback: string simples = caminho direto do manifesto (legado)
        return {
            "deploy_manifest_path": str(form),
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

        rag_chunks, (current_manifest, manifest_error) = await asyncio.gather(rag_task, manifest_task)

        return {
            "rag_context": rag_chunks,
            "current_manifest": current_manifest,
            "manifest_fetch_error": manifest_error,
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

    async def _fetch_manifest(self, repo_url, branch, path, ai_config) -> tuple[str | None, str | None]:
        kwargs = await _github_session_kwargs(ai_config)
        if not kwargs or not repo_url:
            msg = "Token GitHub não configurado — acesse Configurações → Integrações."
            logger.warning("Token GitHub ausente — não é possível ler o manifesto")
            return None, msg
        try:
            owner, name = _parse_repo(repo_url)
            async with github_mcp_session(**kwargs) as session:
                result = await _call_tool(
                    session,
                    "get_file_contents",
                    {"owner": owner, "repo": name, "path": path, "ref": branch},
                )
                if getattr(result, "isError", False):
                    err_text = _mcp_text(result)
                    logger.error(
                        "MCP get_file_contents retornou erro",
                        extra={"path": path, "branch": branch, "repo": f"{owner}/{name}", "error": err_text[:300]},
                    )
                    lower = err_text.lower()
                    is_ref_error = (
                        "could not resolve ref" in lower
                        or "no commit found for the ref" in lower
                    )
                    if is_ref_error:
                        msg = (
                            f"Branch '{branch}' não encontrado no repositório {owner}/{name}. "
                            f"Verifique se o branch existe ou corrija o arquivo .titlis/service.yaml "
                            f"(configuração para o ambiente detectado)."
                        )
                    else:
                        msg = (
                            f"Não foi possível ler '{path}' (branch: {branch}) em {owner}/{name}. "
                            f"Verifique se o arquivo existe nesse branch e se o token tem permissão de leitura. "
                            f"Detalhe: {err_text[:200]}"
                        )
                    return None, msg
                text = _extract_file_content(result)
                if not text:
                    logger.warning("Manifesto vazio ou não encontrado", extra={"path": path, "branch": branch})
                    return None, f"Arquivo '{path}' (branch: {branch}) está vazio ou não encontrado em {owner}/{name}."
                logger.info("Manifesto lido com sucesso", extra={"path": path, "chars": len(text)})
                return text, None
        except Exception as exc:
            err_str = str(exc)[:300]
            logger.error(
                "Exceção ao ler manifest via MCP GitHub",
                extra={"path": path, "branch": branch, "error": err_str},
            )
            return None, f"Erro ao ler o manifesto de {repo_url} (branch: {branch}, path: {path}): {err_str}"

    async def _check_existing_pr(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        ai_config = state.get("ai_config", {})
        kwargs = await _github_session_kwargs(ai_config)
        repo_url = state.get("repo_url", "")
        namespace = state.get("namespace", "")
        deployment_name = state.get("deployment_name", "")
        tenant_id = state.get("tenant_id", 0)
        workload_id = state.get("workload_id", "")
        base_branch = state.get("effective_base_branch") or ai_config.get("github_base_branch", "main")

        if not kwargs or not repo_url:
            return {"existing_pr": None}

        safe_name = deployment_name.replace("/", "-")
        branch_prefix = f"fix/auto-remediation-{namespace}-{safe_name}-"

        try:
            owner, name = _parse_repo(repo_url)
            async with github_mcp_session(**kwargs) as session:
                result = await _call_tool(
                    session,
                    "list_pull_requests",
                    {"owner": owner, "repo": name, "state": "open", "base": base_branch},
                )
                prs = json.loads(_mcp_text(result) or "[]")
                for pr in prs:
                    head_ref = pr.get("head", {}).get("ref", "")
                    if head_ref.startswith(branch_prefix):
                        existing_url = pr.get("html_url") or pr.get("url", "")
                        m = re.search(r"/pull/(\d+)$", existing_url)
                        existing_number = int(m.group(1)) if m else (pr.get("number") or 0)
                        return {
                            "existing_pr": {
                                "pr_url": existing_url,
                                "pr_number": existing_number,
                                "branch": head_ref,
                            }
                        }

                # Nenhum PR aberto com o prefixo — verifica se o DB tem um registro ativo
                # cujo PR foi fechado sem merge e sincroniza o status.
                await self._sync_closed_pr_status(session, owner, name, tenant_id, workload_id)
        except Exception:
            logger.warning("Falha ao verificar PR existente via MCP GitHub")

        return {"existing_pr": None}

    async def _sync_closed_pr_status(
        self,
        session: Any,
        owner: str,
        repo: str,
        tenant_id: int,
        workload_id: str,
    ) -> None:
        if not tenant_id or not workload_id:
            return
        try:
            current = await self._scorecard.get_current_remediation(tenant_id, workload_id)
            if not current:
                return
            status = current.get("status") or ""
            pr_number = current.get("github_pr_number") or None
            if status not in ("IN_PROGRESS", "PR_OPEN") or not pr_number:
                return

            pr_result = await _call_tool(
                session,
                "get_pull_request",
                {"owner": owner, "repo": repo, "pullNumber": pr_number},
            )
            if getattr(pr_result, "isError", False):
                return

            pr_raw = _mcp_text(pr_result)
            try:
                pr_data = json.loads(pr_raw)
            except Exception:
                return

            if pr_data.get("state") == "closed" and not pr_data.get("merged", False):
                logger.info(
                    "PR fechado sem merge detectado — atualizando status para PR_CLOSED",
                    extra={"pr_number": pr_number, "workload_id": workload_id, "tenant_id": tenant_id},
                )
                await self._scorecard.notify_pr_closed(tenant_id, workload_id)
        except Exception as exc:
            logger.warning(
                "Falha ao sincronizar status de PR fechado",
                extra={"workload_id": workload_id, "error": str(exc)[:200]},
            )

    async def _analyze_findings(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        fetch_error = state.get("manifest_fetch_error")
        if fetch_error:
            raise RuntimeError(fetch_error)

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

        import time as _time

        tenant_id = state.get("tenant_id", 0)
        model = ai_config.get("model", "?")
        logger.info(
            "LLM analyze_findings: iniciando",
            extra={"tenant_id": tenant_id, "model": model, "findings_count": len(findings)},
        )
        _t0 = _time.monotonic()
        try:
            analysis = await asyncio.wait_for(
                self._llm.chat(messages, _to_ai_config(ai_config), tenant_id),
                timeout=_LLM_CALL_TIMEOUT,
            )
        except asyncio.TimeoutError:
            elapsed = _time.monotonic() - _t0
            logger.error(
                "LLM analyze_findings: timeout",
                extra={"tenant_id": tenant_id, "model": model, "elapsed_s": round(elapsed, 1)},
            )
            raise RuntimeError(
                f"LLM ({model}) não respondeu em {int(_LLM_CALL_TIMEOUT)}s. Verifique a API key ou tente novamente."
            )
        elapsed = _time.monotonic() - _t0
        logger.info(
            "LLM analyze_findings: concluído",
            extra={"tenant_id": tenant_id, "model": model, "elapsed_s": round(elapsed, 1)},
        )
        return {"analysis": analysis}

    @staticmethod
    def _detect_deployments_in_manifest(manifest: str) -> list[str]:
        names = []
        try:
            docs = list(yaml.safe_load_all(manifest))
            for doc in docs:
                if isinstance(doc, dict) and doc.get("kind") == "Deployment":
                    meta = doc.get("metadata") or {}
                    name = meta.get("name", "")
                    if name:
                        names.append(name)
        except yaml.YAMLError:
            pass
        return names

    async def _generate_yaml_patch(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        ai_config = state.get("ai_config", {})
        analysis = state.get("analysis", "")
        current_manifest = state.get("current_manifest", "")
        findings = state.get("findings", [])
        deployment_name = state.get("deployment_name", "")
        retry_count = state.get("retry_count", 0)
        validation_errors = state.get("validation_errors", [])

        if not current_manifest:
            fetch_error = state.get("manifest_fetch_error")
            if fetch_error:
                raise RuntimeError(fetch_error)
            path = state.get("deploy_manifest_path", "desconhecido")
            raise RuntimeError(
                f"Não foi possível ler o manifesto atual do GitHub (path: {path}). "
                "Verifique se o caminho está correto e se o token tem permissão de leitura no repositório."
            )

        error_feedback = ""
        if validation_errors:
            error_feedback = "\n\n**Erros da tentativa anterior (corrija):**\n" + "\n".join(validation_errors)

        findings_str = "\n".join(
            f"- {f.get('rule_id')}: {f.get('message', '')} (actual={f.get('actual_value', 'N/A')})" for f in findings
        )

        # Detecta múltiplos Deployments no arquivo para proteger os não-alvo
        multi_deployment_guard = ""
        if current_manifest:
            all_deployments = self._detect_deployments_in_manifest(current_manifest)
            other_deployments = [d for d in all_deployments if d != deployment_name]
            if other_deployments:
                others_list = ", ".join(f"`{d}`" for d in other_deployments)
                multi_deployment_guard = (
                    f"\n\nAVISO CRÍTICO — este arquivo contém múltiplos Deployments: "
                    f"{others_list} além de `{deployment_name}`. "
                    f"Modifique EXCLUSIVAMENTE o Deployment `{deployment_name}`. "
                    f"Os outros Deployments devem ser retornados exatamente como estão no manifest atual — "
                    f"sem nenhuma alteração, mesmo que pareçam ter problemas."
                )
                logger.info(
                    "Múltiplos Deployments detectados no manifest",
                    extra={"target": deployment_name, "others": other_deployments},
                )

        messages = [
            {
                "role": "system",
                "content": (
                    "Você é um especialista em Kubernetes. "
                    "Sua tarefa é retornar o conteúdo COMPLETO E INTEGRAL do arquivo deploy.yaml após aplicar as correções. "
                    "REGRAS ABSOLUTAS:\n"
                    "1. Retorne TODOS os documentos YAML do arquivo, separados por '---', exatamente como no original. "
                    "Não remova nenhum recurso (Deployment, Service, HPA, ConfigMap, Secret, Ingress, etc.).\n"
                    "2. Altere SOMENTE os campos necessários para corrigir os findings listados dentro do Deployment alvo. "
                    "Todos os outros campos, em todos os recursos, devem ser idênticos ao original.\n"
                    "3. NUNCA reduza valores de cpu, memory ou replicas — apenas aumente ou adicione.\n"
                    "4. Não adicione explicações, comentários extras ou blocos markdown. Retorne apenas YAML válido.\n"
                    f"{multi_deployment_guard}"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"**Deployment alvo:** `{deployment_name}` (namespace: `{state.get('namespace', '')}`)\n\n"
                    f"**Análise:**\n{analysis}\n\n"
                    f"**Findings a corrigir:**\n{findings_str}\n\n"
                    f"**Arquivo atual (retorne-o COMPLETO com todos os recursos, apenas corrigindo os findings acima):**\n"
                    f"{current_manifest or 'não disponível'}"
                    f"{error_feedback}"
                ),
            },
        ]

        import time as _time

        tenant_id = state.get("tenant_id", 0)
        model = ai_config.get("model", "?")
        logger.info(
            "LLM generate_yaml_patch: iniciando", extra={"tenant_id": tenant_id, "model": model, "retry": retry_count}
        )
        _t0 = _time.monotonic()
        try:
            patched = await asyncio.wait_for(
                self._llm.chat(messages, _to_ai_config(ai_config), tenant_id),
                timeout=_LLM_CALL_TIMEOUT,
            )
        except asyncio.TimeoutError:
            elapsed = _time.monotonic() - _t0
            logger.error(
                "LLM generate_yaml_patch: timeout",
                extra={"tenant_id": tenant_id, "model": model, "elapsed_s": round(elapsed, 1)},
            )
            raise RuntimeError(
                f"LLM ({model}) não respondeu em {int(_LLM_CALL_TIMEOUT)}s ao gerar patch. Verifique a API key."
            )
        elapsed = _time.monotonic() - _t0
        logger.info(
            "LLM generate_yaml_patch: concluído",
            extra={"tenant_id": tenant_id, "model": model, "elapsed_s": round(elapsed, 1)},
        )
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
        patched = state.get("patched_manifest") or ""
        current = state.get("current_manifest") or ""
        errors = []

        try:
            list(yaml.safe_load_all(patched))
        except yaml.YAMLError as exc:
            errors.append(f"YAML inválido: {exc}")
            return {"validation_errors": errors}

        if current:
            violation = _check_never_reduce(current, patched)
            if violation:
                errors.append(violation)

        return {"validation_errors": errors}

    async def _await_user_confirmation(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        files = [
            {
                "path": state.get("deploy_manifest_path", ""),
                "current": state.get("current_manifest") or "",
                "patched": state.get("patched_manifest") or "",
                "is_new": False,
            }
        ]
        if state.get("generated_service_yaml"):
            files.append(
                {
                    "path": state.get("service_yaml_path") or ".titlis/service.yaml",
                    "current": "",
                    "patched": state.get("generated_service_yaml", ""),
                    "is_new": True,
                }
            )
        approved = interrupt(
            {
                "patched_manifest": state.get("patched_manifest"),
                "current_manifest": state.get("current_manifest"),
                "findings": [f.get("rule_id") for f in state.get("findings", [])],
                "deployment_name": state.get("deployment_name"),
                "namespace": state.get("namespace"),
                "files": files,
            }
        )
        # Guard: resume deve ser booleano (True = aprovar, False = rejeitar).
        # Valor diferente indica bug de LangGraph vazando o resume de outro interrupt.
        if not isinstance(approved, bool):
            raise RuntimeError(
                "Sessão de remediação inválida (estado interno corrompido). "
                "Feche esta janela e inicie uma nova remediação."
            )
        return {"approved": approved}

    async def _create_remediation_pr(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        ai_config = state.get("ai_config", {})
        kwargs = await _github_session_kwargs(ai_config)
        base_branch = state.get("effective_base_branch") or ai_config.get("github_base_branch", "main")
        repo_url = state.get("repo_url", "")
        path = state.get("deploy_manifest_path", "deploy.yaml")
        patched = state.get("patched_manifest") or ""
        findings = [f.get("rule_id", "") for f in state.get("findings", [])]
        namespace = state.get("namespace", "")
        deployment_name = state.get("deployment_name", "")

        owner, name = _parse_repo(repo_url)
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        branch_name = f"fix/auto-remediation-{namespace}-{deployment_name}-{ts}"
        has_new_svc_yaml = bool(state.get("generated_service_yaml"))
        svc_yaml_note = "\n- Criação de `.titlis/service.yaml`" if has_new_svc_yaml else ""
        commit_msg = f"fix(titlis-ai): auto-remediation {deployment_name} [{', '.join(findings)}]"
        pr_body = (
            f"## Remediação automática — {deployment_name}\n\n"
            f"**Namespace:** {namespace}\n\n"
            f"**Findings corrigidos:**\n"
            + "\n".join(f"- {f}" for f in findings)
            + svc_yaml_note
            + "\n\n*Gerado pelo Titlis AI Assistant*"
        )

        if not kwargs:
            raise RuntimeError("Token GitHub não configurado — acesse Configurações › Integrações")

        if not patched.strip():
            raise RuntimeError("Patch YAML está vazio — nenhuma alteração a commitar")

        async with github_mcp_session(**kwargs) as session:
            logger.info("Criando branch", extra={"branch": branch_name, "base": base_branch})
            await _call_tool_strict(
                session,
                "create_branch",
                {"owner": owner, "repo": name, "branch": branch_name, "from_branch": base_branch},
            )

            files_to_push = [{"path": path, "content": patched}]
            if has_new_svc_yaml:
                files_to_push.append(
                    {
                        "path": state.get("service_yaml_path") or ".titlis/service.yaml",
                        "content": state.get("generated_service_yaml") or "",
                    }
                )
            logger.info(
                "Fazendo push de arquivos",
                extra={"paths": [f["path"] for f in files_to_push], "branch": branch_name},
            )
            await _call_tool_strict(
                session,
                "push_files",
                {
                    "owner": owner,
                    "repo": name,
                    "branch": branch_name,
                    "message": commit_msg,
                    "files": files_to_push,
                },
            )

            logger.info("Criando Pull Request", extra={"branch": branch_name, "base": base_branch})
            result = await _call_tool_strict(
                session,
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

        raw = _mcp_text(result) or ""
        try:
            pr_data = json.loads(raw)
        except Exception:
            pr_data = {}

        pr_url = pr_data.get("html_url") or pr_data.get("url", "")
        if not pr_url:
            raise RuntimeError(f"create_pull_request não retornou URL — resposta do MCP: {raw[:400]}")
        m = re.search(r"/pull/(\d+)$", pr_url)
        pr_number = int(m.group(1)) if m else (pr_data.get("number") or 0)

        return {
            "pr_result": {
                "pr_url": pr_url,
                "pr_number": pr_number,
                "branch": branch_name,
            }
        }

    async def _notify_api(self, state: ScorecardRemediationState) -> Dict[str, Any]:
        pr = state.get("pr_result", {}) or {}
        tenant_id = state.get("tenant_id", 0)
        workload_id = state.get("workload_id", "")
        pr_url = pr.get("pr_url")
        pr_number = pr.get("pr_number")
        try:
            await self._scorecard.notify_remediation_started(
                tenant_id=tenant_id,
                workload_id=workload_id,
                pr_url=pr_url,
                pr_number=pr_number,
                github_branch=pr.get("branch"),
                repo_url=state.get("repo_url"),
                finding_ids=[f.get("rule_id", "") for f in state.get("findings", [])],
            )
            logger.info(
                "Remediação registrada na API",
                extra={"workload_id": workload_id, "tenant_id": tenant_id, "pr_number": pr_number, "pr_url": pr_url},
            )
        except Exception as exc:
            logger.error(
                "Falha ao registrar remediação na API — PR criado mas não registrado",
                extra={
                    "workload_id": workload_id,
                    "tenant_id": tenant_id,
                    "pr_url": pr_url,
                    "pr_number": pr_number,
                    "error": str(exc)[:300],
                },
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
