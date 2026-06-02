import json
import uuid
from contextlib import AsyncExitStack
from typing import Any, AsyncGenerator, Dict, List, Optional, Set

import litellm

from src.domain.models import AgentToolDecision, TenantAiConfig, ToolProposal
from src.infrastructure.github_app_client import resolve_installation_id
from src.infrastructure.mcp.datadog_mcp import datadog_mcp_session
from src.infrastructure.mcp.github_mcp import github_mcp_session
from src.infrastructure.titlis_api.datadog_config_client import DatadogConfigClient
from src.infrastructure.titlis_api.scorecard_client import ScorecardClient
from src.pipeline.session import AgentSession, SessionStore
from src.services.mcp_adapter import McpAdapter
from src.settings import settings
from src.tools.read_tools import build_read_tools
from src.tools.slo_tools import build_slo_tools
from src.utils.logger import get_logger

logger = get_logger(__name__)

_MCP_CALL_TIMEOUT = 30.0

# GitHub MCP tools que modificam estado — exigem aprovação humana antes de executar.
_WRITE_TOOLS: Set[str] = {
    "update_slo_thresholds",
    # GitHub MCP write tools
    "create_pull_request",
    "push_files",
    "create_or_update_file",
    "merge_pull_request",
    "create_branch",
    "create_repository",
    "delete_file",
    "update_pull_request",
    "create_issue",
    "update_issue",
    "add_issue_comment",
    "create_pull_request_review",
    "request_copilot_review",
}

_SYSTEM_PROMPT_BASE = """Você é ARIA (Assistente de Remediação Inteligente Autônoma), especialista em operações SRE Kubernetes na plataforma Titlis.

Domínio de atuação:
- Análise de findings de scorecard de compliance Kubernetes
- Diagnóstico de problemas de workloads (resource limits, liveness/readiness probes, HPA, anotações)
- Remediação de Deployments com abertura de Pull Requests no GitHub
- Consulta e ajuste de SLOs
- Leitura de histórico de remediações anteriores
- Consulta a repositórios, branches, arquivos e PRs no GitHub (suporte à remediação)
- Consulta a métricas, monitors, dashboards, aplicações/serviços APM, hosts e infraestrutura no Datadog

Se o usuário perguntar sobre algo completamente fora deste domínio (ex: receitas, esportes, política), responda EXATAMENTE com:
FORA_DO_ESCOPO: <explicação objetiva do que está fora do escopo e o que você pode ajudar>

Ao analisar problemas:
1. Use as ferramentas disponíveis para buscar dados reais ANTES de pedir informações ao usuário
2. Quando o namespace não for informado, use list_all_workloads para descobrir os workloads disponíveis
3. Nunca pergunte por namespace, workload_id ou outros dados que você pode buscar com as ferramentas
4. Para criar PRs de remediação, use as ferramentas do GitHub MCP (create_branch, push_files, create_pull_request)
5. Valide que resources nunca são reduzidos antes de criar PRs (never-reduce policy)
6. Seja objetivo e técnico nas respostas
7. Para QUALQUER pergunta sobre Datadog (aplicações, serviços, métricas, monitors, dashboards, hosts, alertas, SLOs do Datadog), use imediatamente as ferramentas do Datadog MCP — nunca recuse ou diga que não consegue consultar o Datadog
8. Após executar qualquer tool de escrita (create_pull_request, push_files, create_branch, merge_pull_request, create_issue, update_issue, update_slo_thresholds), apresente SEMPRE um resumo em PT-BR do que foi feito: o que foi criado/modificado, links retornados (PR URL, issue URL, etc.) e próximos passos relevantes. Nunca encerre silenciosamente após uma operação de escrita.

## Descoberta automática de repositório e manifests

### ⚠️ Docker Hub ≠ GitHub — regra crítica

O namespace do Docker Hub (`containers[].image`, ex: `kailima/titlis-api:v1.2`) **NÃO corresponde**
ao owner/repositório no GitHub. São plataformas com usuários separados e independentes.

**NUNCA** infira o owner ou repositório GitHub a partir de:
- Nome da imagem Docker (`containers[].image` ou `image_tag`)
- Qualquer campo que contenha o nome da imagem ou do registry
- Heurísticas baseadas em nomes similares

A única fonte confiável do repositório GitHub é:
1. O label `titlis.io/repo` no workload (ex: `kaiqui/titlis-api`)
2. O que o usuário informar explicitamente nesta conversa

Se o usuário corrigir o repo (`"o correto é kaiqui/titlis-api, não kailima"`), adote
imediatamente o valor informado pelo usuário e descarte qualquer inferência anterior.

---

Cada serviço pode ter um arquivo `.titlis/service.yaml` na raiz do seu repositório GitHub
que define os caminhos dos manifests e o nome do serviço no Datadog. Siga este fluxo:

### Passo 1 — Obter scorecard e labels do workload
Use `get_deployment_spec(namespace, name)`. O resultado inclui `labels` com campos como:
- `titlis.io/github-owner`: owner no GitHub (ex: `kaiqui`)
- `titlis.io/repo`: nome do repositório no GitHub (ex: `titlis-api`)
- `app.kubernetes.io/name`: nome canônico do serviço

O repositório GitHub completo é: `labels["titlis.io/github-owner"] + "/" + labels["titlis.io/repo"]`
Exemplo: owner=`kaiqui`, repo=`titlis-api` → `kaiqui/titlis-api`

**Importante:** `titlis.io/repo` contém APENAS o nome do repo (sem o owner). O owner vem de
`titlis.io/github-owner`. Nunca confunda com a imagem Docker (`kailima/titlis-api`) — são
namespaces diferentes.

### Passo 2 — Ler .titlis/service.yaml
Se `labels["titlis.io/github-owner"]` e `labels["titlis.io/repo"]` estiverem presentes, chame:
```
get_file_contents(owner=<titlis.io/github-owner>, repo=<titlis.io/repo>, path=".titlis/service.yaml")
```
O YAML retornado tem este formato:
```yaml
spec:
  gitops:
    paths:
      dev: { path: manifest/kubernetes/develop/deploy.yaml, base_branch: develop }
      hml: { path: manifest/kubernetes/release/deploy.yaml, base_branch: release }
      prd: { path: manifest/kubernetes/main/deploy.yaml,     base_branch: main }
  remediation:
    overrides:
      hpa_environment_templates:
        dev: { min: 1, max: 3, target_cpu: 80 }
        hml: { min: 1, max: 3, target_cpu: 80 }
        prd: { min: 2, max: 6, target_cpu: 70 }
  datadog:
    service: titlis-api
    env: preprod
```

### Passo 3 — Mapear namespace → ambiente
| namespace contém | ambiente |
|---|---|
| prod, production | prd |
| hml, homolog, staging, stg | hml |
| dev, develop | dev |

Use o ambiente para selecionar o path e base_branch corretos em `spec.gitops.paths`.

### Passo 4 — Usar os paths
- Para ler o Deployment: `get_file_contents(owner, repo, path=<gitops_path>, ref=<base_branch>)`
- Para HPA: procure arquivo `hpa.yaml` no mesmo diretório, ou use `search_code` para `kind: HorizontalPodAutoscaler`
- Para criar PR: use o `base_branch` do ambiente correto

### Passo 5 — Linkage com Datadog
Quando o usuário pedir métricas ou quiser criar monitors para um workload:
1. Leia `.titlis/service.yaml` → `spec.datadog.service` é o nome do serviço no Datadog
2. Use as tags `service:<nome>,env:<ambiente>` nas queries de métricas
3. Para monitors relacionados a SLO, use o nome do SLO do scorecard

Se `titlis.io/github-owner` ou `titlis.io/repo` não estiverem nos labels (ou estiverem vazios),
pergunte ao usuário apenas o repositório GitHub completo (ex: `owner/nome-repo`).
Nunca peça os caminhos completos dos arquivos — descubra-os via `search_code` ou lendo
o `.titlis/service.yaml` assim que tiver o repo correto.

## Erros de acesso ao GitHub — Repositório Privado

O GitHub retorna HTTP **404** (não 403) quando o PAT não tem permissão para acessar um repositório
privado. Isso é intencional para evitar revelar a existência de repos privados a tokens sem acesso.

Consequência: uma tool call que retorna `{"error": "Not Found: ..."}` pode significar tanto
"o repositório não existe" quanto "o PAT não tem permissão".

**Regra obrigatória:** quando qualquer tool do GitHub retornar erro contendo "Not Found" ou "404"
para um repositório que você já *conhece* (extraído do label `titlis.io/repo` ou informado pelo
usuário anteriormente nesta conversa):

1. **NÃO** peça ao usuário para confirmar o nome do repositório — a informação é confiável
2. **NÃO** pergunte pela branch padrão — não é essa a causa do problema
3. **Informe diretamente:** "O repositório `<owner/repo>` não está acessível. O GitHub retorna 404
   para repositórios privados quando o Personal Access Token não tem permissão. Provavelmente o PAT
   não possui o scope `repo` (necessário para repositórios privados) ou `read:org` (necessário para
   repos de organizações). Acesse **Configurações → Integrações → GitHub** para atualizar o token."
4. Não tente adivinhar caminhos nem prosseguir com a remediação até que o usuário corrija as credenciais

Idioma: português brasileiro."""

_SYSTEM_PROMPT = _SYSTEM_PROMPT_BASE


def _build_system_prompt(has_github: bool, has_datadog: bool) -> str:
    parts = [_SYSTEM_PROMPT_BASE]
    if has_datadog:
        parts.append(
            "\nVocê tem acesso às ferramentas do Datadog MCP para consultar métricas, monitors, dashboards, aplicações/serviços APM, hosts e infraestrutura. Use-as imediatamente para qualquer pergunta sobre o Datadog — nunca recuse uma consulta Datadog quando as ferramentas estiverem disponíveis."
        )
    else:
        parts.append(
            "\nAVISO CRÍTICO: As ferramentas do Datadog NÃO estão disponíveis nesta sessão porque as credenciais não estão configuradas. Para QUALQUER pergunta sobre o Datadog, responda EXATAMENTE: 'As credenciais do Datadog não estão configuradas para este tenant. Acesse Configurações → Integrações → Datadog para configurá-las.' Nunca diga que não consegue consultar o Datadog por falta de capacidade — o problema é somente a ausência de credenciais."
        )
    if not has_github:
        parts.append(
            "AVISO: As ferramentas do GitHub NÃO estão disponíveis nesta sessão. Não é possível criar branches, commits ou Pull Requests."
        )
    return "\n".join(parts)


class _ToolRunner:
    def __init__(
        self,
        adapter: McpAdapter,
        openai_tools: List[Dict[str, Any]],
        github_session: Any,
        dd_session: Any,
        github_tool_names: Set[str],
        dd_tool_names: Set[str],
        has_github: bool = False,
        has_datadog: bool = False,
    ) -> None:
        self._adapter = adapter
        self.openai_tools = openai_tools
        self._github_session = github_session
        self._dd_session = dd_session
        self._github_tool_names = github_tool_names
        self._dd_tool_names = dd_tool_names
        self.has_github = has_github
        self.has_datadog = has_datadog

    async def execute(self, tool_name: str, args: Dict[str, Any]) -> Any:
        # Sem asyncio.wait_for aqui: a sessão MCP é long-lived por turn e
        # compartilhada entre múltiplas tool calls. Cancelar via wait_for no
        # meio de I/O stdio corrompe o estado interno do cliente MCP,
        # quebrando todas as chamadas subsequentes no mesmo turn.
        # O timeout de operações MCP no agente é gerenciado pela desconexão
        # do cliente SSE e pelo keepalive_stream da rota.
        if tool_name in self._github_tool_names and self._github_session is not None:
            result = await self._github_session.call_tool(tool_name, args)
            return _parse_mcp_result(result)
        if tool_name in self._dd_tool_names and self._dd_session is not None:
            result = await self._dd_session.call_tool(tool_name, args)
            return _parse_mcp_result(result)
        return await self._adapter.execute(tool_name, args)


class AgentService:
    def __init__(
        self,
        scorecard_client: ScorecardClient,
        session_store: SessionStore,
        dd_client: DatadogConfigClient,
    ) -> None:
        self._scorecard = scorecard_client
        self._store = session_store
        self._dd_client = dd_client

    async def _build_runner(self, session: AgentSession, stack: AsyncExitStack) -> _ToolRunner:
        ai_cfg = session.ai_config
        tenant_id = session.tenant_id

        adapter = McpAdapter(
            build_read_tools(self._scorecard, tenant_id),
            build_slo_tools(self._scorecard, tenant_id),
        )
        custom_tools = adapter.to_openai_tools()

        github_session: Optional[Any] = None
        github_tool_names: Set[str] = set()
        github_mcp_tools: List[Dict[str, Any]] = []
        github_auth_mode = ai_cfg.get("github_auth_mode", "pat")
        github_token = ai_cfg.get("github_token")
        github_app_id = ai_cfg.get("github_app_id")
        github_app_private_key = ai_cfg.get("github_app_private_key")
        github_app_installation_id = ai_cfg.get("github_app_installation_id")

        if github_auth_mode == "github_app" and github_app_id and github_app_private_key:
            resolved_installation_id = github_app_installation_id or await resolve_installation_id(
                github_app_id, github_app_private_key
            )
            if resolved_installation_id:
                try:
                    github_session = await stack.enter_async_context(
                        github_mcp_session(
                            github_app_id=github_app_id,
                            github_app_private_key=github_app_private_key,
                            github_app_installation_id=resolved_installation_id,
                        )
                    )
                    tool_list = await github_session.list_tools()  # type: ignore[union-attr]
                    for tool in tool_list.tools:
                        github_tool_names.add(tool.name)
                        github_mcp_tools.append(_tool_to_openai(tool))
                    logger.info(
                        "GitHub App MCP iniciado", extra={"tenant_id": tenant_id, "tool_count": len(github_tool_names)}
                    )
                except Exception:
                    logger.exception("GitHub App MCP init falhou — sem tools GitHub neste turno")
            else:
                logger.warning(
                    "GitHub App MCP ignorado — installation_id não encontrado", extra={"tenant_id": tenant_id}
                )
        elif github_token:
            try:
                github_session = await stack.enter_async_context(github_mcp_session(github_token=github_token))
                tool_list = await github_session.list_tools()  # type: ignore[union-attr]
                for tool in tool_list.tools:
                    github_tool_names.add(tool.name)
                    github_mcp_tools.append(_tool_to_openai(tool))
                logger.info("GitHub MCP iniciado", extra={"tenant_id": tenant_id, "tool_count": len(github_tool_names)})
            except Exception:
                logger.exception("GitHub MCP init falhou — sem tools GitHub neste turno")

        dd_session: Optional[Any] = None
        dd_tool_names: Set[str] = set()
        dd_mcp_tools: List[Dict[str, Any]] = []
        try:
            dd_config = await self._dd_client.get_dd_config(tenant_id)
            dd_api_key = (dd_config or {}).get("ddApiKey") or ""
            dd_app_key = (dd_config or {}).get("ddAppKey") or ""
            if dd_api_key:
                dd_session = await stack.enter_async_context(
                    datadog_mcp_session(
                        dd_api_key,
                        dd_app_key,
                        (dd_config or {}).get("site", "datadoghq.com"),
                    )
                )
                tool_list = await dd_session.list_tools()  # type: ignore[union-attr]
                for tool in tool_list.tools:
                    dd_tool_names.add(tool.name)
                    dd_mcp_tools.append(_tool_to_openai(tool))
                logger.info("Datadog MCP iniciado", extra={"tenant_id": tenant_id, "tool_count": len(dd_tool_names)})
            else:
                logger.info("Datadog MCP ignorado — credenciais não configuradas", extra={"tenant_id": tenant_id})
        except Exception:
            logger.exception("Datadog MCP init falhou — sem tools Datadog neste turno")

        all_tools = custom_tools + github_mcp_tools + dd_mcp_tools
        return _ToolRunner(
            adapter=adapter,
            openai_tools=all_tools,
            github_session=github_session,
            dd_session=dd_session,
            github_tool_names=github_tool_names,
            dd_tool_names=dd_tool_names,
            has_github=bool(github_tool_names),
            has_datadog=bool(dd_tool_names),
        )

    def _ai_config(self, session: AgentSession) -> TenantAiConfig:
        cfg = session.ai_config
        return TenantAiConfig(
            provider=cfg.get("provider", "openai"),
            model=cfg.get("model", "gpt-4o"),
            api_key=cfg.get("api_key", ""),
            github_token=cfg.get("github_token"),
            github_base_branch=cfg.get("github_base_branch", "main"),
            monthly_token_budget=cfg.get("monthly_token_budget"),
            tokens_used_month=cfg.get("tokens_used_month", 0),
        )

    async def run_turn(self, session: AgentSession, user_message: str) -> AsyncGenerator[str, None]:
        session.messages.append({"role": "user", "content": user_message})
        session.append_audit({"event": "user_message", "content": user_message})

        ai_config = self._ai_config(session)
        model_id = _build_model_id(ai_config.provider, ai_config.model)

        async with AsyncExitStack() as stack:
            runner = await self._build_runner(session, stack)
            system_prompt = _build_system_prompt(runner.has_github, runner.has_datadog)
            messages = [{"role": "system", "content": system_prompt}] + session.messages
            async for event in self._llm_loop(
                session, messages, runner.openai_tools, model_id, ai_config, runner, system_prompt
            ):
                yield event

    async def run_tool_responses(
        self, session: AgentSession, decisions: List[AgentToolDecision]
    ) -> AsyncGenerator[str, None]:
        ai_config = self._ai_config(session)
        model_id = _build_model_id(ai_config.provider, ai_config.model)

        proposal_map = {p.proposal_id: p for p in session.pending_proposals}
        session.pending_proposals = []

        async with AsyncExitStack() as stack:
            runner = await self._build_runner(session, stack)
            system_prompt = _build_system_prompt(runner.has_github, runner.has_datadog)

            for decision in decisions:
                proposal = proposal_map.get(decision.proposal_id)
                if proposal is None:
                    continue

                session.append_audit(
                    {
                        "event": "tool_decision",
                        "tool": proposal.tool_name,
                        "approved": decision.approved,
                        "proposal_id": decision.proposal_id,
                    }
                )

                if decision.approved:
                    args = decision.edited_args if decision.edited_args is not None else proposal.args
                    try:
                        result = await runner.execute(proposal.tool_name, args)
                        result_str = json.dumps(result, ensure_ascii=False, default=str)
                        session.append_audit(
                            {"event": "tool_result", "tool": proposal.tool_name, "result": result_str[:500]}
                        )
                        if proposal.tool_name == "create_pull_request" and session.workload_id:
                            await self._notify_pr_created(session, args, result)
                        yield _sse(
                            {
                                "type": "tool_result",
                                "proposal_id": decision.proposal_id,
                                "tool_name": proposal.tool_name,
                                "approved": True,
                                "result": result,
                            }
                        )
                        session.messages.append(
                            {"role": "tool", "tool_call_id": decision.proposal_id, "content": result_str}
                        )
                    except Exception as exc:
                        err = str(exc)
                        session.append_audit({"event": "tool_error", "tool": proposal.tool_name, "error": err})
                        yield _sse(
                            {
                                "type": "tool_result",
                                "proposal_id": decision.proposal_id,
                                "tool_name": proposal.tool_name,
                                "approved": True,
                                "error": err,
                            }
                        )
                        session.messages.append(
                            {"role": "tool", "tool_call_id": decision.proposal_id, "content": f"Erro: {err}"}
                        )
                else:
                    yield _sse(
                        {
                            "type": "tool_result",
                            "proposal_id": decision.proposal_id,
                            "tool_name": proposal.tool_name,
                            "approved": False,
                        }
                    )
                    session.messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": decision.proposal_id,
                            "content": "Usuário rejeitou a execução desta ferramenta.",
                        }
                    )

            messages = [{"role": "system", "content": system_prompt}] + session.messages

            async for event in self._llm_loop(
                session, messages, runner.openai_tools, model_id, ai_config, runner, system_prompt
            ):
                yield event

    async def _notify_pr_created(self, session: AgentSession, args: Dict[str, Any], result: Any) -> None:
        if not isinstance(result, dict):
            return
        pr_url = result.get("html_url") or result.get("url")
        pr_number = result.get("number")
        if not pr_url:
            return
        try:
            head = result.get("head") or {}
            branch = head.get("ref") if isinstance(head, dict) else None
            base_repo = (result.get("base") or {}).get("repo") or {}
            repo_url = base_repo.get("html_url") or args.get("repo")
            await self._scorecard.notify_remediation_started(
                tenant_id=session.tenant_id,
                workload_id=session.workload_id,  # type: ignore[arg-type]
                pr_url=pr_url,
                pr_number=pr_number,
                github_branch=branch or args.get("head"),
                repo_url=repo_url,
                finding_ids=[],
            )
            logger.info(
                "Remediação registrada via agente",
                extra={"tenant_id": session.tenant_id, "workload_id": session.workload_id, "pr_url": pr_url},
            )
        except Exception:
            logger.warning("Falha ao registrar remediação via agente", exc_info=True)

    async def _llm_loop(
        self,
        session: AgentSession,
        messages: List[Dict[str, Any]],
        tools: List[Dict[str, Any]],
        model_id: str,
        ai_config: TenantAiConfig,
        runner: _ToolRunner,
        system_prompt: str = _SYSTEM_PROMPT,
    ) -> AsyncGenerator[str, None]:
        _prev_read_tool_names: Optional[frozenset] = None
        for _iteration in range(5):
            result: Dict[str, Any] = {}
            async for chunk in self._stream_llm(messages, tools, model_id, ai_config, session, result):
                if chunk:
                    yield _sse({"type": "thinking", "content": chunk})
            text_acc = result["text_acc"]
            tool_calls_acc = result["tool_calls_acc"]
            finish_reason = result["finish_reason"]

            if text_acc:
                if text_acc.startswith("FORA_DO_ESCOPO:"):
                    reason = text_acc[len("FORA_DO_ESCOPO:") :].strip()
                    session.append_audit({"event": "scope_rejected", "reason": reason})
                    yield _sse({"type": "scope_rejected", "reason": reason})
                    yield _sse({"type": "done"})
                    return

            if finish_reason == "tool_calls" and tool_calls_acc:
                proposals = _build_proposals(tool_calls_acc)

                read_proposals = [p for p in proposals if not p.is_write]
                write_proposals = [p for p in proposals if p.is_write]

                if not write_proposals:
                    current_read_names: frozenset = frozenset(p.tool_name for p in read_proposals)
                    if current_read_names and current_read_names == _prev_read_tool_names:
                        reply = "Não consegui obter as informações necessárias. Tente reformular a pergunta."
                        session.messages.append({"role": "assistant", "content": reply})
                        session.append_audit({"event": "loop_detected", "tools": list(current_read_names)})
                        yield _sse({"type": "message", "content": reply})
                        yield _sse({"type": "done"})
                        return
                    _prev_read_tool_names = current_read_names

                assistant_msg: Dict[str, Any] = {
                    "role": "assistant",
                    "content": text_acc or None,
                    "tool_calls": [
                        {
                            "id": p.proposal_id,
                            "type": "function",
                            "function": {"name": p.tool_name, "arguments": json.dumps(p.args)},
                        }
                        for p in proposals
                    ],
                }
                session.messages.append(assistant_msg)
                messages = messages + [assistant_msg]

                session.append_audit({"event": "tool_proposals", "tools": [p.tool_name for p in proposals]})

                for proposal in read_proposals:
                    try:
                        tool_result = await runner.execute(proposal.tool_name, proposal.args)
                        result_str = json.dumps(tool_result, ensure_ascii=False, default=str)
                        session.append_audit(
                            {"event": "tool_result", "tool": proposal.tool_name, "result": result_str[:500]}
                        )
                        session.messages.append(
                            {"role": "tool", "tool_call_id": proposal.proposal_id, "content": result_str}
                        )
                    except Exception as exc:
                        err = str(exc)
                        session.append_audit({"event": "tool_error", "tool": proposal.tool_name, "error": err})
                        session.messages.append(
                            {"role": "tool", "tool_call_id": proposal.proposal_id, "content": f"Erro: {err}"}
                        )

                if write_proposals:
                    session.pending_proposals = write_proposals
                    yield _sse(
                        {
                            "type": "awaiting_approvals",
                            "proposals": [p.model_dump() for p in write_proposals],
                        }
                    )
                    yield _sse({"type": "done"})
                    return

                messages = [{"role": "system", "content": system_prompt}] + session.messages
                continue

            if text_acc:
                session.messages.append({"role": "assistant", "content": text_acc})
                session.append_audit({"event": "assistant_message", "content": text_acc[:200]})
                yield _sse({"type": "message", "content": text_acc})
                yield _sse({"type": "done"})
                return

            yield _sse({"type": "done"})
            return

    async def _stream_llm(
        self,
        messages: List[Dict[str, Any]],
        tools: List[Dict[str, Any]],
        model_id: str,
        ai_config: TenantAiConfig,
        session: AgentSession,
        result: Dict[str, Any],
    ) -> AsyncGenerator[str, None]:
        text_acc = ""
        thinking_acc = ""
        tool_calls_acc: Dict[int, Dict[str, Any]] = {}
        finish_reason = None

        kwargs: Dict[str, Any] = {
            "model": model_id,
            "messages": messages,
            "api_key": ai_config.api_key,
            "stream": True,
            "request_timeout": settings.llm_request_timeout,
        }
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"
        if _needs_thinking_param(ai_config.provider, ai_config.model):
            kwargs["thinking"] = {"type": "enabled", "budget_tokens": 8000}

        response = await litellm.acompletion(**kwargs)

        async for chunk in response:
            choice = chunk.choices[0]
            if choice.finish_reason:
                finish_reason = choice.finish_reason
            delta = choice.delta

            thinking_chunk = _extract_thinking(delta)
            if thinking_chunk:
                thinking_acc += thinking_chunk
                yield thinking_chunk

            if delta.content:
                text_acc += delta.content

            if hasattr(delta, "tool_calls") and delta.tool_calls:
                for tc in delta.tool_calls:
                    idx = tc.index
                    if idx not in tool_calls_acc:
                        tool_calls_acc[idx] = {"id": tc.id or "", "name": "", "arguments": ""}
                    if tc.id:
                        tool_calls_acc[idx]["id"] = tc.id
                    if tc.function and tc.function.name:
                        tool_calls_acc[idx]["name"] += tc.function.name
                    if tc.function and tc.function.arguments:
                        tool_calls_acc[idx]["arguments"] += tc.function.arguments

        logger.info(
            "LLM agent turn concluído",
            extra={
                "tenant_id": session.tenant_id,
                "model": model_id,
                "finish_reason": finish_reason,
                "tool_calls": len(tool_calls_acc),
                "thinking_tokens": len(thinking_acc),
            },
        )
        result["text_acc"] = text_acc
        result["thinking_acc"] = thinking_acc
        result["tool_calls_acc"] = tool_calls_acc
        result["finish_reason"] = finish_reason


_PROVIDER_ALIASES = {
    "google": "gemini",
}


def _build_model_id(provider: str, model: str) -> str:
    if "/" in model:
        return model
    normalized = _PROVIDER_ALIASES.get(provider, provider)
    return f"{normalized}/{model}"


def _tool_to_openai(tool: Any) -> Dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description or "",
            "parameters": tool.inputSchema if isinstance(tool.inputSchema, dict) else {},
        },
    }


def _parse_mcp_result(result: Any) -> Any:
    # isError=True indica falha na execução da tool — retornamos com chave "error"
    # para que o LLM interprete corretamente como falha, não como resultado.
    if getattr(result, "isError", False):
        content = getattr(result, "content", None)
        error_text = ""
        if content:
            item = content[0]
            error_text = getattr(item, "text", str(item))
        return {"error": error_text or "tool_call_failed"}

    content = getattr(result, "content", None)
    if not content:
        return {}
    if len(content) == 1:
        item = content[0]
        text = getattr(item, "text", None)
        if text:
            try:
                return json.loads(text)
            except (json.JSONDecodeError, ValueError):
                return {"text": text}
    return [{"type": getattr(c, "type", "text"), "text": getattr(c, "text", str(c))} for c in content]


def _build_proposals(tool_calls_acc: Dict[int, Dict[str, Any]]) -> List[ToolProposal]:
    proposals = []
    for tc in tool_calls_acc.values():
        try:
            args = json.loads(tc["arguments"]) if tc["arguments"] else {}
        except json.JSONDecodeError:
            args = {}
        tool_name = tc["name"]
        proposals.append(
            ToolProposal(
                proposal_id=tc["id"] or str(uuid.uuid4()),
                tool_name=tool_name,
                description=_tool_description(tool_name),
                args=args,
                is_write=tool_name in _WRITE_TOOLS,
            )
        )
    return proposals


_TOOL_DESC: Dict[str, str] = {
    # Titlis read tools
    "list_all_workloads": "Listar todos os workloads do tenant",
    "get_deployment_spec": "Buscar dados do Deployment",
    "get_current_scorecard": "Buscar scorecard atual do workload",
    "get_hpa_config": "Verificar configuração de HPA",
    "get_similar_resolved": "Buscar workloads que resolveram esta regra",
    "get_namespace_inventory": "Listar Deployments do namespace",
    "get_remediation_history": "Buscar histórico de remediações",
    # Titlis SLO tools
    "get_slo_status": "Verificar status de SLO",
    "list_auto_created_slos": "Listar SLOs criados automaticamente",
    "update_slo_thresholds": "Atualizar thresholds de SLO",
    # GitHub MCP tools
    "create_pull_request": "Criar Pull Request no GitHub",
    "push_files": "Enviar arquivos para repositório GitHub",
    "create_or_update_file": "Criar ou atualizar arquivo no GitHub",
    "create_branch": "Criar branch no GitHub",
    "merge_pull_request": "Fazer merge de Pull Request",
    "create_repository": "Criar repositório no GitHub",
    "delete_file": "Deletar arquivo no GitHub",
    "update_pull_request": "Atualizar Pull Request no GitHub",
    "create_issue": "Criar issue no GitHub",
    "update_issue": "Atualizar issue no GitHub",
    "add_issue_comment": "Adicionar comentário em issue do GitHub",
    "create_pull_request_review": "Criar revisão de Pull Request",
    "get_file_contents": "Ler conteúdo de arquivo no GitHub",
    "list_pull_requests": "Listar Pull Requests no GitHub",
    "get_pull_request": "Buscar Pull Request no GitHub",
    "search_repositories": "Buscar repositórios no GitHub",
    "search_code": "Buscar código no GitHub",
    "list_commits": "Listar commits no GitHub",
}


def _tool_description(tool_name: str) -> str:
    return _TOOL_DESC.get(tool_name, tool_name)


def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


def _needs_thinking_param(provider: str, model: str) -> bool:
    if provider == "anthropic":
        return True
    if provider in ("google", "gemini"):
        return "gemini-2.5" in model or "gemini-3" in model
    return False


def _extract_thinking(delta: Any) -> str:
    thinking_blocks = getattr(delta, "thinking_blocks", None)
    if thinking_blocks:
        parts = []
        for block in thinking_blocks:
            if isinstance(block, dict):
                text = block.get("thinking", "")
            else:
                text = getattr(block, "thinking", "") or ""
            if text:
                parts.append(text)
        if parts:
            return "".join(parts)

    reasoning = getattr(delta, "reasoning_content", None)
    if reasoning:
        return reasoning

    return ""
