import json
import uuid
from typing import Any, AsyncGenerator, Dict, List

import litellm

from src.domain.models import AgentToolDecision, TenantAiConfig, ToolProposal
from src.infrastructure.titlis_api.scorecard_client import ScorecardClient
from src.pipeline.session import AgentSession, SessionStore
from src.services.mcp_adapter import McpAdapter
from src.tools.campaign_tools import build_campaign_tools
from src.tools.github_tools import build_github_tools
from src.tools.read_tools import build_read_tools
from src.tools.slo_tools import build_slo_tools
from src.utils.logger import get_logger

logger = get_logger(__name__)

_WRITE_TOOLS = {"create_remediation_pr", "update_slo_thresholds", "trigger_bulk_pr_campaign"}

_SYSTEM_PROMPT = """Você é ARIA (Assistente de Remediação Inteligente Autônoma), especialista em operações SRE Kubernetes na plataforma Titlis.

Domínio exclusivo de atuação:
- Análise de findings de scorecard de compliance Kubernetes
- Diagnóstico de problemas de workloads (resource limits, liveness/readiness probes, HPA, anotações)
- Remediação de Deployments com abertura de Pull Requests no GitHub
- Consulta e ajuste de SLOs
- Leitura de histórico de remediações anteriores

Se o usuário perguntar sobre algo FORA deste domínio, responda EXATAMENTE com:
FORA_DO_ESCOPO: <explicação objetiva do que está fora do escopo e o que você pode ajudar>

Ao analisar problemas:
1. Use as ferramentas disponíveis para buscar dados reais ANTES de pedir informações ao usuário
2. Quando o namespace não for informado, use list_all_workloads para descobrir os workloads disponíveis
3. Nunca pergunte por namespace, workload_id ou outros dados que você pode buscar com as ferramentas
4. Seja objetivo e técnico nas respostas

Idioma: português brasileiro."""


class AgentService:
    def __init__(self, scorecard_client: ScorecardClient, session_store: SessionStore) -> None:
        self._scorecard = scorecard_client
        self._store = session_store

    def _build_adapter(self, session: AgentSession) -> McpAdapter:
        ai_cfg = session.ai_config
        tenant_id = session.tenant_id
        registries = [build_read_tools(self._scorecard, tenant_id)]
        registries.append(build_slo_tools(self._scorecard, tenant_id))
        github_token = ai_cfg.get("github_token")
        if github_token:
            base_branch = ai_cfg.get("github_base_branch", "main")
            registries.append(build_github_tools(github_token, base_branch, tenant_id, self._scorecard))
            actor_email = ai_cfg.get("actor_email")
            registries.append(build_campaign_tools(tenant_id=tenant_id, actor_email=actor_email))
        return McpAdapter(*registries)

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

    async def run_turn(
        self, session: AgentSession, user_message: str
    ) -> AsyncGenerator[str, None]:
        session.messages.append({"role": "user", "content": user_message})
        session.append_audit({"event": "user_message", "content": user_message})

        adapter = self._build_adapter(session)
        ai_config = self._ai_config(session)
        messages = [{"role": "system", "content": _SYSTEM_PROMPT}] + session.messages
        tools = adapter.to_openai_tools()
        model_id = _build_model_id(ai_config.provider, ai_config.model)

        async for event in self._llm_loop(session, messages, tools, model_id, ai_config, adapter):
            yield event

    async def run_tool_responses(
        self, session: AgentSession, decisions: List[AgentToolDecision]
    ) -> AsyncGenerator[str, None]:
        adapter = self._build_adapter(session)
        ai_config = self._ai_config(session)
        model_id = _build_model_id(ai_config.provider, ai_config.model)

        proposal_map = {p.proposal_id: p for p in session.pending_proposals}
        session.pending_proposals = []

        for decision in decisions:
            proposal = proposal_map.get(decision.proposal_id)
            if proposal is None:
                continue

            session.append_audit({
                "event": "tool_decision",
                "tool": proposal.tool_name,
                "approved": decision.approved,
                "proposal_id": decision.proposal_id,
            })

            if decision.approved:
                args = decision.edited_args if decision.edited_args is not None else proposal.args
                try:
                    result = await adapter.execute(proposal.tool_name, args)
                    result_str = json.dumps(result, ensure_ascii=False, default=str)
                    session.append_audit({"event": "tool_result", "tool": proposal.tool_name, "result": result_str[:500]})
                    yield _sse({"type": "tool_result", "proposal_id": decision.proposal_id, "tool_name": proposal.tool_name, "approved": True, "result": result})
                    session.messages.append({"role": "tool", "tool_call_id": decision.proposal_id, "content": result_str})
                except Exception as exc:
                    err = str(exc)
                    session.append_audit({"event": "tool_error", "tool": proposal.tool_name, "error": err})
                    yield _sse({"type": "tool_result", "proposal_id": decision.proposal_id, "tool_name": proposal.tool_name, "approved": True, "error": err})
                    session.messages.append({"role": "tool", "tool_call_id": decision.proposal_id, "content": f"Erro: {err}"})
            else:
                yield _sse({"type": "tool_result", "proposal_id": decision.proposal_id, "tool_name": proposal.tool_name, "approved": False})
                session.messages.append({"role": "tool", "tool_call_id": decision.proposal_id, "content": "Usuário rejeitou a execução desta ferramenta."})

        messages = [{"role": "system", "content": _SYSTEM_PROMPT}] + session.messages
        tools = adapter.to_openai_tools()

        async for event in self._llm_loop(session, messages, tools, model_id, ai_config, adapter):
            yield event

    async def _llm_loop(
        self,
        session: AgentSession,
        messages: List[Dict[str, Any]],
        tools: List[Dict[str, Any]],
        model_id: str,
        ai_config: TenantAiConfig,
        adapter: McpAdapter,
    ) -> AsyncGenerator[str, None]:
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
                    reason = text_acc[len("FORA_DO_ESCOPO:"):].strip()
                    session.append_audit({"event": "scope_rejected", "reason": reason})
                    yield _sse({"type": "scope_rejected", "reason": reason})
                    yield _sse({"type": "done"})
                    return

            if finish_reason == "tool_calls" and tool_calls_acc:
                proposals = _build_proposals(tool_calls_acc)

                read_proposals = [p for p in proposals if not p.is_write]
                write_proposals = [p for p in proposals if p.is_write]

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
                        tool_result = await adapter.execute(proposal.tool_name, proposal.args)
                        result_str = json.dumps(tool_result, ensure_ascii=False, default=str)
                        session.append_audit({"event": "tool_result", "tool": proposal.tool_name, "result": result_str[:500]})
                        session.messages.append({"role": "tool", "tool_call_id": proposal.proposal_id, "content": result_str})
                    except Exception as exc:
                        err = str(exc)
                        session.append_audit({"event": "tool_error", "tool": proposal.tool_name, "error": err})
                        session.messages.append({"role": "tool", "tool_call_id": proposal.proposal_id, "content": f"Erro: {err}"})

                if write_proposals:
                    session.pending_proposals = write_proposals
                    yield _sse({
                        "type": "awaiting_approvals",
                        "proposals": [p.model_dump() for p in write_proposals],
                    })
                    yield _sse({"type": "done"})
                    return

                messages = [{"role": "system", "content": _SYSTEM_PROMPT}] + session.messages
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
        tool_calls_acc: Dict[int, Dict[str, Any]] = {}
        finish_reason = None

        kwargs: Dict[str, Any] = {
            "model": model_id,
            "messages": messages,
            "api_key": ai_config.api_key,
            "stream": True,
            "request_timeout": 120,
        }
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"

        response = await litellm.acompletion(**kwargs)

        async for chunk in response:
            choice = chunk.choices[0]
            if choice.finish_reason:
                finish_reason = choice.finish_reason
            delta = choice.delta

            if delta.content:
                text_acc += delta.content
                yield delta.content

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
            },
        )
        result["text_acc"] = text_acc
        result["tool_calls_acc"] = tool_calls_acc
        result["finish_reason"] = finish_reason


def _build_model_id(provider: str, model: str) -> str:
    # litellm model IDs use "<provider>/<model>" format.
    # If model already contains a "/" (e.g. "gemini/gemini-2.0-flash"), use it as-is
    # to avoid double-prefixing (e.g. "google/gemini/gemini-2.0-flash" is invalid).
    if "/" in model:
        return model
    return f"{provider}/{model}"


def _build_proposals(tool_calls_acc: Dict[int, Dict[str, Any]]) -> List[ToolProposal]:
    proposals = []
    for tc in tool_calls_acc.values():
        try:
            args = json.loads(tc["arguments"]) if tc["arguments"] else {}
        except json.JSONDecodeError:
            args = {}
        tool_name = tc["name"]
        proposals.append(ToolProposal(
            proposal_id=tc["id"] or str(uuid.uuid4()),
            tool_name=tool_name,
            description=_tool_description(tool_name),
            args=args,
            is_write=tool_name in _WRITE_TOOLS,
        ))
    return proposals


_TOOL_DESC: Dict[str, str] = {
    "list_all_workloads": "Listar todos os workloads do tenant",
    "get_deployment_spec": "Buscar dados do Deployment",
    "get_current_scorecard": "Buscar scorecard atual do workload",
    "get_hpa_config": "Verificar configuração de HPA",
    "get_similar_resolved": "Buscar workloads que resolveram esta regra",
    "get_namespace_inventory": "Listar Deployments do namespace",
    "get_remediation_history": "Buscar histórico de remediações",
    "read_deploy_manifest": "Ler manifesto YAML do repositório",
    "check_existing_pr": "Verificar PR de remediação existente",
    "create_remediation_pr": "Criar Pull Request de remediação no GitHub",
    "get_slo_status": "Verificar status de SLO",
    "list_auto_created_slos": "Listar SLOs criados automaticamente",
    "update_slo_thresholds": "Atualizar thresholds de SLO",
    "trigger_bulk_pr_campaign": "Disparar campanha de PRs em lote para múltiplos workloads",
}


def _tool_description(tool_name: str) -> str:
    return _TOOL_DESC.get(tool_name, tool_name)


def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
