# CLAUDE.md — titlis-ai

> Após toda alteração: `make lint && make test` devem passar.
> Nunca chame APIs externas (OpenAI, GitHub, Datadog) em testes — use mocks.

---

## 1. Contexto e Responsabilidade

`titlis-ai` é o serviço de IA do Titlis. Ele **não acessa o cluster Kubernetes** e não
tem `kubeconfig` — o operator é o único ator K8s. O titlis-ai é agnóstico de provider:
cada tenant configura sua própria API key e modelo via `TenantAiConfig`.

**Responsabilidades que migraram do `titlis-operator`:**
- Ler `deploy.yaml` do GitHub
- Gerar patch de resources/HPA
- Abrir PR no GitHub (com validação never-reduce)
- Controle humano antes do PR (human-in-the-loop)

O operator após a migração: avalia → escreve CRD → envia UDP. Sem GitHub.

---

## 2. Stack

| Categoria | Tecnologia | Versão |
|---|---|---|
| Linguagem | Python | 3.12 |
| Framework HTTP | FastAPI | — |
| LLM abstraction | LiteLLM | — |
| Grafo de agente | LangGraph | 0.3.5 |
| Embeddings | pgvector (via titlis-api) | — |
| HTTP client | httpx (async) | — |
| Validação | Pydantic v2 | — |
| Testes | pytest + pytest-asyncio | — |
| Lint | flake8 + black + mypy | — |

---

## 3. Estrutura de Diretórios

```
titlis-ai/
├── src/
│   ├── main.py                          # FastAPI app + lifespan
│   ├── settings.py                      # Pydantic Settings (env vars)
│   ├── domain/
│   │   └── models.py                    # ExplainRequest, RemediateRequest, ConfirmRemediationRequest, TenantAiConfig
│   ├── routes/
│   │   ├── agent.py                     # POST /v1/agent/chat + tools/respond + GET audit (SSE)
│   │   ├── explain.py                   # POST /v1/explain (SSE streaming)
│   │   ├── remediate.py                 # POST /v1/remediate + POST /v1/remediate/{thread_id}/confirm
│   │   ├── knowledge.py                 # POST /v1/knowledge/seed + POST /v1/knowledge/search
│   │   └── health.py                    # GET /health
│   ├── services/
│   │   ├── agent_service.py             # AgentService — loop LLM + human-in-the-loop tool approval
│   │   ├── llm_service.py               # LiteLLM wrapper (chat, stream)
│   │   ├── prompt_builder.py            # Monta prompts por rule_id
│   │   ├── embedding_service.py         # Gera embeddings via LiteLLM
│   │   ├── knowledge_seeder.py          # Semeia base de conhecimento
│   │   └── mcp_adapter.py               # Converte ToolRegistry → function calling
│   ├── tools/
│   │   ├── base.py                      # ToolDefinition + ToolRegistry
│   │   ├── read_tools.py                # 6 tools de leitura (scorecard, RAG, history)
│   │   ├── github_tools.py              # 3 tools GitHub (read/check/create PR)
│   │   └── slo_tools.py                 # 3 tools SLO (get/list/update)
│   ├── pipeline/
│   │   ├── session.py                   # AgentSession dataclass + SessionStore (TTL 3600s)
│   │   ├── state.py                     # ScorecardRemediationState TypedDict
│   │   └── graph.py                     # RemediationGraph (LangGraph StateGraph)
│   ├── infrastructure/
│   │   ├── github/
│   │   │   ├── client.py                # httpx async — GitHub REST API
│   │   │   └── repository.py            # branch, commit, PR, find_existing_pr
│   │   ├── titlis_api/
│   │   │   ├── scorecard_client.py      # 7 métodos — endpoints /v1/internal/ai/*
│   │   │   └── knowledge_client.py      # store/search chunks RAG
│   │   └── udp_client.py                # UdpEventClient — envia remediation_started
│   └── bootstrap/
│       └── dependencies.py              # Singletons: llm, scorecard, knowledge, embedding, udp, graph, session_store, agent_service
├── tests/
│   ├── unit/
│   │   ├── test_llm_service.py
│   │   ├── test_pipeline_nodes.py       # Testa cada nó do grafo isoladamente
│   │   └── test_remediate_route.py      # Testa os endpoints SSE
│   └── integration/                     # Marcados com @pytest.mark.integration
├── pyproject.toml
└── Makefile
```

---

## 4. Auth Interna

Todos os endpoints (`/v1/agent/*`, `/v1/remediate`, `/v1/explain`, `/v1/knowledge/*`) exigem o header:
```
X-Internal-Secret: <settings.internal_secret>
```
Sem ele → HTTP 403. Definido em `settings.py` via env var `TITLIS_AI_INTERNAL_SECRET`.

O titlis-api é o proxy que autentica o JWT do usuário e repassa `X-Internal-Secret`
para o titlis-ai — o titlis-ai não valida JWTs diretamente.

---

## 5. Fase 1 — LLM Service e Explain Route

### LLMService (`src/services/llm_service.py`)

Wrapper sobre `litellm.completion()` e `litellm.acompletion()`:
- `chat(config, messages)` → resposta completa
- `stream(config, messages)` → gerador assíncrono de chunks SSE

`TenantAiConfig` (em `domain/models.py`) porta: `provider`, `model`, `api_key`,
`github_token`, `github_base_branch`, `monthly_token_budget`, `tokens_used_month`.

Trocar de provider é só mudar `model="anthropic/claude-3-5-sonnet"` — zero mudança de código.

### PromptBuilder (`src/services/prompt_builder.py`)

Templates por `rule_id` com system prompt universal SRE:
> *"Você é um especialista em SRE e Kubernetes. Sua única função é ajudar a resolver
> issues de compliance identificadas pelo scorecard do Titlis."*

### Endpoint de explicação

`POST /v1/explain` — body: `ExplainRequest` — resposta: SSE stream com chunks markdown.

---

## 6. Fase 2 — RAG e Base de Conhecimento

### EmbeddingService (`src/services/embedding_service.py`)

Gera embeddings via LiteLLM. Agnóstico de modelo:
- OpenAI → `text-embedding-3-small`
- Cohere → `embed-english-v3`

### KnowledgeSeeder (`src/services/knowledge_seeder.py`)

Semeia a base global de conhecimento (documentação das 26 regras, best practices K8s).
Chamado via `POST /v1/knowledge/seed`.

### KnowledgeClient (`src/infrastructure/titlis_api/knowledge_client.py`)

HTTP client para os endpoints internos de RAG no titlis-api:
- `store_chunk(tenant_id, source_type, source_id, chunk_text, embedding, metadata)`
- `search_chunks(tenant_id, embedding, limit, source_types)`

Os vetores ficam no pgvector do PostgreSQL gerenciado pelo titlis-api.

### Fontes do knowledge base

| source_type | tenant_id | Origem |
|---|---|---|
| `rule_doc` | NULL (global) | Documentação das 26 regras |
| `k8s_best_practice` | NULL (global) | Snippets K8s: probes, resources, HPA, etc. |
| `past_remediation` | `<id>` (por tenant) | PRs mergeados pelo tenant |

---

## 7. Fase 3 — MCP Tools

### ToolDefinition e ToolRegistry (`src/tools/base.py`)

```python
@dataclass
class ToolDefinition:
    name: str
    description: str
    parameters: Dict[str, Any]   # JSON Schema
    handler: Callable

class ToolRegistry:
    def register(self, tool: ToolDefinition) -> None: ...
    def get(self, name: str) -> Optional[ToolDefinition]: ...
    def all(self) -> List[ToolDefinition]: ...
```

### ScorecardClient (`src/infrastructure/titlis_api/scorecard_client.py`)

7 métodos assíncronos que chamam `/v1/internal/ai/*` no titlis-api:

| Método | Endpoint chamado |
|---|---|
| `get_scorecard_by_uid(tenant_id, k8s_uid)` | `GET /v1/internal/ai/scorecards/{uid}` |
| `get_scorecard_by_name(tenant_id, name, ns)` | `GET /v1/internal/ai/scorecards/by-name` |
| `get_dashboard(tenant_id, cluster?)` | `GET /v1/internal/ai/dashboard` |
| `get_similar_resolved(tenant_id, rule_id)` | `GET /v1/internal/ai/scorecards/similar-resolved` |
| `get_slos(tenant_id, namespace?)` | `GET /v1/internal/ai/slos` |
| `get_remediation_history(tenant_id, k8s_uid)` | `GET /v1/internal/ai/remediations/{uid}/history` |
| `propose_slo_change(tenant_id, slo_config_id, ...)` | `POST /v1/internal/ai/slo-configs/{id}/propose-change` |

### Tools de leitura (`src/tools/read_tools.py`)

`build_read_tools(scorecard_client, tenant_id)` → `ToolRegistry` com 6 tools:
- `get_deployment_spec(namespace, name)` — scorecard por nome+namespace
- `get_current_scorecard(workload_id)` — scorecard por k8s_uid
- `get_hpa_config(namespace, name)` — retorna `{hpa: null}` (HPA não armazenado em DB ainda)
- `get_similar_resolved(rule_id, pillar)` — workloads do tenant que resolveram a regra
- `get_namespace_inventory(namespace)` — lista deployments do namespace
- `get_remediation_history(workload_id)` — PRs anteriores do workload

### Tools GitHub (`src/tools/github_tools.py`)

`build_github_tools(github_token, base_branch, tenant_id)` → `ToolRegistry` com 3 tools:
- `read_deploy_manifest(repo_url, branch, path)` — lê arquivo via GitHub API
- `check_existing_pr(repo_url, namespace, deployment)` — verifica PR aberto (idempotência)
- `create_remediation_pr(repo_url, path, patched_yaml, current_yaml, findings, ...)` — cria branch + commit + PR; valida never-reduce antes de qualquer escrita

**Validação never-reduce** (em `github_tools.py`):
```python
def _never_reduce_violated(current: str, suggested: str) -> bool:
    # Parseia cpu (millicores) e memory (MiB)
    # Retorna True se sugerido < atual (violação)
```

### Tools SLO (`src/tools/slo_tools.py`)

`build_slo_tools(scorecard_client, tenant_id)` → `ToolRegistry` com 3 tools:
- `get_slo_status(workload_id)` — SLO atual do workload
- `list_auto_created_slos(namespace?)` — SLOs criados automaticamente pelo operator
- `update_slo_thresholds(slo_config_id, target?, warning?, timeframe?)` — propõe mudança via titlis-api; valida `target > warning` antes de qualquer HTTP call

`SloValidationError(ValueError)` — exception customizada para validação de SLO.
**Atenção:** nunca capture com `except ValueError` ao redor do código que usa estas tools —
a `SloValidationError` herda de `ValueError` e pode ser silenciada acidentalmente.

### McpAdapter (`src/services/mcp_adapter.py`)

Converte `ToolRegistry` → function calling de qualquer provider via LiteLLM:
```python
class McpAdapter:
    def to_openai_tools(self) -> List[Dict[str, Any]]: ...    # type="function"
    def to_anthropic_tools(self) -> List[Dict[str, Any]]: ... # input_schema key
    async def execute(self, tool_name: str, args: Dict[str, Any]) -> Any: ...
```

---

## 8. Fase 4 — LangGraph Pipeline

### Estado (`src/pipeline/state.py`)

```python
class ScorecardRemediationState(TypedDict, total=False):
    tenant_id: int
    workload_id: str          # k8s_uid
    finding_ids: List[str]
    repo_url: str
    deploy_manifest_path: str
    ai_config: Dict[str, Any]
    findings: List[Dict[str, Any]]
    namespace: str
    deployment_name: str
    rag_context: List[Dict[str, Any]]
    current_manifest: Optional[str]
    live_deployment: Optional[Dict[str, Any]]
    existing_pr: Optional[Dict[str, Any]]
    analysis: Optional[str]
    patched_manifest: Optional[str]
    validation_errors: List[str]
    retry_count: int
    approved: Optional[bool]
    pr_result: Optional[Dict[str, Any]]
```

### RemediationGraph (`src/pipeline/graph.py`)

Singleton, compilado com `MemorySaver()` checkpointer. Serviços injetados no construtor:
`llm_service`, `scorecard_client`, `knowledge_client`, `embedding_service`, `udp_client`.

**9 nós:**

| Nó | O que faz |
|---|---|
| `classify_findings` | Busca scorecard por k8s_uid; filtra findings pelos `finding_ids` solicitados; seta `namespace`, `deployment_name` |
| `fetch_context` | Paralelo via `asyncio.gather`: RAG (embedding + search) + leitura do manifest do GitHub |
| `check_existing_pr` | Usa `GitHubRepository.find_open_remediation_pr()`; seta `existing_pr` |
| `analyze_findings` | LLM `chat()` com findings + manifest + contexto RAG |
| `generate_yaml_patch` | LLM gera YAML patchado; strip de markdown fences; incrementa `retry_count`; limpa `validation_errors` |
| `validate_patch` | `yaml.safe_load()` + verificação never-reduce linha a linha |
| `await_user_confirmation` | `interrupt({patched_manifest, current_manifest, findings, ...})`; pausa o grafo |
| `create_remediation_pr` | Cria branch → commit → PR via `GitHubRepository` |
| `notify_api` | Envia UDP `remediation_started` para titlis-api:8125 |

**Topologia:**
```
START → classify_findings → fetch_context → check_existing_pr
check_existing_pr --condicional--> analyze_findings | END (PR já existe)
analyze_findings → generate_yaml_patch → validate_patch
validate_patch --condicional--> generate_yaml_patch (retry, max 3) | END (esgotou) | await_user_confirmation
await_user_confirmation --condicional--> create_remediation_pr | END (rejeitado)
create_remediation_pr → notify_api → END
```

**3 métodos de roteamento (estáticos):**
- `_route_after_check_pr(state)` → `END` se `existing_pr`, senão `"analyze_findings"`
- `_route_after_validate(state)` → `"generate_yaml_patch"` se erros e retry < 3; `END` se retry ≥ 3; `"await_user_confirmation"` se sem erros
- `_route_after_confirmation(state)` → `"create_remediation_pr"` se `approved`, senão `END`

### UdpEventClient (`src/infrastructure/udp_client.py`)

Envelope UDP padrão: `{v:1, t:event_type, ts:..., tenant_id:..., data:...}`.
Envia via `socket.SOCK_DGRAM` usando `run_in_executor` (não bloqueia event loop).
Host padrão: `titlis-api`, porta: 8125.

### Endpoints SSE (`src/routes/remediate.py`)

**`POST /v1/remediate`** — inicia o pipeline:
- Gera `thread_id` (UUID)
- Monta `initial_state` com dados do body + `retry_count=0`
- Roda `graph.compiled.astream(initial_state, config, stream_mode="updates")`
- Detecta `__interrupt__` → emite evento SSE `fix_ready` com `thread_id`
- Ao final → emite `done`

**`POST /v1/remediate/{thread_id}/confirm`** — retoma grafo pausado:
- Roda `graph.compiled.astream(Command(resume=body.approved), config, stream_mode="updates")`
- Detecta nó `create_remediation_pr` → emite `pr_created`
- Ao final → emite `done`

**Tipos de eventos SSE:**

| Tipo | Quando |
|---|---|
| `fix_ready` | Grafo pausa para confirmação humana (inclui `thread_id`, `patched_manifest`, `current_manifest`, `findings`) |
| `existing_pr` | PR já existe — retorna `pr_url` e encerra |
| `progress` | A cada nó concluído (inclui `node` name) |
| `pr_created` | PR criado com sucesso (inclui `pr_url`, `pr_number`, `branch`) |
| `error` | Exceção não tratada no pipeline |
| `done` | Sempre o último evento do stream |

---

## 9. Dependências Singleton (`src/bootstrap/dependencies.py`)

```python
@lru_cache() get_llm_service() → LLMService
@lru_cache() get_prompt_builder() → PromptBuilder
@lru_cache() get_embedding_service() → EmbeddingService
@lru_cache() get_knowledge_client() → KnowledgeClient
@lru_cache() get_knowledge_seeder() → KnowledgeSeeder
@lru_cache() get_scorecard_client() → ScorecardClient
@lru_cache() get_udp_client() → UdpEventClient
@lru_cache() get_remediation_graph() → RemediationGraph
@lru_cache() get_session_store() → SessionStore
@lru_cache() get_agent_service() → AgentService
```

O `RemediationGraph` é singleton — serviços são injetados na construção. Dados
por-request (github_token, tenant_id) vêm do estado do grafo (`state["ai_config"]`),
não do construtor.

O `AgentService` usa o `SessionStore` para persistir histórico de conversa entre turns.
Sessões expiram após 3600s de inatividade (TTL não renova a cada acesso).

---

## 10. Variáveis de Ambiente

```bash
# Auth interna
TITLIS_AI_INTERNAL_SECRET=titlis-ai-internal-secret

# titlis-api (para ScorecardClient e KnowledgeClient)
TITLIS_API_BASE_URL=http://titlis-api:8080
TITLIS_API_INTERNAL_SECRET=titlis-internal-secret

# UDP (para UdpEventClient)
TITLIS_API_UDP_HOST=titlis-api
TITLIS_API_UDP_PORT=8125

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

---

## 11. Comandos

```bash
make dev-install     # poetry install --with dev
make test            # pytest tests/ -v --tb=short --cov=src
make test-unit       # pytest tests/unit/ -v
make lint            # flake8 + black --check + mypy --ignore-missing-imports
make format          # black src/ tests/
make run             # uvicorn src.main:app --reload --port 8001
```

---

## 12. Fase 5 — Agente Conversacional (Human-in-the-Loop)

### AgentSession e SessionStore (`src/pipeline/session.py`)

```python
@dataclass
class AgentSession:
    session_id: str
    tenant_id: int
    ai_config: TenantAiConfig
    messages: List[Dict]        # histórico OpenAI-format
    pending_proposals: List[ToolProposal]
    audit_log: List[Dict]
    created_at: float
    last_active: float

class SessionStore:
    get_or_create(session_id, tenant_id, ai_config) → AgentSession
    get(session_id) → Optional[AgentSession]
    cleanup_expired()  # TTL 3600s
```

### AgentService (`src/services/agent_service.py`)

- `run_turn(session, user_message)` → AsyncGenerator de eventos SSE
- `run_tool_responses(session, decisions)` → AsyncGenerator de eventos SSE
- `_llm_loop()` — até 5 iterações; detecta `finish_reason == "tool_calls"` e pausa
- `_stream_llm()` — acumula deltas de tool calls por `tc.index` (LiteLLM streaming)
- `_WRITE_TOOLS` — set de nomes de tools que modificam estado (badge âmbar na UI)
- `_TOOL_DESC` — dict `tool_name → descrição PT-BR` exibida no card de aprovação
- System prompt: persona ARIA, escopo K8s/SRE, prefixo `FORA_DO_ESCOPO:` para rejeição

### Endpoints (`src/routes/agent.py`)

| Método | Path | Descrição |
|---|---|---|
| `POST` | `/v1/agent/chat` | Inicia ou continua turno; body: `AgentChatRequest`; SSE |
| `POST` | `/v1/agent/{session_id}/tools/respond` | Retoma com decisões; body: `AgentToolsRespondRequest`; SSE |
| `GET` | `/v1/agent/{session_id}/audit` | Retorna audit log JSON da sessão |

### Eventos SSE do agente

| Tipo | Quando |
|---|---|
| `thinking` | Chunk de texto de raciocínio do LLM (streaming) |
| `awaiting_approvals` | LLM propôs tools; inclui lista de `ToolProposal` |
| `tool_result` | Resultado de uma tool executada (após decisão do usuário) |
| `message` | Resposta final em texto do LLM |
| `scope_rejected` | Resposta começou com `FORA_DO_ESCOPO:` |
| `done` | Sempre o último evento |
| `error` | Exceção não tratada |

### Modelos (`src/domain/models.py`)

```python
class ToolProposal(BaseModel):
    proposal_id: str    # UUID gerado pelo AgentService
    tool_name: str
    description: str    # vem de _TOOL_DESC
    args: Dict[str, Any]
    is_write: bool      # True se tool_name in _WRITE_TOOLS

class AgentChatRequest(BaseModel):
    tenant_id: int
    session_id: str
    message: str
    ai_config: TenantAiConfig

class AgentToolDecision(BaseModel):
    proposal_id: str
    approved: bool
    edited_args: Optional[Dict[str, Any]] = None

class AgentToolsRespondRequest(BaseModel):
    decisions: List[AgentToolDecision]
```

---

## 13. O Que Não Fazer

- **Nunca** chame APIs externas (OpenAI, GitHub) em testes — use `AsyncMock`
- **Nunca** instancie `RemediationGraph` fora de `bootstrap/dependencies.py` — quebra o DI
- **Nunca** instancie `AgentService` ou `SessionStore` fora de `bootstrap/dependencies.py`
- **Nunca** reduza CPU/memory em PRs — a validação never-reduce é obrigatória antes de `create_remediation_pr`
- **Nunca** acesse o cluster Kubernetes — o titlis-ai não tem kubeconfig
- **Nunca** emita `pr_created` sem ter confirmação do usuário (`await_user_confirmation`)
- **Nunca** capture `except ValueError` ao redor de código SLO — `SloValidationError` herda de `ValueError`
- **Nunca** passe `github_token` no estado global do grafo — ele vem do `ai_config` da request
- **Nunca** execute uma write tool sem aprovação explícita do usuário via `AgentToolDecision.approved`
- **Nunca** adicione docstrings — código deve ser autoexplicativo
