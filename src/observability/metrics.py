from prometheus_client import Counter, Histogram

ai_requests_total = Counter(
    "titlis_ai_requests_total",
    "Total de requests ao assistente de IA",
    ["tenant_id", "provider", "model", "rule_id", "status"],
)

ai_latency_seconds = Histogram(
    "titlis_ai_latency_seconds",
    "Latência das requests de IA por fase",
    ["tenant_id", "provider", "model", "phase"],
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

ai_tokens_total = Counter(
    "titlis_ai_tokens_total",
    "Total de tokens consumidos por tipo",
    ["tenant_id", "provider", "model", "token_type"],
)

ai_cost_usd_total = Counter(
    "titlis_ai_cost_usd_total",
    "Custo estimado em USD por request",
    ["tenant_id", "provider", "model"],
)

ai_validation_errors_total = Counter(
    "titlis_ai_validation_errors_total",
    "Erros de validação de patch por tipo",
    ["rule_id", "error_type"],
)

ai_user_feedback_total = Counter(
    "titlis_ai_user_feedback_total",
    "Feedback dos usuários por sentimento",
    ["rule_id", "provider", "sentiment"],
)

ai_rag_retrieval_score = Histogram(
    "titlis_ai_rag_retrieval_score",
    "Score de similaridade do RAG retrieval",
    ["rule_id"],
    buckets=(0.1, 0.2, 0.3, 0.5, 0.7, 0.85, 0.95, 1.0),
)

ai_graph_node_errors_total = Counter(
    "titlis_ai_graph_node_errors_total",
    "Erros em nós do grafo LangGraph",
    ["node_name", "error_type"],
)

ai_pr_created_total = Counter(
    "titlis_ai_pr_created_total",
    "PRs de remediação criados",
    ["tenant_id", "pillar"],
)

ai_pr_user_rejected_total = Counter(
    "titlis_ai_pr_user_rejected_total",
    "PRs rejeitados pelo usuário",
    ["tenant_id", "rule_id"],
)

ai_feedback_alerts_total = Counter(
    "titlis_ai_feedback_alerts_total",
    "Alertas disparados por alta taxa de feedback negativo",
    ["rule_id"],
)

mcp_init_failed_total = Counter(
    "titlis_ai_mcp_init_failed_total",
    "Falhas de inicialização de sessão MCP após todos os retries",
    ["provider"],
)
