from typing import Dict, List, Optional

from src.domain.models import FindingContext

SYSTEM_PROMPT = (
    "Você é um especialista em SRE e Kubernetes. "
    "Sua única função é ajudar a resolver issues de compliance identificadas pelo scorecard do Titlis. "
    "Responda APENAS sobre o issue fornecido. "
    "Não responda perguntas fora deste escopo. "
    "Seja objetivo e prático. Inclua exemplos de YAML quando relevante."
)

_RULE_CONTEXT: Dict[str, Dict[str, str]] = {
    "RES-001": {
        "title": "Liveness Probe não configurada",
        "pillar": "Resiliência",
        "why": "Sem livenessProbe, o Kubernetes não consegue detectar containers travados e não os reinicia.",
        "fix_hint": "Adicione livenessProbe ao container com httpGet, exec ou tcpSocket.",
    },
    "RES-002": {
        "title": "Readiness Probe não configurada",
        "pillar": "Resiliência",
        "why": "Sem readinessProbe, o Kubernetes envia tráfego para pods não prontos, causando erros 502/503.",
        "fix_hint": "Adicione readinessProbe que valide que a aplicação está pronta para receber requisições.",
    },
    "RES-003": {
        "title": "CPU Requests não definido",
        "pillar": "Resiliência",
        "why": "Sem cpu requests, o scheduler não consegue alocar o pod no nó adequado e o QoS fica como BestEffort.",
        "fix_hint": "Defina resources.requests.cpu com valor mínimo razoável (ex: 100m).",
    },
    "RES-004": {
        "title": "CPU Limits não definido",
        "pillar": "Resiliência",
        "why": "Sem cpu limits, um pod pode consumir toda a CPU do nó, afetando outros workloads.",
        "fix_hint": "Defina resources.limits.cpu com valor máximo adequado (ex: 500m).",
    },
    "RES-005": {
        "title": "Memory Requests não definido",
        "pillar": "Resiliência",
        "why": "Sem memory requests, o scheduler pode alocar o pod em nós com pouca memória.",
        "fix_hint": "Defina resources.requests.memory com valor mínimo razoável (ex: 128Mi).",
    },
    "RES-006": {
        "title": "Memory Limits não definido",
        "pillar": "Resiliência",
        "why": "Sem memory limits, o pod pode consumir toda a memória disponível e causar OOM no nó.",
        "fix_hint": "Defina resources.limits.memory com valor máximo adequado (ex: 512Mi).",
    },
    "RES-007": {
        "title": "HPA não configurado",
        "pillar": "Resiliência",
        "why": "Sem HorizontalPodAutoscaler, o deployment não escala automaticamente sob carga.",
        "fix_hint": "Crie um HPA com minReplicas, maxReplicas e métricas de CPU ou memória.",
    },
    "RES-008": {
        "title": "HPA sem métricas configuradas",
        "pillar": "Resiliência",
        "why": "HPA sem métricas não sabe quando escalar, tornando o autoscaling ineficaz.",
        "fix_hint": "Adicione targetCPUUtilizationPercentage ou metrics customizadas ao HPA.",
    },
    "RES-009": {
        "title": "Graceful Shutdown não configurado",
        "pillar": "Resiliência",
        "why": "Sem terminationGracePeriodSeconds adequado, conexões ativas podem ser interrompidas abruptamente.",
        "fix_hint": "Configure spec.terminationGracePeriodSeconds (recomendado: 30s ou mais).",
    },
    "RES-010": {
        "title": "Container rodando como root",
        "pillar": "Resiliência",
        "why": "Rodar como root aumenta o raio de blast — um processo comprometido tem acesso total ao sistema.",
        "fix_hint": "Configure securityContext.runAsNonRoot: true e runAsUser com UID > 0.",
    },
    "RES-011": {
        "title": "Pod Security Context não configurado",
        "pillar": "Resiliência",
        "why": "Sem securityContext no nível do pod, todos os containers herdam permissões padrão permissivas.",
        "fix_hint": "Adicione spec.securityContext com fsGroup, runAsUser e runAsNonRoot.",
    },
    "RES-012": {
        "title": "NetworkPolicy não aplicada",
        "pillar": "Resiliência",
        "why": "Sem NetworkPolicy, o pod aceita conexões de qualquer origem no cluster.",
        "fix_hint": "Crie NetworkPolicy restringindo tráfego ingress/egress apenas às fontes necessárias.",
    },
    "RES-013": {
        "title": "Réplicas insuficientes",
        "pillar": "Resiliência",
        "why": "Com menos de 2 réplicas, qualquer falha de nó ou reinício derruba o serviço.",
        "fix_hint": "Aumente spec.replicas para pelo menos 2. Use PodAntiAffinity para distribuir em nós diferentes.",
    },
    "RES-014": {
        "title": "Estratégia de rollout não configurada",
        "pillar": "Resiliência",
        "why": "Sem estratégia de rollout, deploys podem causar downtime.",
        "fix_hint": "Configure spec.strategy.type: RollingUpdate com maxUnavailable: 0 e maxSurge: 1.",
    },
    "RES-015": {
        "title": "PodDisruptionBudget não configurado",
        "pillar": "Resiliência",
        "why": "Sem PDB, manutenções de nó podem derrubar todas as réplicas simultaneamente.",
        "fix_hint": "Crie PodDisruptionBudget com minAvailable: 1 ou maxUnavailable: 1.",
    },
    "RES-016": {
        "title": "HPA com réplicas mínimas insuficientes",
        "pillar": "Resiliência",
        "why": "minReplicas menor que 2 no HPA permite que o cluster reduza para 1 pod, criando SPOF.",
        "fix_hint": "Defina hpa.spec.minReplicas: 2 ou mais.",
    },
    "RES-017": {
        "title": "Scale-up stabilization window indevida",
        "pillar": "Resiliência",
        "why": "Window de estabilização muito longa no scale-up atrasa resposta a picos de tráfego.",
        "fix_hint": "Configure spec.behavior.scaleUp.stabilizationWindowSeconds: 0 para scale-up imediato.",
    },
    "RES-018": {
        "title": "Scale-down stabilization window muito curta",
        "pillar": "Resiliência",
        "why": "Window muito curta no scale-down causa flapping de réplicas em tráfego variável.",
        "fix_hint": "Configure spec.behavior.scaleDown.stabilizationWindowSeconds: 300 (5 minutos).",
    },
    "RES-019": {
        "title": "HPA sem behavior policies",
        "pillar": "Resiliência",
        "why": "Sem behavior policies, o HPA pode escalar de forma agressiva ou lenta demais.",
        "fix_hint": "Adicione spec.behavior com scaleUp e scaleDown policies controlando pods por período.",
    },
    "SEC-001": {
        "title": "Imagem usando tag 'latest'",
        "pillar": "Segurança",
        "why": "Tag 'latest' é mutável — pode apontar para imagens diferentes, tornando deploys não determinísticos.",
        "fix_hint": "Use tags imutáveis versionadas (ex: v1.2.3 ou SHA digest).",
    },
    "SEC-002": {
        "title": "Root filesystem não é read-only",
        "pillar": "Segurança",
        "why": "Filesystem gravável permite que um atacante modifique binários ou persista malware.",
        "fix_hint": "Configure securityContext.readOnlyRootFilesystem: true e use volumes para dados mutáveis.",
    },
    "SEC-003": {
        "title": "Privilege escalation permitido",
        "pillar": "Segurança",
        "why": "allowPrivilegeEscalation: true permite que processos obtenham permissões adicionais via setuid/setgid.",
        "fix_hint": "Configure securityContext.allowPrivilegeEscalation: false.",
    },
    "SEC-004": {
        "title": "Capabilities não removidas",
        "pillar": "Segurança",
        "why": "Linux capabilities como NET_RAW, SYS_ADMIN permitem ataques de rede e escalonamento de privilégios.",
        "fix_hint": "Adicione securityContext.capabilities.drop: [ALL] e adicione de volta apenas as necessárias.",
    },
    "PERF-001": {
        "title": "Ratio CPU limit/request excessivo",
        "pillar": "Performance",
        "why": "Ratio muito alto entre limits e requests causa CPU throttling inesperado sob carga.",
        "fix_hint": "Mantenha cpu limits entre 2-4x o valor de requests para evitar throttling.",
    },
    "PERF-002": {
        "title": "HPA CPU target muito alto",
        "pillar": "Performance",
        "why": "Target de CPU acima de 80% no HPA faz o scaling ocorrer tarde demais, causando latência elevada.",
        "fix_hint": "Configure targetCPUUtilizationPercentage entre 50-70% para scaling proativo.",
    },
    "PERF-003": {
        "title": "HPA memory target não configurado",
        "pillar": "Performance",
        "why": "Sem target de memória no HPA, picos de consumo de RAM não disparam scaling.",
        "fix_hint": "Adicione métricas de memória ao HPA com targetMemoryUtilizationPercentage: 70.",
    },
    "OPS-001": {
        "title": "Instrumentação Datadog ausente",
        "pillar": "Operacional",
        "why": "Sem labels Datadog, o serviço não aparece no APM e não pode ter SLOs configurados automaticamente.",
        "fix_hint": (
            "Adicione labels ao spec.template.metadata: "
            "tags.datadoghq.com/service, tags.datadoghq.com/env, tags.datadoghq.com/version."
        ),
    },
}

_GENERIC_CONTEXT = {
    "title": "Violação de compliance detectada",
    "pillar": "Compliance",
    "why": "Esta configuração viola as políticas de compliance do cluster.",
    "fix_hint": "Revise a configuração do Deployment conforme as políticas internas.",
}


class PromptBuilder:
    def build_explain_messages(
        self,
        finding: FindingContext,
        chunks: Optional[List[dict]] = None,
    ) -> List[dict]:
        ctx = _RULE_CONTEXT.get(finding.rule_id, _GENERIC_CONTEXT)

        actual = finding.actual_value or "não configurado"
        expected = finding.expected_value or "valor esperado não especificado"

        rag_section = ""
        if chunks:
            rag_section = "\n\n---\n\n**Contexto adicional da base de conhecimento:**\n"
            for i, chunk in enumerate(chunks, start=1):
                rag_section += f"\n_{i}._ {chunk.get('chunkText', '')}\n"
            rag_section += "\n---\n"

        user_content = (
            f"## Finding: {ctx['title']} ({finding.rule_id})\n\n"
            f"**Deployment:** `{finding.deployment_name}` no namespace `{finding.namespace}`\n"
            f"**Pilar:** {ctx['pillar']} | **Severidade:** {finding.severity.upper()}\n\n"
            f"**Valor atual:** `{actual}`\n"
            f"**Valor esperado:** `{expected}`\n\n"
            f"**Por que isso importa:**\n{ctx['why']}\n\n"
            f"**Dica de correção:**\n{ctx['fix_hint']}"
            f"{rag_section}\n\n"
            "---\n\n"
            "Por favor, forneça:\n"
            "1. **Explicação detalhada** do problema e seu impacto operacional\n"
            "2. **Causa raiz** mais provável\n"
            "3. **Passos para correção** com exemplos de YAML quando aplicável\n"
            "4. **Boas práticas** relacionadas\n\n"
            "Responda em português brasileiro."
        )

        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ]

    def get_rule_title(self, rule_id: str) -> str:
        return _RULE_CONTEXT.get(rule_id, _GENERIC_CONTEXT)["title"]
