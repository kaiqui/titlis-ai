from functools import lru_cache

from src.infrastructure.titlis_api.datadog_config_client import DatadogConfigClient
from src.infrastructure.titlis_api.knowledge_client import KnowledgeClient
from src.infrastructure.titlis_api.scorecard_client import ScorecardClient
from src.pipeline.graph import RemediationGraph
from src.pipeline.session import SessionStore
from src.services.agent_service import AgentService
from src.services.embedding_service import EmbeddingService
from src.services.knowledge_seeder import KnowledgeSeeder
from src.services.llm_service import LLMService
from src.services.prompt_builder import PromptBuilder


@lru_cache()
def get_llm_service() -> LLMService:
    return LLMService()


@lru_cache()
def get_prompt_builder() -> PromptBuilder:
    return PromptBuilder()


@lru_cache()
def get_embedding_service() -> EmbeddingService:
    return EmbeddingService()


@lru_cache()
def get_knowledge_client() -> KnowledgeClient:
    return KnowledgeClient()


@lru_cache()
def get_knowledge_seeder() -> KnowledgeSeeder:
    return KnowledgeSeeder(
        embedding_service=get_embedding_service(),
        knowledge_client=get_knowledge_client(),
    )


@lru_cache()
def get_scorecard_client() -> ScorecardClient:
    return ScorecardClient()


@lru_cache()
def get_datadog_config_client() -> DatadogConfigClient:
    return DatadogConfigClient()


@lru_cache()
def get_remediation_graph() -> RemediationGraph:
    return RemediationGraph(
        llm_service=get_llm_service(),
        scorecard_client=get_scorecard_client(),
        knowledge_client=get_knowledge_client(),
        embedding_service=get_embedding_service(),
    )


@lru_cache()
def get_session_store() -> SessionStore:
    return SessionStore()


@lru_cache()
def get_agent_service() -> AgentService:
    return AgentService(
        scorecard_client=get_scorecard_client(),
        session_store=get_session_store(),
        dd_client=get_datadog_config_client(),
    )
