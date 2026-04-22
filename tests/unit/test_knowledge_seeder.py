import pytest
from unittest.mock import AsyncMock, MagicMock

from src.services.knowledge_seeder import KnowledgeSeeder, _build_chunk_text
from src.services.prompt_builder import _RULE_CONTEXT


class TestBuildChunkText:
    def test_includes_rule_id_and_title(self) -> None:
        ctx = _RULE_CONTEXT["RES-001"]
        text = _build_chunk_text("RES-001", ctx)
        assert "RES-001" in text
        assert ctx["title"] in text

    def test_includes_pillar_and_fix_hint(self) -> None:
        ctx = _RULE_CONTEXT["SEC-001"]
        text = _build_chunk_text("SEC-001", ctx)
        assert ctx["pillar"] in text
        assert ctx["fix_hint"] in text


class TestKnowledgeSeeder:
    @pytest.mark.asyncio
    async def test_seed_global_rules_seeds_all_known_rules(self) -> None:
        fake_embedding = [0.1] * 1536
        embedding_service = MagicMock()
        embedding_service.embed = AsyncMock(return_value=fake_embedding)

        knowledge_client = MagicMock()
        knowledge_client.index_chunk = AsyncMock(return_value="some-uuid")

        seeder = KnowledgeSeeder(embedding_service, knowledge_client)
        count = await seeder.seed_global_rules(provider="openai", api_key="sk-test")

        assert count == len(_RULE_CONTEXT)
        assert knowledge_client.index_chunk.call_count == len(_RULE_CONTEXT)

    @pytest.mark.asyncio
    async def test_seed_passes_null_tenant_id_for_global(self) -> None:
        fake_embedding = [0.0] * 1536
        embedding_service = MagicMock()
        embedding_service.embed = AsyncMock(return_value=fake_embedding)

        knowledge_client = MagicMock()
        knowledge_client.index_chunk = AsyncMock(return_value="uuid")

        seeder = KnowledgeSeeder(embedding_service, knowledge_client)
        await seeder.seed_global_rules(provider="openai", api_key="sk-test", tenant_id=None)

        first_call_kwargs = knowledge_client.index_chunk.call_args_list[0].kwargs
        assert first_call_kwargs["tenant_id"] is None
        assert first_call_kwargs["source_type"] == "global_rule_doc"

    @pytest.mark.asyncio
    async def test_seed_tolerates_individual_chunk_failure(self) -> None:
        fake_embedding = [0.0] * 1536
        embedding_service = MagicMock()
        embedding_service.embed = AsyncMock(return_value=fake_embedding)

        call_count = 0

        async def flaky_index(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("network error")
            return "uuid"

        knowledge_client = MagicMock()
        knowledge_client.index_chunk = AsyncMock(side_effect=flaky_index)

        seeder = KnowledgeSeeder(embedding_service, knowledge_client)
        count = await seeder.seed_global_rules(provider="openai", api_key="sk-test")

        assert count == len(_RULE_CONTEXT) - 1
