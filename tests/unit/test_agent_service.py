import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.agent_service import AgentService, _ToolRunner, _build_system_prompt
from src.domain.models import AgentToolDecision, ToolProposal
from src.pipeline.session import AgentSession, SessionStore


def _make_session(tenant_id: int = 1, ai_config: dict | None = None) -> AgentSession:
    return AgentSession(
        session_id="sess-1",
        tenant_id=tenant_id,
        ai_config=ai_config or {"provider": "openai", "model": "gpt-4o", "api_key": "sk-test"},
        messages=[],
        pending_proposals=[],
        audit_log=[],
        created_at=0.0,
        last_active=0.0,
    )


def _make_service() -> AgentService:
    scorecard = AsyncMock()
    store = MagicMock(spec=SessionStore)
    dd_client = AsyncMock()
    dd_client.get_dd_config = AsyncMock(return_value=None)
    return AgentService(scorecard_client=scorecard, session_store=store, dd_client=dd_client)


class TestBuildSystemPrompt:
    def test_with_both_github_and_datadog(self):
        prompt = _build_system_prompt(has_github=True, has_datadog=True)
        assert "Datadog MCP" in prompt
        assert "AVISO: As ferramentas do GitHub NÃO" not in prompt

    def test_without_datadog_shows_warning(self):
        prompt = _build_system_prompt(has_github=True, has_datadog=False)
        assert "Datadog NÃO estão disponíveis" in prompt

    def test_without_github_shows_warning(self):
        prompt = _build_system_prompt(has_github=False, has_datadog=True)
        assert "GitHub NÃO estão disponíveis" in prompt

    def test_without_both(self):
        prompt = _build_system_prompt(has_github=False, has_datadog=False)
        assert "GitHub NÃO estão disponíveis" in prompt
        assert "Datadog NÃO estão disponíveis" in prompt


class TestToolRunner:
    def _make_runner(self, gh_names=None, dd_names=None):
        adapter = AsyncMock()
        adapter.execute = AsyncMock(return_value={"result": "custom"})
        gh_session = AsyncMock()
        gh_session.call_tool = AsyncMock(return_value=MagicMock(content=[MagicMock(text='{"pr": 1}')]))
        dd_session = AsyncMock()
        dd_session.call_tool = AsyncMock(return_value=MagicMock(content=[MagicMock(text='{"metric": 99}')]))
        return _ToolRunner(
            adapter=adapter,
            openai_tools=[],
            github_session=gh_session,
            dd_session=dd_session,
            github_tool_names=gh_names or {"create_pull_request"},
            dd_tool_names=dd_names or {"query_metrics"},
            has_github=True,
            has_datadog=True,
        )

    @pytest.mark.asyncio
    async def test_routes_github_tool(self):
        runner = self._make_runner()
        await runner.execute("create_pull_request", {"title": "fix"})
        runner._github_session.call_tool.assert_called_once_with("create_pull_request", {"title": "fix"})

    @pytest.mark.asyncio
    async def test_routes_datadog_tool(self):
        runner = self._make_runner()
        await runner.execute("query_metrics", {"query": "avg:cpu"})
        runner._dd_session.call_tool.assert_called_once_with("query_metrics", {"query": "avg:cpu"})

    @pytest.mark.asyncio
    async def test_routes_custom_tool_to_adapter(self):
        runner = self._make_runner()
        result = await runner.execute("get_current_scorecard", {"workload_id": "uid-1"})
        runner._adapter.execute.assert_called_once_with("get_current_scorecard", {"workload_id": "uid-1"})
        assert result == {"result": "custom"}

    @pytest.mark.asyncio
    async def test_github_session_none_falls_through_to_adapter(self):
        adapter = AsyncMock()
        adapter.execute = AsyncMock(return_value={"result": "fallback"})
        runner = _ToolRunner(
            adapter=adapter,
            openai_tools=[],
            github_session=None,
            dd_session=None,
            github_tool_names={"create_pull_request"},
            dd_tool_names=set(),
        )
        await runner.execute("create_pull_request", {})
        adapter.execute.assert_called_once()


class TestRunToolResponses:
    @pytest.mark.asyncio
    async def test_approved_tool_is_executed(self):
        service = _make_service()
        session = _make_session()
        proposal = ToolProposal(
            proposal_id="prop-1",
            tool_name="get_current_scorecard",
            description="Get scorecard",
            args={"workload_id": "uid-1"},
            is_write=False,
        )
        session.pending_proposals = [proposal]

        decision = AgentToolDecision(proposal_id="prop-1", approved=True)

        mock_runner = MagicMock()
        mock_runner.execute = AsyncMock(return_value={"score": 80})
        mock_runner.has_github = False
        mock_runner.has_datadog = False
        mock_runner.openai_tools = []

        collected = []

        with (
            patch.object(service, "_build_runner", new_callable=AsyncMock, return_value=mock_runner),
            patch.object(service, "_llm_loop", return_value=aiter([])),
        ):
            async for event in service.run_tool_responses(session, [decision]):
                collected.append(event)

        tool_result_events = [e for e in collected if '"type": "tool_result"' in e]
        assert any('"approved": true' in e for e in tool_result_events)
        mock_runner.execute.assert_called_once_with("get_current_scorecard", {"workload_id": "uid-1"})

    @pytest.mark.asyncio
    async def test_rejected_tool_is_not_executed(self):
        service = _make_service()
        session = _make_session()
        proposal = ToolProposal(
            proposal_id="prop-2",
            tool_name="create_pull_request",
            description="Create PR",
            args={"title": "fix"},
            is_write=True,
        )
        session.pending_proposals = [proposal]

        decision = AgentToolDecision(proposal_id="prop-2", approved=False)

        mock_runner = MagicMock()
        mock_runner.execute = AsyncMock()
        mock_runner.has_github = False
        mock_runner.has_datadog = False
        mock_runner.openai_tools = []

        with (
            patch.object(service, "_build_runner", new_callable=AsyncMock, return_value=mock_runner),
            patch.object(service, "_llm_loop", return_value=aiter([])),
        ):
            async for _ in service.run_tool_responses(session, [decision]):
                pass

        mock_runner.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_proposal_id_is_skipped(self):
        service = _make_service()
        session = _make_session()
        session.pending_proposals = []
        decision = AgentToolDecision(proposal_id="nonexistent", approved=True)

        mock_runner = MagicMock()
        mock_runner.execute = AsyncMock()
        mock_runner.has_github = False
        mock_runner.has_datadog = False
        mock_runner.openai_tools = []

        with (
            patch.object(service, "_build_runner", new_callable=AsyncMock, return_value=mock_runner),
            patch.object(service, "_llm_loop", return_value=aiter([])),
        ):
            async for _ in service.run_tool_responses(session, [decision]):
                pass

        mock_runner.execute.assert_not_called()


async def aiter(items):
    for item in items:
        yield item
