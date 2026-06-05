import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.pipeline.graph import RemediationGraph

# ── helpers ───────────────────────────────────────────────────────────────────

SCORECARD = {
    "workload_id": "uid-abc",
    "workload": "payment-api",
    "namespace": "production",
    "overall_score": 72,
    "validation_results": [
        {"rule_id": "RES-003", "passed": False, "message": "No cpu request", "actual_value": None},
        {"rule_id": "RES-001", "passed": True, "message": "ok", "actual_value": "1"},
    ],
}

AI_CONFIG = {
    "provider": "openai",
    "model": "gpt-4o",
    "api_key": "sk-test",
    "github_token": "ghp-test",
    "github_base_branch": "main",
}


def _build_graph(**overrides):
    defaults = dict(
        llm_service=AsyncMock(),
        scorecard_client=AsyncMock(),
        knowledge_client=AsyncMock(),
        embedding_service=AsyncMock(),
        udp_client=AsyncMock(),
    )
    defaults.update(overrides)
    g = RemediationGraph.__new__(RemediationGraph)
    for k, v in defaults.items():
        setattr(g, f"_{k.split('_')[0] if '_' not in k else k.split('service')[0].rstrip('_') or k}", v)
    g._llm = defaults["llm_service"]
    g._scorecard = defaults["scorecard_client"]
    g._knowledge = defaults["knowledge_client"]
    g._embedding = defaults["embedding_service"]
    g._udp = defaults["udp_client"]
    return g


# ── classify_findings ─────────────────────────────────────────────────────────


class TestClassifyFindings:
    @pytest.mark.asyncio
    async def test_filters_to_requested_finding_ids(self):
        g = _build_graph()
        g._scorecard.get_scorecard_by_uid = AsyncMock(return_value=SCORECARD)
        state = {"tenant_id": 1, "workload_id": "uid-abc", "finding_ids": ["RES-003"]}
        result = await g._classify_findings(state)
        assert len(result["findings"]) == 1
        assert result["findings"][0]["rule_id"] == "RES-003"

    @pytest.mark.asyncio
    async def test_empty_finding_ids_returns_all_failed(self):
        g = _build_graph()
        g._scorecard.get_scorecard_by_uid = AsyncMock(return_value=SCORECARD)
        state = {"tenant_id": 1, "workload_id": "uid-abc", "finding_ids": []}
        result = await g._classify_findings(state)
        assert all(not f.get("passed") for f in result["findings"])

    @pytest.mark.asyncio
    async def test_workload_not_found_returns_empty(self):
        g = _build_graph()
        g._scorecard.get_scorecard_by_uid = AsyncMock(return_value=None)
        state = {"tenant_id": 1, "workload_id": "missing", "finding_ids": []}
        result = await g._classify_findings(state)
        assert result["findings"] == []

    @pytest.mark.asyncio
    async def test_sets_namespace_and_deployment_name(self):
        g = _build_graph()
        g._scorecard.get_scorecard_by_uid = AsyncMock(return_value=SCORECARD)
        state = {"tenant_id": 1, "workload_id": "uid-abc", "finding_ids": []}
        result = await g._classify_findings(state)
        assert result["namespace"] == "production"
        assert result["deployment_name"] == "payment-api"


# ── validate_patch ────────────────────────────────────────────────────────────


class TestValidatePatch:
    @pytest.mark.asyncio
    async def test_valid_yaml_no_errors(self):
        g = _build_graph()
        state = {
            "patched_manifest": "apiVersion: apps/v1\nkind: Deployment\n",
            "current_manifest": "apiVersion: apps/v1\n",
        }
        result = await g._validate_patch(state)
        assert result["validation_errors"] == []

    @pytest.mark.asyncio
    async def test_invalid_yaml_returns_error(self):
        g = _build_graph()
        state = {"patched_manifest": "key: [unclosed", "current_manifest": ""}
        result = await g._validate_patch(state)
        assert any("YAML" in e for e in result["validation_errors"])

    @pytest.mark.asyncio
    async def test_never_reduce_cpu_detected(self):
        g = _build_graph()
        state = {
            "patched_manifest": "resources:\n  requests:\n    cpu: 50m\n",
            "current_manifest": "resources:\n  requests:\n    cpu: 200m\n",
        }
        result = await g._validate_patch(state)
        assert any("never-reduce" in e for e in result["validation_errors"])

    @pytest.mark.asyncio
    async def test_increase_allowed(self):
        g = _build_graph()
        state = {
            "patched_manifest": "resources:\n  requests:\n    cpu: 500m\n",
            "current_manifest": "resources:\n  requests:\n    cpu: 200m\n",
        }
        result = await g._validate_patch(state)
        assert result["validation_errors"] == []


# ── routing ───────────────────────────────────────────────────────────────────


class TestRouting:
    def test_route_after_check_pr_with_existing_pr(self):
        from langgraph.graph import END

        state = {"existing_pr": {"pr_url": "https://github.com/org/repo/pull/1"}}
        assert RemediationGraph._route_after_check_pr(state) == END

    def test_route_after_check_pr_no_pr(self):
        state = {"existing_pr": None}
        assert RemediationGraph._route_after_check_pr(state) == "analyze_findings"

    def test_route_after_validate_no_errors(self):
        state = {"validation_errors": [], "retry_count": 0}
        assert RemediationGraph._route_after_validate(state) == "await_user_confirmation"

    def test_route_after_validate_errors_retry(self):
        state = {"validation_errors": ["bad yaml"], "retry_count": 1}
        assert RemediationGraph._route_after_validate(state) == "generate_yaml_patch"

    def test_route_after_validate_errors_max_retries(self):
        from langgraph.graph import END

        state = {"validation_errors": ["still bad"], "retry_count": 3}
        assert RemediationGraph._route_after_validate(state) == END

    def test_route_after_confirmation_approved(self):
        state = {"approved": True}
        assert RemediationGraph._route_after_confirmation(state) == "create_remediation_pr"

    def test_route_after_confirmation_rejected(self):
        from langgraph.graph import END

        state = {"approved": False}
        assert RemediationGraph._route_after_confirmation(state) == END


# ── notify_api ────────────────────────────────────────────────────────────────


class TestNotifyApi:
    @pytest.mark.asyncio
    async def test_sends_remediation_started(self):
        scorecard = AsyncMock()
        g = _build_graph(scorecard_client=scorecard)
        state = {
            "tenant_id": 1,
            "workload_id": "uid-abc",
            "findings": [{"rule_id": "RES-003"}],
            "pr_result": {"pr_url": "https://github.com/org/repo/pull/42", "pr_number": 42, "branch": "fix/..."},
        }
        await g._notify_api(state)
        scorecard.notify_remediation_started.assert_called_once()
        call_kwargs = scorecard.notify_remediation_started.call_args
        assert call_kwargs.kwargs.get("tenant_id") == 1
        assert call_kwargs.kwargs.get("workload_id") == "uid-abc"


# ── _analyze_findings ─────────────────────────────────────────────────────────


class TestAnalyzeFindings:
    @pytest.mark.asyncio
    async def test_calls_llm_and_returns_analysis(self):
        g = _build_graph()
        g._llm.chat = AsyncMock(return_value="Adicione cpu limits ao container.")
        state = {
            "ai_config": {"provider": "openai", "model": "gpt-4o", "api_key": "sk-test"},
            "findings": [{"rule_id": "RES-003", "message": "No cpu limit", "actual_value": None}],
            "current_manifest": "apiVersion: apps/v1\n",
            "deployment_name": "payment-api",
            "namespace": "production",
            "rag_context": [],
            "tenant_id": 1,
        }
        result = await g._analyze_findings(state)
        assert "analysis" in result
        assert result["analysis"] == "Adicione cpu limits ao container."
        g._llm.chat.assert_called_once()

    @pytest.mark.asyncio
    async def test_injects_rag_context_into_prompt(self):
        g = _build_graph()
        g._llm.chat = AsyncMock(return_value="fix it")
        state = {
            "ai_config": {"provider": "openai", "model": "gpt-4o", "api_key": "sk-test"},
            "findings": [{"rule_id": "RES-003", "message": "No cpu", "actual_value": None}],
            "current_manifest": "",
            "deployment_name": "api",
            "namespace": "prod",
            "rag_context": [{"chunkText": "Use requests.cpu: 100m"}],
            "tenant_id": 1,
        }
        await g._analyze_findings(state)
        call_messages = g._llm.chat.call_args[0][0]
        user_content = call_messages[1]["content"]
        assert "Use requests.cpu: 100m" in user_content


# ── _generate_yaml_patch ──────────────────────────────────────────────────────


class TestGenerateYamlPatch:
    @pytest.mark.asyncio
    async def test_returns_patched_yaml(self):
        g = _build_graph()
        g._llm.chat = AsyncMock(return_value="apiVersion: apps/v1\nkind: Deployment\n")
        state = {
            "ai_config": {"provider": "openai", "model": "gpt-4o", "api_key": "sk-test"},
            "analysis": "Adicione cpu limits.",
            "current_manifest": "apiVersion: apps/v1\n",
            "findings": [{"rule_id": "RES-003", "message": "No cpu", "actual_value": None}],
            "retry_count": 0,
            "validation_errors": [],
            "tenant_id": 1,
        }
        result = await g._generate_yaml_patch(state)
        assert result["patched_manifest"] == "apiVersion: apps/v1\nkind: Deployment"
        assert result["retry_count"] == 1
        assert result["validation_errors"] == []

    @pytest.mark.asyncio
    async def test_strips_markdown_fences(self):
        g = _build_graph()
        g._llm.chat = AsyncMock(return_value="```yaml\napiVersion: apps/v1\n```")
        state = {
            "ai_config": {"provider": "openai", "model": "gpt-4o", "api_key": "sk-test"},
            "analysis": "fix",
            "current_manifest": "apiVersion: apps/v1\n",
            "findings": [],
            "retry_count": 0,
            "validation_errors": [],
            "tenant_id": 1,
        }
        result = await g._generate_yaml_patch(state)
        assert "```" not in result["patched_manifest"]
        assert "apiVersion" in result["patched_manifest"]

    @pytest.mark.asyncio
    async def test_includes_error_feedback_on_retry(self):
        g = _build_graph()
        g._llm.chat = AsyncMock(return_value="apiVersion: apps/v1\n")
        state = {
            "ai_config": {"provider": "openai", "model": "gpt-4o", "api_key": "sk-test"},
            "analysis": "fix",
            "current_manifest": "apiVersion: apps/v1\n",
            "findings": [],
            "retry_count": 1,
            "validation_errors": ["YAML inválido: scan error"],
            "tenant_id": 1,
        }
        await g._generate_yaml_patch(state)
        call_messages = g._llm.chat.call_args[0][0]
        user_content = call_messages[1]["content"]
        assert "YAML inválido" in user_content


# ── _check_existing_pr ────────────────────────────────────────────────────────


class TestCheckExistingPr:
    @pytest.mark.asyncio
    async def test_returns_none_when_no_github_config(self):
        g = _build_graph()
        state = {
            "ai_config": {},
            "repo_url": "",
            "namespace": "production",
            "deployment_name": "payment-api",
        }
        result = await g._check_existing_pr(state)
        assert result == {"existing_pr": None}

    @pytest.mark.asyncio
    async def test_finds_matching_pr(self):
        g = _build_graph()
        prs = [
            {
                "head": {"ref": "fix/auto-remediation-production-payment-api-20240101"},
                "html_url": "https://github.com/org/repo/pull/7",
                "number": 7,
            }
        ]
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.content = [MagicMock(text=json.dumps(prs))]
        mock_session.call_tool = AsyncMock(return_value=mock_result)

        with (
            patch(
                "src.pipeline.graph._github_session_kwargs",
                new_callable=AsyncMock,
                return_value={"github_token": "ghp-test"},
            ),
            patch("src.pipeline.graph.github_mcp_session") as mock_mcp,
        ):
            mock_mcp.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_mcp.return_value.__aexit__ = AsyncMock(return_value=False)
            state = {
                "ai_config": {"github_token": "ghp-test", "github_base_branch": "main"},
                "repo_url": "https://github.com/org/repo",
                "namespace": "production",
                "deployment_name": "payment-api",
            }
            result = await g._check_existing_pr(state)

        assert result["existing_pr"]["pr_url"] == "https://github.com/org/repo/pull/7"

    @pytest.mark.asyncio
    async def test_returns_none_when_no_matching_pr(self):
        g = _build_graph()
        prs = [{"head": {"ref": "feature/other-branch"}, "html_url": "...", "number": 1}]
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.content = [MagicMock(text=json.dumps(prs))]
        mock_session.call_tool = AsyncMock(return_value=mock_result)

        with (
            patch(
                "src.pipeline.graph._github_session_kwargs",
                new_callable=AsyncMock,
                return_value={"github_token": "ghp-test"},
            ),
            patch("src.pipeline.graph.github_mcp_session") as mock_mcp,
        ):
            mock_mcp.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_mcp.return_value.__aexit__ = AsyncMock(return_value=False)
            state = {
                "ai_config": {"github_token": "ghp-test", "github_base_branch": "main"},
                "repo_url": "https://github.com/org/repo",
                "namespace": "production",
                "deployment_name": "payment-api",
            }
            result = await g._check_existing_pr(state)

        assert result == {"existing_pr": None}


# ── _fetch_rag_context ────────────────────────────────────────────────────────


class TestFetchRagContext:
    @pytest.mark.asyncio
    async def test_returns_empty_when_no_findings(self):
        g = _build_graph()
        result = await g._fetch_rag_context([], {"api_key": "sk-test"})
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_api_key(self):
        g = _build_graph()
        result = await g._fetch_rag_context([{"rule_id": "RES-003"}], {})
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_chunks_on_success(self):
        g = _build_graph()
        g._embedding.embed = AsyncMock(return_value=[0.1, 0.2])
        g._knowledge.search_similar = AsyncMock(return_value=[{"chunkText": "Use cpu limits"}])
        result = await g._fetch_rag_context([{"rule_id": "RES-003"}], {"api_key": "sk-test", "provider": "openai"})
        assert result == [{"chunkText": "Use cpu limits"}]

    @pytest.mark.asyncio
    async def test_returns_empty_on_error(self):
        g = _build_graph()
        g._embedding.embed = AsyncMock(side_effect=Exception("embed failed"))
        result = await g._fetch_rag_context([{"rule_id": "RES-003"}], {"api_key": "sk-test", "provider": "openai"})
        assert result == []


# ── _generate_yaml_patch — manifest_fetch_error ───────────────────────────────


class TestGenerateYamlPatchManifestError:
    @pytest.mark.asyncio
    async def test_raises_with_manifest_fetch_error_message(self):
        g = _build_graph()
        state = {
            "ai_config": {"provider": "openai", "model": "gpt-4o", "api_key": "sk-test"},
            "analysis": "fix",
            "current_manifest": None,
            "manifest_fetch_error": "Branch 'release' não encontrado no repositório org/repo.",
            "findings": [],
            "retry_count": 0,
            "validation_errors": [],
            "tenant_id": 1,
        }
        with pytest.raises(RuntimeError, match="Branch 'release'"):
            await g._generate_yaml_patch(state)

    @pytest.mark.asyncio
    async def test_raises_generic_message_when_no_fetch_error(self):
        g = _build_graph()
        state = {
            "ai_config": {"provider": "openai", "model": "gpt-4o", "api_key": "sk-test"},
            "analysis": "fix",
            "current_manifest": None,
            "manifest_fetch_error": None,
            "deploy_manifest_path": "deploy.yaml",
            "findings": [],
            "retry_count": 0,
            "validation_errors": [],
            "tenant_id": 1,
        }
        with pytest.raises(RuntimeError, match="deploy.yaml"):
            await g._generate_yaml_patch(state)


# ── _check_existing_pr — effective_base_branch ───────────────────────────────


class TestCheckExistingPrEffectiveBranch:
    @pytest.mark.asyncio
    async def test_uses_effective_base_branch_over_ai_config(self):
        g = _build_graph()
        prs: list = []
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.content = [MagicMock(text=json.dumps(prs))]
        mock_session.call_tool = AsyncMock(return_value=mock_result)

        with (
            patch(
                "src.pipeline.graph._github_session_kwargs",
                new_callable=AsyncMock,
                return_value={"github_token": "ghp-test"},
            ),
            patch("src.pipeline.graph.github_mcp_session") as mock_mcp,
        ):
            mock_mcp.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_mcp.return_value.__aexit__ = AsyncMock(return_value=False)
            state = {
                "ai_config": {"github_token": "ghp-test", "github_base_branch": "main"},
                "effective_base_branch": "release",
                "repo_url": "https://github.com/org/repo",
                "namespace": "production",
                "deployment_name": "payment-api",
            }
            await g._check_existing_pr(state)

        call_args = mock_session.call_tool.call_args
        assert call_args[0][1]["base"] == "release"


# ── _sync_closed_pr_status ────────────────────────────────────────────────────


class TestSyncClosedPrStatus:
    @pytest.mark.asyncio
    async def test_calls_notify_pr_closed_when_pr_is_closed_without_merge(self):
        scorecard = AsyncMock()
        scorecard.get_current_remediation = AsyncMock(return_value={"status": "IN_PROGRESS", "github_pr_number": 42})
        scorecard.notify_pr_closed = AsyncMock()
        g = _build_graph(scorecard_client=scorecard)

        closed_pr_result = MagicMock()
        closed_pr_result.isError = False
        closed_pr_result.content = [MagicMock(text=json.dumps({"state": "closed", "merged": False}))]

        mock_session = AsyncMock()
        mock_session.call_tool = AsyncMock(return_value=closed_pr_result)

        await g._sync_closed_pr_status(mock_session, "org", "repo", tenant_id=1, workload_id="uid-abc")

        scorecard.notify_pr_closed.assert_called_once_with(1, "uid-abc")

    @pytest.mark.asyncio
    async def test_does_not_call_notify_when_pr_still_open(self):
        scorecard = AsyncMock()
        scorecard.get_current_remediation = AsyncMock(return_value={"status": "IN_PROGRESS", "github_pr_number": 42})
        scorecard.notify_pr_closed = AsyncMock()
        g = _build_graph(scorecard_client=scorecard)

        open_pr_result = MagicMock()
        open_pr_result.isError = False
        open_pr_result.content = [MagicMock(text=json.dumps({"state": "open", "merged": False}))]

        mock_session = AsyncMock()
        mock_session.call_tool = AsyncMock(return_value=open_pr_result)

        await g._sync_closed_pr_status(mock_session, "org", "repo", tenant_id=1, workload_id="uid-abc")

        scorecard.notify_pr_closed.assert_not_called()

    @pytest.mark.asyncio
    async def test_does_not_call_notify_when_pr_was_merged(self):
        scorecard = AsyncMock()
        scorecard.get_current_remediation = AsyncMock(return_value={"status": "IN_PROGRESS", "github_pr_number": 42})
        scorecard.notify_pr_closed = AsyncMock()
        g = _build_graph(scorecard_client=scorecard)

        merged_pr_result = MagicMock()
        merged_pr_result.isError = False
        merged_pr_result.content = [MagicMock(text=json.dumps({"state": "closed", "merged": True}))]

        mock_session = AsyncMock()
        mock_session.call_tool = AsyncMock(return_value=merged_pr_result)

        await g._sync_closed_pr_status(mock_session, "org", "repo", tenant_id=1, workload_id="uid-abc")

        scorecard.notify_pr_closed.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_no_active_remediation_in_db(self):
        scorecard = AsyncMock()
        scorecard.get_current_remediation = AsyncMock(return_value=None)
        scorecard.notify_pr_closed = AsyncMock()
        g = _build_graph(scorecard_client=scorecard)

        mock_session = AsyncMock()
        await g._sync_closed_pr_status(mock_session, "org", "repo", tenant_id=1, workload_id="uid-abc")

        scorecard.notify_pr_closed.assert_not_called()
        mock_session.call_tool.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_status_is_not_active(self):
        scorecard = AsyncMock()
        scorecard.get_current_remediation = AsyncMock(return_value={"status": "PR_MERGED", "github_pr_number": 42})
        scorecard.notify_pr_closed = AsyncMock()
        g = _build_graph(scorecard_client=scorecard)

        mock_session = AsyncMock()
        await g._sync_closed_pr_status(mock_session, "org", "repo", tenant_id=1, workload_id="uid-abc")

        scorecard.notify_pr_closed.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_no_pr_number(self):
        scorecard = AsyncMock()
        scorecard.get_current_remediation = AsyncMock(return_value={"status": "IN_PROGRESS", "github_pr_number": None})
        scorecard.notify_pr_closed = AsyncMock()
        g = _build_graph(scorecard_client=scorecard)

        mock_session = AsyncMock()
        await g._sync_closed_pr_status(mock_session, "org", "repo", tenant_id=1, workload_id="uid-abc")

        scorecard.notify_pr_closed.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_pr_number_is_zero(self):
        scorecard = AsyncMock()
        scorecard.get_current_remediation = AsyncMock(return_value={"status": "IN_PROGRESS", "github_pr_number": 0})
        scorecard.notify_pr_closed = AsyncMock()
        g = _build_graph(scorecard_client=scorecard)

        mock_session = AsyncMock()
        await g._sync_closed_pr_status(mock_session, "org", "repo", tenant_id=1, workload_id="uid-abc")

        scorecard.notify_pr_closed.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_status_is_null_from_db(self):
        scorecard = AsyncMock()
        scorecard.get_current_remediation = AsyncMock(return_value={"status": None, "github_pr_number": 42})
        scorecard.notify_pr_closed = AsyncMock()
        g = _build_graph(scorecard_client=scorecard)

        mock_session = AsyncMock()
        await g._sync_closed_pr_status(mock_session, "org", "repo", tenant_id=1, workload_id="uid-abc")

        scorecard.notify_pr_closed.assert_not_called()

    @pytest.mark.asyncio
    async def test_is_resilient_to_api_errors(self):
        scorecard = AsyncMock()
        scorecard.get_current_remediation = AsyncMock(side_effect=Exception("network error"))
        scorecard.notify_pr_closed = AsyncMock()
        g = _build_graph(scorecard_client=scorecard)

        mock_session = AsyncMock()
        # não deve propagar exceção
        await g._sync_closed_pr_status(mock_session, "org", "repo", tenant_id=1, workload_id="uid-abc")

        scorecard.notify_pr_closed.assert_not_called()
