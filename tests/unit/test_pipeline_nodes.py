import pytest
from unittest.mock import AsyncMock

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
    async def test_sends_udp_event(self):
        udp = AsyncMock()
        g = _build_graph(udp_client=udp)
        state = {
            "tenant_id": 1,
            "workload_id": "uid-abc",
            "findings": [{"rule_id": "RES-003"}],
            "pr_result": {"pr_url": "https://github.com/org/repo/pull/42", "pr_number": 42, "branch": "fix/..."},
        }
        await g._notify_api(state)
        udp.send.assert_called_once()
        call_kwargs = udp.send.call_args
        assert call_kwargs[1]["event_type"] == "remediation_started" or call_kwargs[0][0] == "remediation_started"
