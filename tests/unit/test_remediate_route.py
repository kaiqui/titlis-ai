import json as _json
import time
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

import src.routes.remediate as remediate_module
from src.main import app
from src.settings import settings


def _headers() -> dict:
    return {"X-Internal-Secret": settings.internal_secret}


def _remediate_payload(workload_id: str = "uid-abc") -> dict:
    return {
        "tenant_id": 1,
        "workload_id": workload_id,
        "finding_ids": ["RES-003"],
        "repo_url": "https://github.com/org/payment-api",
        "deploy_manifest_path": "deploy.yaml",
        "ai_config": {
            "provider": "openai",
            "model": "gpt-4o",
            "api_key": "sk-test",
            "github_token": "ghp-test",
        },
    }


def _fake_astream_fix_ready():
    async def _gen(*args, **kwargs):
        yield {
            "__interrupt__": [
                MagicMock(
                    value={
                        "patched_manifest": "yaml: patched",
                        "current_manifest": "yaml: current",
                        "findings": ["RES-003"],
                        "deployment_name": "payment-api",
                        "namespace": "production",
                    }
                )
            ]
        }

    return _gen


def _fake_astream_existing_pr():
    async def _gen(*args, **kwargs):
        yield {"check_existing_pr": {"existing_pr": {"pr_url": "https://github.com/org/repo/pull/1"}}}

    return _gen


def _fake_astream_confirm_pr():
    async def _gen(*args, **kwargs):
        yield {
            "create_remediation_pr": {
                "pr_result": {"pr_url": "https://github.com/org/repo/pull/42", "pr_number": 42, "branch": "fix/..."}
            }
        }
        yield {"notify_api": {}}

    return _gen


class TestRemediateRoute:
    def test_returns_403_without_secret(self):
        client = TestClient(app)
        resp = client.post("/v1/remediate", json=_remediate_payload())
        assert resp.status_code == 403

    def test_returns_422_on_missing_field(self):
        client = TestClient(app)
        resp = client.post("/v1/remediate", json={"tenant_id": 1}, headers=_headers())
        assert resp.status_code == 422

    def test_streams_fix_ready_event(self):
        client = TestClient(app, raise_server_exceptions=False)

        mock_graph = MagicMock()
        mock_graph.compiled.astream = _fake_astream_fix_ready()
        mock_graph.compiled.get_state = MagicMock(return_value=MagicMock(values={}))

        with patch("src.routes.remediate.get_remediation_graph", return_value=mock_graph):
            resp = client.post("/v1/remediate", json=_remediate_payload(), headers=_headers())

        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers["content-type"]
        body = resp.text
        assert "fix_ready" in body
        assert "thread_id" in body
        assert "patched_manifest" in body
        assert '"type": "done"' in body or '"type":"done"' in body

    def test_streams_existing_pr_event(self):
        client = TestClient(app, raise_server_exceptions=False)

        mock_graph = MagicMock()
        mock_graph.compiled.astream = _fake_astream_existing_pr()
        mock_graph.compiled.get_state = MagicMock(return_value=MagicMock(values={}))

        with patch("src.routes.remediate.get_remediation_graph", return_value=mock_graph):
            resp = client.post("/v1/remediate", json=_remediate_payload(), headers=_headers())

        assert resp.status_code == 200
        body = resp.text
        assert "existing_pr" in body

    def test_streams_error_on_exception(self):
        client = TestClient(app, raise_server_exceptions=False)

        async def _bad_stream(*args, **kwargs):
            raise RuntimeError("boom")
            yield {}  # make it a generator

        mock_graph = MagicMock()
        mock_graph.compiled.astream = _bad_stream

        with patch("src.routes.remediate.get_remediation_graph", return_value=mock_graph):
            resp = client.post("/v1/remediate", json=_remediate_payload(), headers=_headers())

        assert resp.status_code == 200
        assert "error" in resp.text

    def test_fix_ready_contains_required_fields(self):
        """fix_ready deve conter thread_id, patched_manifest e current_manifest."""
        client = TestClient(app, raise_server_exceptions=False)

        mock_graph = MagicMock()
        mock_graph.compiled.astream = _fake_astream_fix_ready()
        mock_graph.compiled.get_state = MagicMock(return_value=MagicMock(values={}))

        with patch("src.routes.remediate.get_remediation_graph", return_value=mock_graph):
            resp = client.post("/v1/remediate", json=_remediate_payload(), headers=_headers())

        assert resp.status_code == 200
        fix_ready_line = next(
            (line for line in resp.text.splitlines() if "fix_ready" in line and line.startswith("data:")),
            None,
        )
        assert fix_ready_line is not None, "Linha SSE com fix_ready nao encontrada"
        event = _json.loads(fix_ready_line.removeprefix("data: "))
        assert event["type"] == "fix_ready"
        assert "thread_id" in event and event["thread_id"]
        assert "patched_manifest" in event
        assert "current_manifest" in event

    def test_llm_timeout_emits_error_event(self):
        """Se o LLM demorar mais de _LLM_CALL_TIMEOUT, deve chegar um evento error."""
        client = TestClient(app, raise_server_exceptions=False)

        async def _timeout_stream(*args, **kwargs):
            raise RuntimeError("LLM (gemini-2.5-flash) não respondeu em 120s. Verifique a API key ou tente novamente.")
            yield {}  # torna gerador

        mock_graph = MagicMock()
        mock_graph.compiled.astream = _timeout_stream

        with patch("src.routes.remediate.get_remediation_graph", return_value=mock_graph):
            resp = client.post("/v1/remediate", json=_remediate_payload(), headers=_headers())

        assert resp.status_code == 200
        body = resp.text
        assert '"type": "error"' in body or '"type":"error"' in body
        assert "120s" in body or "API key" in body
        assert '"type": "done"' in body or '"type":"done"' in body

    def test_done_emitted_after_existing_pr(self):
        """Quando existing_pr é detectado, done deve vir após o evento existing_pr."""
        client = TestClient(app, raise_server_exceptions=False)

        mock_graph = MagicMock()
        mock_graph.compiled.astream = _fake_astream_existing_pr()
        mock_graph.compiled.get_state = MagicMock(return_value=MagicMock(values={}))

        with patch("src.routes.remediate.get_remediation_graph", return_value=mock_graph):
            resp = client.post("/v1/remediate", json=_remediate_payload(), headers=_headers())

        assert resp.status_code == 200
        body = resp.text
        assert "existing_pr" in body
        existing_pos = body.index("existing_pr")
        done_pos = body.index('"type": "done"') if '"type": "done"' in body else body.index('"type":"done"')
        assert done_pos > existing_pos, "done deve vir DEPOIS de existing_pr"


class TestConfirmRemediationRoute:
    def test_returns_403_without_secret(self):
        client = TestClient(app)
        resp = client.post("/v1/remediate/some-thread/confirm", json={"approved": True})
        assert resp.status_code == 403

    def test_streams_pr_created_on_approve(self):
        client = TestClient(app, raise_server_exceptions=False)

        mock_graph = MagicMock()
        mock_graph.compiled.astream = _fake_astream_confirm_pr()

        remediate_module._thread_interrupt_times["test-thread-id"] = time.time()
        with patch("src.routes.remediate.get_remediation_graph", return_value=mock_graph):
            resp = client.post(
                "/v1/remediate/test-thread-id/confirm",
                json={"approved": True},
                headers=_headers(),
            )

        assert resp.status_code == 200
        body = resp.text
        assert "pr_created" in body
        assert "pr_url" in body

    def test_streams_done_on_reject(self):
        client = TestClient(app, raise_server_exceptions=False)

        async def _empty_stream(*args, **kwargs):
            return
            yield {}

        mock_graph = MagicMock()
        mock_graph.compiled.astream = _empty_stream

        remediate_module._thread_interrupt_times["test-thread-id"] = time.time()
        with patch("src.routes.remediate.get_remediation_graph", return_value=mock_graph):
            resp = client.post(
                "/v1/remediate/test-thread-id/confirm",
                json={"approved": False},
                headers=_headers(),
            )

        assert resp.status_code == 200
        assert '"type": "done"' in resp.text or '"type":"done"' in resp.text
