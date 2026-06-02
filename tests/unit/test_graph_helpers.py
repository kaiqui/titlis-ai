import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.pipeline.graph import (
    _detect_env_from_cluster,
    _detect_env_from_namespace,
    _mcp_text,
    _github_session_kwargs,
)


class TestDetectEnvFromCluster:
    def test_prod_keyword(self):
        assert _detect_env_from_cluster("prod-us-east") == "prd"

    def test_production_keyword(self):
        assert _detect_env_from_cluster("production-cluster") == "prd"

    def test_homolog_keyword(self):
        assert _detect_env_from_cluster("hml-cluster") == "hml"

    def test_staging_keyword(self):
        assert _detect_env_from_cluster("staging-k8s") == "hml"

    def test_dev_keyword(self):
        assert _detect_env_from_cluster("dev-cluster") == "dev"

    def test_development_keyword(self):
        assert _detect_env_from_cluster("development-us") == "dev"

    def test_unknown_returns_empty(self):
        assert _detect_env_from_cluster("my-cluster-01") == ""

    def test_case_insensitive(self):
        assert _detect_env_from_cluster("PROD-CLUSTER") == "prd"


class TestDetectEnvFromNamespace:
    def test_production_namespace(self):
        assert _detect_env_from_namespace("production") == "prd"

    def test_prod_namespace(self):
        assert _detect_env_from_namespace("prod-payments") == "prd"

    def test_homolog_namespace(self):
        assert _detect_env_from_namespace("hml-services") == "hml"

    def test_staging_namespace(self):
        assert _detect_env_from_namespace("staging") == "hml"

    def test_dev_namespace(self):
        assert _detect_env_from_namespace("dev-team") == "dev"

    def test_develop_namespace(self):
        assert _detect_env_from_namespace("development") == "dev"

    def test_unknown_returns_empty(self):
        assert _detect_env_from_namespace("payments") == ""


class TestMcpText:
    def test_returns_text_from_content(self):
        item = MagicMock()
        item.text = "manifest content"
        result = MagicMock()
        result.content = [item]
        assert _mcp_text(result) == "manifest content"

    def test_returns_empty_when_no_content(self):
        result = MagicMock()
        result.content = []
        assert _mcp_text(result) == ""

    def test_returns_empty_when_result_is_none(self):
        assert _mcp_text(None) == ""

    def test_returns_empty_when_item_has_no_text(self):
        item = MagicMock(spec=[])
        result = MagicMock()
        result.content = [item]
        assert _mcp_text(result) == ""


class TestGithubSessionKwargs:
    @pytest.mark.asyncio
    async def test_returns_pat_token(self):
        ai_config = {"github_auth_mode": "pat", "github_token": "ghp-test"}
        kwargs = await _github_session_kwargs(ai_config)
        assert kwargs == {"github_token": "ghp-test"}

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_token(self):
        ai_config = {}
        kwargs = await _github_session_kwargs(ai_config)
        assert kwargs == {}

    @pytest.mark.asyncio
    async def test_github_app_mode_with_installation_id(self):
        ai_config = {
            "github_auth_mode": "github_app",
            "github_app_id": "123",
            "github_app_private_key": "pem",
            "github_app_installation_id": "456",
        }
        kwargs = await _github_session_kwargs(ai_config)
        assert kwargs["github_app_id"] == "123"
        assert kwargs["github_app_installation_id"] == "456"

    @pytest.mark.asyncio
    async def test_github_app_mode_resolves_installation_id(self):
        ai_config = {
            "github_auth_mode": "github_app",
            "github_app_id": "123",
            "github_app_private_key": "pem",
        }
        with patch("src.pipeline.graph.resolve_installation_id", new_callable=AsyncMock, return_value="789"):
            kwargs = await _github_session_kwargs(ai_config)
        assert kwargs["github_app_installation_id"] == "789"

    @pytest.mark.asyncio
    async def test_github_app_falls_back_to_pat_if_no_app_id(self):
        ai_config = {
            "github_auth_mode": "github_app",
            "github_token": "ghp-fallback",
        }
        kwargs = await _github_session_kwargs(ai_config)
        assert kwargs == {"github_token": "ghp-fallback"}
