import pytest
from unittest.mock import AsyncMock, patch

from src.domain.models import PullRequestResult
from src.tools.github_tools import (
    _never_reduce_violated,
    build_github_tools,
)

TENANT = 1
TOKEN = "ghp-test"
BASE_BRANCH = "main"


def _fake_pr(number=42, url="https://github.com/org/repo/pull/42", branch="fix/auto-remediation-ns-app-ts"):
    return PullRequestResult(number=number, title="fix", url=url, branch=branch, base_branch=BASE_BRANCH)


def _repo_mock(*, file_content="yaml: content", existing_pr=None, branch_exists=False):
    r = AsyncMock()
    r.get_file_content = AsyncMock(return_value=file_content)
    r.find_open_remediation_pr = AsyncMock(return_value=existing_pr)
    r.branch_exists = AsyncMock(return_value=branch_exists)
    r.create_branch = AsyncMock(return_value=True)
    r.commit_files = AsyncMock(return_value=True)
    r.create_pull_request = AsyncMock(return_value=_fake_pr())
    return r


class TestNeverReduceValidation:
    def test_cpu_reduction_detected(self):
        assert _never_reduce_violated("200m", "100m") is True

    def test_cpu_increase_allowed(self):
        assert _never_reduce_violated("100m", "200m") is False

    def test_memory_reduction_detected(self):
        assert _never_reduce_violated("256Mi", "128Mi") is True

    def test_memory_increase_allowed(self):
        assert _never_reduce_violated("128Mi", "256Mi") is False

    def test_empty_value_safe(self):
        assert _never_reduce_violated("", "100m") is False
        assert _never_reduce_violated("100m", "") is False


class TestReadDeployManifest:
    @pytest.mark.asyncio
    async def test_returns_file_content(self):
        mock_repo = _repo_mock(file_content="apiVersion: apps/v1\n")
        with (
            patch("src.tools.github_tools.GitHubAPIClient"),
            patch("src.tools.github_tools.GitHubRepository", return_value=mock_repo),
        ):
            registry = build_github_tools(TOKEN, BASE_BRANCH, TENANT)
            tool = registry.get("read_deploy_manifest")
            result = await tool.handler(
                repo_url="https://github.com/org/repo",
                branch="main",
                path="deploy.yaml",
            )
        assert result["content"] == "apiVersion: apps/v1\n"
        assert result["path"] == "deploy.yaml"

    @pytest.mark.asyncio
    async def test_file_not_found_returns_error(self):
        mock_repo = _repo_mock(file_content=None)
        with (
            patch("src.tools.github_tools.GitHubAPIClient"),
            patch("src.tools.github_tools.GitHubRepository", return_value=mock_repo),
        ):
            registry = build_github_tools(TOKEN, BASE_BRANCH, TENANT)
            tool = registry.get("read_deploy_manifest")
            result = await tool.handler(
                repo_url="https://github.com/org/repo",
                branch="main",
                path="missing.yaml",
            )
        assert result["error"] == "file_not_found"


class TestCheckExistingPr:
    @pytest.mark.asyncio
    async def test_no_pr_returns_none(self):
        mock_repo = _repo_mock(existing_pr=None)
        with (
            patch("src.tools.github_tools.GitHubAPIClient"),
            patch("src.tools.github_tools.GitHubRepository", return_value=mock_repo),
        ):
            registry = build_github_tools(TOKEN, BASE_BRANCH, TENANT)
            tool = registry.get("check_existing_pr")
            result = await tool.handler(
                repo_url="https://github.com/org/repo",
                namespace="production",
                deployment="payment-api",
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_open_pr_returns_url(self):
        pr = _fake_pr(url="https://github.com/org/repo/pull/7")
        mock_repo = _repo_mock(existing_pr=pr)
        with (
            patch("src.tools.github_tools.GitHubAPIClient"),
            patch("src.tools.github_tools.GitHubRepository", return_value=mock_repo),
        ):
            registry = build_github_tools(TOKEN, BASE_BRANCH, TENANT)
            tool = registry.get("check_existing_pr")
            result = await tool.handler(
                repo_url="https://github.com/org/repo",
                namespace="production",
                deployment="payment-api",
            )
        assert result["pr_url"] == "https://github.com/org/repo/pull/7"


class TestCreateRemediationPr:
    CURRENT_YAML = "resources:\n  requests:\n    cpu: 100m\n    memory: 128Mi\n"
    PATCHED_YAML = "resources:\n  requests:\n    cpu: 200m\n    memory: 256Mi\n"

    @pytest.mark.asyncio
    async def test_creates_pr_successfully(self):
        mock_repo = _repo_mock()
        with (
            patch("src.tools.github_tools.GitHubAPIClient"),
            patch("src.tools.github_tools.GitHubRepository", return_value=mock_repo),
        ):
            registry = build_github_tools(TOKEN, BASE_BRANCH, TENANT)
            tool = registry.get("create_remediation_pr")
            result = await tool.handler(
                repo_url="https://github.com/org/repo",
                path="deploy.yaml",
                patched_yaml=self.PATCHED_YAML,
                current_yaml=self.CURRENT_YAML,
                findings=["RES-003"],
                namespace="production",
                deployment_name="payment-api",
            )
        assert result["pr_number"] == 42
        assert "pr_url" in result

    @pytest.mark.asyncio
    async def test_reuses_existing_branch(self):
        mock_repo = _repo_mock(branch_exists=True)
        with (
            patch("src.tools.github_tools.GitHubAPIClient"),
            patch("src.tools.github_tools.GitHubRepository", return_value=mock_repo),
        ):
            registry = build_github_tools(TOKEN, BASE_BRANCH, TENANT)
            tool = registry.get("create_remediation_pr")
            await tool.handler(
                repo_url="https://github.com/org/repo",
                path="deploy.yaml",
                patched_yaml=self.PATCHED_YAML,
                current_yaml=self.CURRENT_YAML,
                findings=["RES-003"],
                namespace="production",
                deployment_name="payment-api",
            )
        mock_repo.create_branch.assert_not_called()

    @pytest.mark.asyncio
    async def test_never_reduce_raises(self):
        reduced_yaml = "resources:\n  requests:\n    cpu: 50m\n    memory: 64Mi\n"
        mock_repo = _repo_mock()
        with (
            patch("src.tools.github_tools.GitHubAPIClient"),
            patch("src.tools.github_tools.GitHubRepository", return_value=mock_repo),
        ):
            registry = build_github_tools(TOKEN, BASE_BRANCH, TENANT)
            tool = registry.get("create_remediation_pr")
            with pytest.raises(ValueError, match="never-reduce"):
                await tool.handler(
                    repo_url="https://github.com/org/repo",
                    path="deploy.yaml",
                    patched_yaml=reduced_yaml,
                    current_yaml=self.CURRENT_YAML,
                    findings=["RES-003"],
                    namespace="production",
                    deployment_name="payment-api",
                )
