import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.infrastructure.mcp.github_mcp import github_mcp_session
from src.infrastructure.mcp.datadog_mcp import datadog_mcp_session


def _make_session(init_side_effects=None):
    session = AsyncMock()
    if init_side_effects:
        session.initialize.side_effect = init_side_effects
    else:
        session.initialize.return_value = None
    return session


class TestGithubMcpSession:
    @pytest.mark.asyncio
    async def test_raises_when_no_auth(self):
        with pytest.raises(ValueError, match="GitHub auth"):
            async with github_mcp_session():
                pass

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self):
        session = _make_session()
        with (
            patch("src.infrastructure.mcp.github_mcp.stdio_client") as mock_stdio,
            patch("src.infrastructure.mcp.github_mcp.ClientSession") as mock_cs,
        ):
            mock_cs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_cs.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_stdio.return_value.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock()))
            mock_stdio.return_value.__aexit__ = AsyncMock(return_value=False)

            async with github_mcp_session(github_token="ghp-test") as s:
                assert s is session

        session.initialize.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_retries_on_init_timeout(self):
        session = _make_session(init_side_effects=[asyncio.TimeoutError(), None])
        with (
            patch("src.infrastructure.mcp.github_mcp.stdio_client") as mock_stdio,
            patch("src.infrastructure.mcp.github_mcp.ClientSession") as mock_cs,
            patch("src.infrastructure.mcp.github_mcp.asyncio.sleep", new_callable=AsyncMock),
        ):
            mock_cs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_cs.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_stdio.return_value.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock()))
            mock_stdio.return_value.__aexit__ = AsyncMock(return_value=False)

            async with github_mcp_session(github_token="ghp-test") as s:
                assert s is session

        assert session.initialize.await_count == 2

    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self):
        err = asyncio.TimeoutError("init timeout")
        session = _make_session(init_side_effects=[err, err, err])
        with (
            patch("src.infrastructure.mcp.github_mcp.stdio_client") as mock_stdio,
            patch("src.infrastructure.mcp.github_mcp.ClientSession") as mock_cs,
            patch("src.infrastructure.mcp.github_mcp.asyncio.sleep", new_callable=AsyncMock),
            patch("src.infrastructure.mcp.github_mcp.mcp_init_failed_total") as mock_metric,
        ):
            mock_cs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_cs.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_stdio.return_value.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock()))
            mock_stdio.return_value.__aexit__ = AsyncMock(return_value=False)

            with pytest.raises(asyncio.TimeoutError):
                async with github_mcp_session(github_token="ghp-test"):
                    pass

        mock_metric.labels.assert_called_once_with(provider="github")
        mock_metric.labels.return_value.inc.assert_called_once()

    @pytest.mark.asyncio
    async def test_caller_exception_propagates_without_retry(self):
        session = _make_session()
        with (
            patch("src.infrastructure.mcp.github_mcp.stdio_client") as mock_stdio,
            patch("src.infrastructure.mcp.github_mcp.ClientSession") as mock_cs,
        ):
            mock_cs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_cs.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_stdio.return_value.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock()))
            mock_stdio.return_value.__aexit__ = AsyncMock(return_value=False)

            with pytest.raises(RuntimeError, match="caller error"):
                async with github_mcp_session(github_token="ghp-test"):
                    raise RuntimeError("caller error")

        session.initialize.assert_awaited_once()


class TestDatadogMcpSession:
    @pytest.mark.asyncio
    async def test_raises_when_no_api_key(self):
        with pytest.raises(ValueError, match="DD-API-KEY"):
            async with datadog_mcp_session(dd_api_key="", dd_app_key=""):
                pass

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self):
        session = _make_session()
        with (
            patch("src.infrastructure.mcp.datadog_mcp.streamable_http_client") as mock_http,
            patch("src.infrastructure.mcp.datadog_mcp.ClientSession") as mock_cs,
            patch("src.infrastructure.mcp.datadog_mcp.httpx.AsyncClient") as mock_client,
        ):
            mock_cs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_cs.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_http.return_value.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock(), None))
            mock_http.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_client.return_value.__aexit__ = AsyncMock(return_value=False)

            async with datadog_mcp_session(dd_api_key="key", dd_app_key="app") as s:
                assert s is session

        session.initialize.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self):
        err = asyncio.TimeoutError("dd init timeout")
        session = _make_session(init_side_effects=[err, err, err])
        with (
            patch("src.infrastructure.mcp.datadog_mcp.streamable_http_client") as mock_http,
            patch("src.infrastructure.mcp.datadog_mcp.ClientSession") as mock_cs,
            patch("src.infrastructure.mcp.datadog_mcp.httpx.AsyncClient") as mock_client,
            patch("src.infrastructure.mcp.datadog_mcp.asyncio.sleep", new_callable=AsyncMock),
            patch("src.infrastructure.mcp.datadog_mcp.mcp_init_failed_total") as mock_metric,
        ):
            mock_cs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_cs.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_http.return_value.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock(), None))
            mock_http.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_client.return_value.__aexit__ = AsyncMock(return_value=False)

            with pytest.raises(asyncio.TimeoutError):
                async with datadog_mcp_session(dd_api_key="key", dd_app_key="app"):
                    pass

        mock_metric.labels.assert_called_once_with(provider="datadog")
        mock_metric.labels.return_value.inc.assert_called_once()
