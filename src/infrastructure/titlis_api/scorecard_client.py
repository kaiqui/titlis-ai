import asyncio
from typing import Any, Dict, List, Optional

import httpx

from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_RETRYABLE = (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.ConnectTimeout)

_POOL = httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30)


class ScorecardClient:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=settings.titlis_api_url,
            headers={"X-Internal-Secret": settings.internal_secret},
            timeout=15.0,
            limits=_POOL,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        for attempt in range(3):
            try:
                return await self._client.get(path, params=params)
            except _RETRYABLE as exc:
                if attempt == 2:
                    raise
                delay = 1.0 * (2**attempt)
                logger.warning(
                    "ScorecardClient GET erro transitório, retentando",
                    extra={"path": path, "attempt": attempt + 1, "delay": delay, "error": str(exc)},
                )
                await asyncio.sleep(delay)
        raise RuntimeError("unreachable")

    async def _post(self, path: str, json: Any, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        for attempt in range(3):
            try:
                return await self._client.post(path, json=json, params=params)
            except _RETRYABLE as exc:
                if attempt == 2:
                    raise
                delay = 1.0 * (2**attempt)
                logger.warning(
                    "ScorecardClient POST erro transitório, retentando",
                    extra={"path": path, "attempt": attempt + 1, "delay": delay, "error": str(exc)},
                )
                await asyncio.sleep(delay)
        raise RuntimeError("unreachable")

    async def get_scorecard_by_uid(self, tenant_id: int, k8s_uid: str) -> Optional[Dict[str, Any]]:
        resp = await self._get("/v1/internal/ai/scorecards", {"tenantId": tenant_id, "k8sUid": k8s_uid})
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    async def get_scorecard_by_name(self, tenant_id: int, name: str, namespace: str) -> Optional[Dict[str, Any]]:
        resp = await self._get(
            "/v1/internal/ai/workloads", {"tenantId": tenant_id, "name": name, "namespace": namespace}
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    async def get_dashboard(self, tenant_id: int, cluster: Optional[str] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"tenantId": tenant_id}
        if cluster:
            params["cluster"] = cluster
        resp = await self._get("/v1/internal/ai/dashboard", params)
        resp.raise_for_status()
        return resp.json()

    async def get_similar_resolved(self, tenant_id: int, rule_id: str, limit: int = 5) -> List[Dict[str, Any]]:
        resp = await self._get(
            "/v1/internal/ai/similar-resolved", {"tenantId": tenant_id, "ruleId": rule_id, "limit": limit}
        )
        resp.raise_for_status()
        return resp.json()

    async def get_slos(self, tenant_id: int, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"tenantId": tenant_id}
        if namespace:
            params["namespace"] = namespace
        resp = await self._get("/v1/internal/ai/slos", params)
        resp.raise_for_status()
        return resp.json()

    async def get_remediation_history(self, tenant_id: int, k8s_uid: str) -> List[Dict[str, Any]]:
        resp = await self._get("/v1/internal/ai/remediations", {"tenantId": tenant_id, "k8sUid": k8s_uid})
        resp.raise_for_status()
        return resp.json()

    async def store_feedback(
        self,
        tenant_id: int,
        response_id: str,
        rule_id: str,
        sentiment: str,
        comment: Optional[str] = None,
    ) -> None:
        resp = await self._post(
            "/v1/internal/ai/feedback",
            json={
                "responseId": response_id,
                "ruleId": rule_id,
                "sentiment": sentiment,
                "comment": comment,
            },
            params={"tenantId": tenant_id},
        )
        resp.raise_for_status()

    async def notify_remediation_started(
        self,
        tenant_id: int,
        workload_id: str,
        pr_url: Optional[str],
        pr_number: Optional[int],
        github_branch: Optional[str],
        repo_url: Optional[str],
        finding_ids: List[str],
    ) -> None:
        resp = await self._post(
            "/v1/internal/ai/remediations",
            json={
                "workloadId": workload_id,
                "tenantId": tenant_id,
                "prUrl": pr_url,
                "prNumber": pr_number,
                "githubBranch": github_branch,
                "repoUrl": repo_url,
                "findingIds": finding_ids,
            },
        )
        resp.raise_for_status()

    async def propose_slo_change(
        self,
        tenant_id: int,
        slo_config_id: int,
        field: str,
        old_value: str,
        new_value: str,
        requested_by: str = "titlis-ai",
    ) -> Dict[str, Any]:
        resp = await self._post(
            f"/v1/internal/ai/slo-configs/{slo_config_id}/propose-change",
            json={
                "field": field,
                "oldValue": old_value,
                "newValue": new_value,
                "requestedBy": requested_by,
            },
            params={"tenantId": tenant_id},
        )
        resp.raise_for_status()
        return resp.json()
