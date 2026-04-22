from typing import Any, Dict, List, Optional

import httpx

from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ScorecardClient:
    def __init__(self) -> None:
        self._base = settings.titlis_api_url
        self._secret = settings.internal_secret

    def _headers(self) -> Dict[str, str]:
        return {"X-Internal-Secret": self._secret}

    async def get_scorecard_by_uid(self, tenant_id: int, k8s_uid: str) -> Optional[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self._base}/v1/internal/ai/scorecards",
                params={"tenantId": tenant_id, "k8sUid": k8s_uid},
                headers=self._headers(),
                timeout=15.0,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()

    async def get_scorecard_by_name(self, tenant_id: int, name: str, namespace: str) -> Optional[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self._base}/v1/internal/ai/workloads",
                params={"tenantId": tenant_id, "name": name, "namespace": namespace},
                headers=self._headers(),
                timeout=15.0,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()

    async def get_dashboard(self, tenant_id: int, cluster: Optional[str] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"tenantId": tenant_id}
        if cluster:
            params["cluster"] = cluster
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self._base}/v1/internal/ai/dashboard",
                params=params,
                headers=self._headers(),
                timeout=15.0,
            )
            resp.raise_for_status()
            return resp.json()

    async def get_similar_resolved(self, tenant_id: int, rule_id: str, limit: int = 5) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self._base}/v1/internal/ai/similar-resolved",
                params={"tenantId": tenant_id, "ruleId": rule_id, "limit": limit},
                headers=self._headers(),
                timeout=15.0,
            )
            resp.raise_for_status()
            return resp.json()

    async def get_slos(self, tenant_id: int, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"tenantId": tenant_id}
        if namespace:
            params["namespace"] = namespace
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self._base}/v1/internal/ai/slos",
                params=params,
                headers=self._headers(),
                timeout=15.0,
            )
            resp.raise_for_status()
            return resp.json()

    async def get_remediation_history(self, tenant_id: int, k8s_uid: str) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self._base}/v1/internal/ai/remediations",
                params={"tenantId": tenant_id, "k8sUid": k8s_uid},
                headers=self._headers(),
                timeout=15.0,
            )
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
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self._base}/v1/internal/ai/feedback",
                params={"tenantId": tenant_id},
                json={
                    "responseId": response_id,
                    "ruleId": rule_id,
                    "sentiment": sentiment,
                    "comment": comment,
                },
                headers=self._headers(),
                timeout=10.0,
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
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self._base}/v1/internal/ai/remediations",
                json={
                    "workloadId": workload_id,
                    "tenantId": tenant_id,
                    "prUrl": pr_url,
                    "prNumber": pr_number,
                    "githubBranch": github_branch,
                    "repoUrl": repo_url,
                    "findingIds": finding_ids,
                },
                headers=self._headers(),
                timeout=10.0,
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
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self._base}/v1/internal/ai/slo-configs/{slo_config_id}/propose-change",
                params={"tenantId": tenant_id},
                json={
                    "field": field,
                    "oldValue": old_value,
                    "newValue": new_value,
                    "requestedBy": requested_by,
                },
                headers=self._headers(),
                timeout=15.0,
            )
            resp.raise_for_status()
            return resp.json()
