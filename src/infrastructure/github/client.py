from typing import Any, Dict, List, Optional

import httpx

from src.utils.logger import get_logger

logger = get_logger(__name__)

GITHUB_API_BASE = "https://api.github.com"


class GitHubAPIClient:
    def __init__(self, token: str, timeout: float = 30.0) -> None:
        self._token = token
        self._timeout = timeout
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        }

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{GITHUB_API_BASE}{path}"
        async with httpx.AsyncClient(timeout=self._timeout) as http:
            response = await http.get(url, headers=self._headers, params=params)
            response.raise_for_status()
            result: Dict[str, Any] = response.json()
            return result

    async def get_list(self, path: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        url = f"{GITHUB_API_BASE}{path}"
        async with httpx.AsyncClient(timeout=self._timeout) as http:
            response = await http.get(url, headers=self._headers, params=params)
            response.raise_for_status()
            result: List[Dict[str, Any]] = response.json()
            return result

    async def post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{GITHUB_API_BASE}{path}"
        async with httpx.AsyncClient(timeout=self._timeout) as http:
            response = await http.post(url, headers=self._headers, json=payload)
            response.raise_for_status()
            result: Dict[str, Any] = response.json()
            return result

    async def put(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{GITHUB_API_BASE}{path}"
        async with httpx.AsyncClient(timeout=self._timeout) as http:
            response = await http.put(url, headers=self._headers, json=payload)
            response.raise_for_status()
            result: Dict[str, Any] = response.json()
            return result
