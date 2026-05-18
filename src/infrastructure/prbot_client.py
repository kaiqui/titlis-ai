import httpx

from src.settings import settings


class PrbotClient:
    def __init__(self):
        self._base_url = settings.titlis_api_url
        self._secret = settings.internal_secret

    async def create_campaign(self, payload: dict) -> dict:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self._base_url}/v1/bulk-pr/campaigns",
                json=payload,
                headers={"X-Internal-Secret": self._secret},
            )
            resp.raise_for_status()
            return resp.json()
