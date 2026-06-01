import time
from typing import Optional

import httpx
import jwt

from src.utils.logger import get_logger

logger = get_logger(__name__)

_GITHUB_API = "https://api.github.com"
_JWT_EXPIRY_SECONDS = 600
_JWT_CLOCK_DRIFT_SECONDS = 60
_HTTP_TIMEOUT = 10.0


def _generate_app_jwt(app_id: str, private_key_pem: str) -> str:
    now = int(time.time())
    payload = {
        "iss": app_id,
        "iat": now - _JWT_CLOCK_DRIFT_SECONDS,
        "exp": now + _JWT_EXPIRY_SECONDS,
    }
    return jwt.encode(payload, private_key_pem, algorithm="RS256")


async def resolve_installation_id(app_id: str, private_key_pem: str) -> Optional[str]:
    try:
        app_jwt = _generate_app_jwt(app_id, private_key_pem)
        headers = {
            "Authorization": f"Bearer {app_jwt}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            resp = await client.get(f"{_GITHUB_API}/app/installations", headers=headers)
            resp.raise_for_status()
            installations = resp.json()

        if not installations:
            logger.warning("GitHub App não está instalado em nenhuma conta", extra={"app_id": app_id})
            return None

        if len(installations) > 1:
            logger.warning(
                "GitHub App tem múltiplas instalações — usando a primeira",
                extra={"app_id": app_id, "count": len(installations)},
            )

        installation_id = str(installations[0]["id"])
        logger.info("Installation ID descoberto automaticamente", extra={"app_id": app_id, "installation_id": installation_id})
        return installation_id

    except Exception:
        logger.exception("Falha ao descobrir installation_id do GitHub App", extra={"app_id": app_id})
        return None
