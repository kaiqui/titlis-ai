import base64
import json
import time
from typing import Optional

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from src.utils.logger import get_logger

logger = get_logger(__name__)

_GITHUB_API = "https://api.github.com"
_HTTP_TIMEOUT = 10.0


def _generate_app_jwt(app_id: str, private_key_pem: str) -> str:
    header = base64.urlsafe_b64encode(json.dumps({"alg": "RS256", "typ": "JWT"}).encode()).rstrip(b"=")
    now = int(time.time())
    payload = base64.urlsafe_b64encode(json.dumps({"iss": app_id, "iat": now - 60, "exp": now + 600}).encode()).rstrip(
        b"="
    )
    signing_input = header + b"." + payload
    private_key = serialization.load_pem_private_key(private_key_pem.encode(), password=None)
    signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())  # type: ignore[union-attr, call-arg, arg-type]
    sig = base64.urlsafe_b64encode(signature).rstrip(b"=")
    return (signing_input + b"." + sig).decode()


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
        logger.info(
            "Installation ID descoberto automaticamente",
            extra={"app_id": app_id, "installation_id": installation_id},
        )
        return installation_id

    except Exception:
        logger.exception("Falha ao descobrir installation_id do GitHub App", extra={"app_id": app_id})
        return None
