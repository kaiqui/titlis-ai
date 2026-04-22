from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from src.bootstrap.dependencies import get_knowledge_seeder
from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


class SeedRequest(BaseModel):
    provider: str
    api_key: str
    tenant_id: int | None = None


def _verify_internal_secret(request: Request) -> None:
    secret = request.headers.get("X-Internal-Secret", "")
    if secret != settings.internal_secret:
        raise HTTPException(status_code=403, detail="internal_secret_invalid")


@router.post("/knowledge/seed")
async def seed_knowledge(body: SeedRequest, request: Request) -> dict:
    _verify_internal_secret(request)
    seeder = get_knowledge_seeder()
    count = await seeder.seed_global_rules(
        provider=body.provider,
        api_key=body.api_key,
        tenant_id=body.tenant_id,
    )
    return {"seeded": count}
