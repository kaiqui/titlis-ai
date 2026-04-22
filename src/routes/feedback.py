from collections import defaultdict
from typing import Dict

from fastapi import APIRouter, HTTPException, Request

from src.bootstrap.dependencies import get_scorecard_client
from src.domain.models import FeedbackRequest
from src.observability.metrics import (
    ai_feedback_alerts_total,
    ai_user_feedback_total,
)
from src.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()

_ALERT_THRESHOLD = 0.30
_MIN_SAMPLES_FOR_ALERT = 5

# In-memory per-rule sentiment counts (resets on process restart)
_sentiment_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {"positive": 0, "negative": 0})


def _verify_internal_secret(request: Request) -> None:
    secret = request.headers.get("X-Internal-Secret", "")
    if secret != settings.internal_secret:
        raise HTTPException(status_code=403, detail="internal_secret_invalid")


def _check_negative_rate(rule_id: str) -> None:
    counts = _sentiment_counts[rule_id]
    total = counts["positive"] + counts["negative"]
    if total < _MIN_SAMPLES_FOR_ALERT:
        return
    rate = counts["negative"] / total
    if rate > _ALERT_THRESHOLD:
        logger.warning(
            "Alta taxa de feedback negativo para regra — revisar configuração",
            extra={
                "rule_id": rule_id,
                "negative_rate_pct": round(rate * 100, 1),
                "total_samples": total,
            },
        )
        ai_feedback_alerts_total.labels(rule_id=rule_id).inc()


@router.post("/feedback")
async def submit_feedback(body: FeedbackRequest, request: Request) -> dict:
    _verify_internal_secret(request)

    if body.sentiment not in ("positive", "negative"):
        raise HTTPException(status_code=422, detail="sentiment deve ser 'positive' ou 'negative'")

    # Prometheus counter
    ai_user_feedback_total.labels(
        rule_id=body.rule_id,
        provider=body.provider or "unknown",
        sentiment=body.sentiment,
    ).inc()

    # In-memory rate tracking
    _sentiment_counts[body.rule_id][body.sentiment] += 1
    _check_negative_rate(body.rule_id)

    # Persist via titlis-api (best-effort)
    try:
        client = get_scorecard_client()
        await client.store_feedback(
            tenant_id=body.tenant_id,
            response_id=body.response_id,
            rule_id=body.rule_id,
            sentiment=body.sentiment,
            comment=body.comment,
        )
    except Exception:
        logger.warning(
            "Falha ao persistir feedback no titlis-api",
            extra={"tenant_id": body.tenant_id, "rule_id": body.rule_id},
        )

    return {"status": "ok"}
