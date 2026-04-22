#!/usr/bin/env python3
"""
seed_rules.py — Idempotent knowledge seeder for context/rules/*.yaml

Reads all rule YAML files, generates embeddings via LiteLLM, and persists chunks
in the titlis-api knowledge base (POST /v1/internal/rag/chunks). Skips files whose
content hash hasn't changed since the last run.

Usage:
    poetry run python scripts/seed_rules.py [--force] [--dry-run] [--rule RES-001]

Env vars (required):
    GENAI_API_KEY             — API key for the embedding provider
    TITLIS_API_URL            — titlis-api base URL (e.g. http://localhost:18080)
    TITLIS_AI_INTERNAL_SECRET — shared secret for X-Internal-Secret header

Env vars (optional):
    SEED_PROVIDER    — LiteLLM embedding provider (default: gemini)
    SEED_EMBED_MODEL — embedding model (default: gemini/text-embedding-004)
    SEED_EMBED_DIMS  — expected embedding dimensions (default: 768)
    SEED_CONCURRENCY — parallel embedding calls (default: 1)
    SEED_STATE_FILE  — path to hash state file (default: context/rules/.seed_state.json)
    LOG_LEVEL        — DEBUG | INFO | WARNING (default: INFO)
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import litellm
import yaml

# ── paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
RULES_DIR = REPO_ROOT / "context" / "rules"
DEFAULT_STATE_FILE = RULES_DIR / ".seed_state.json"

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("seed_rules")

# ── config ───────────────────────────────────────────────────────────────────
PROVIDER = os.getenv("SEED_PROVIDER", "gemini")
EMBED_MODEL = os.getenv("SEED_EMBED_MODEL", "gemini/text-embedding-004")
EMBED_DIMS = int(os.getenv("SEED_EMBED_DIMS", "768"))
CONCURRENCY = int(os.getenv("SEED_CONCURRENCY", "1"))
STATE_FILE = Path(os.getenv("SEED_STATE_FILE", str(DEFAULT_STATE_FILE)))

API_URL = os.getenv("TITLIS_API_URL", "http://localhost:18080")
INTERNAL_SECRET = os.getenv("TITLIS_AI_INTERNAL_SECRET", "")
GENAI_API_KEY = os.getenv("GENAI_API_KEY", "")

SOURCE_TYPE = "global_rule_doc"

# ── required dimension check per provider ────────────────────────────────────
# titlis-api currently enforces VECTOR(1536). Gemini produces 768.
# Set SEED_SKIP_DIM_CHECK=true to bypass (useful when the schema is being migrated).
SKIP_DIM_CHECK = os.getenv("SEED_SKIP_DIM_CHECK", "false").lower() == "true"
API_REQUIRED_DIMS = 1536


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_state() -> dict[str, str]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            log.warning("State file corrupt — starting fresh: %s", STATE_FILE)
    return {}


def _save_state(state: dict[str, str]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True))


def _canonical_hash(data: dict) -> str:
    """SHA-256 of the canonical (sorted keys) JSON representation."""
    canonical = json.dumps(data, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _build_chunk_text(rule: dict) -> str:
    """Build the full text to embed from a rule's enriched fields."""
    lines = [
        f"Regra: {rule['rule_id']} — {rule['title']}",
        f"Pilar: {rule['pillar']}",
        f"Por que importa: {rule['why'].strip()}",
        f"Impacto: {rule.get('impact', '').strip()}",
        f"Como corrigir: {rule['fix_hint'].strip()}",
    ]
    if rule.get("tags"):
        lines.append(f"Tags: {', '.join(rule['tags'])}")
    if rule.get("related_rules"):
        lines.append(f"Regras relacionadas: {', '.join(rule['related_rules'])}")
    return "\n".join(l for l in lines if l)


def _load_rule_file(path: Path) -> dict | None:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            log.error("Invalid YAML structure in %s — expected mapping", path.name)
            return None
        required = {"rule_id", "title", "pillar", "why", "fix_hint"}
        missing = required - data.keys()
        if missing:
            log.error("Missing fields %s in %s", missing, path.name)
            return None
        return data
    except yaml.YAMLError as exc:
        log.error("YAML parse error in %s: %s", path.name, exc)
        return None


# ── embedding ─────────────────────────────────────────────────────────────────

async def _embed(text: str, retries: int = 3) -> list[float]:
    delay = 1.0
    for attempt in range(retries):
        try:
            resp = await litellm.aembedding(
                model=EMBED_MODEL,
                input=text,
                api_key=GENAI_API_KEY,
            )
            return resp.data[0]["embedding"]
        except Exception as exc:
            if attempt == retries - 1:
                raise
            wait = delay * (2 ** attempt)
            log.warning("Embedding attempt %d/%d failed: %s — retry in %.1fs", attempt + 1, retries, exc, wait)
            await asyncio.sleep(wait)
    raise RuntimeError("unreachable")


# ── API persistence ───────────────────────────────────────────────────────────

async def _persist_chunk(
    client: httpx.AsyncClient,
    rule: dict,
    embedding: list[float],
    content_hash: str,
) -> None:
    payload: dict[str, Any] = {
        "tenantId": None,
        "sourceType": SOURCE_TYPE,
        "sourceId": rule["rule_id"],
        "chunkText": _build_chunk_text(rule),
        "embedding": embedding,
        "metadata": json.dumps({
            "rule_id": rule["rule_id"],
            "title": rule["title"],
            "pillar": rule["pillar"],
            "severity": rule.get("severity", "medium"),
            "tags": rule.get("tags", []),
            "content_hash": content_hash,
        }),
    }
    resp = await client.post(
        f"{API_URL}/v1/internal/rag/chunks",
        json=payload,
        headers={"X-Internal-Secret": INTERNAL_SECRET},
        timeout=30.0,
    )
    resp.raise_for_status()


# ── core ──────────────────────────────────────────────────────────────────────

async def seed(
    rule_filter: str | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> int:
    _validate_env()

    rule_files = sorted(RULES_DIR.glob("*.yaml"))
    if not rule_files:
        log.error("No YAML files found in %s", RULES_DIR)
        return 1

    if rule_filter:
        rule_files = [f for f in rule_files if f.stem.upper() == rule_filter.upper()]
        if not rule_files:
            log.error("Rule %s not found in %s", rule_filter, RULES_DIR)
            return 1

    state = {} if force else _load_state()
    stats = {"processed": 0, "skipped": 0, "errors": 0}
    new_state = dict(state)

    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient() as http_client:
        for path in rule_files:
            rule = _load_rule_file(path)
            if rule is None:
                stats["errors"] += 1
                continue

            rule_id = rule["rule_id"]
            content_hash = _canonical_hash(rule)

            if not force and state.get(rule_id) == content_hash:
                log.debug("%-10s  SKIP  (hash unchanged)", rule_id)
                stats["skipped"] += 1
                continue

            if dry_run:
                log.info("%-10s  DRY   would seed (hash=%s)", rule_id, content_hash[:12])
                stats["processed"] += 1
                continue

            log.info("%-10s  SEED  generating embedding...", rule_id)
            t0 = time.monotonic()

            async with sem:
                try:
                    embedding = await _embed(_build_chunk_text(rule))

                    if len(embedding) != EMBED_DIMS:
                        log.warning(
                            "%-10s  WARN  embedding has %d dims (expected %d)",
                            rule_id, len(embedding), EMBED_DIMS,
                        )

                    if not SKIP_DIM_CHECK and len(embedding) != API_REQUIRED_DIMS:
                        log.error(
                            "%-10s  SKIP  API requires %d dims but got %d. "
                            "Set SEED_SKIP_DIM_CHECK=true or migrate schema. "
                            "Skipping to avoid API error.",
                            rule_id, API_REQUIRED_DIMS, len(embedding),
                        )
                        stats["errors"] += 1
                        continue

                    if SKIP_DIM_CHECK and len(embedding) != API_REQUIRED_DIMS:
                        padding = [0.0] * (API_REQUIRED_DIMS - len(embedding))
                        embedding = embedding + padding
                        log.debug("%-10s  PAD   embedding padded %d→%d dims", rule_id, EMBED_DIMS, API_REQUIRED_DIMS)

                    await _persist_chunk(http_client, rule, embedding, content_hash)
                    elapsed = time.monotonic() - t0
                    log.info("%-10s  OK    %.1fs  hash=%s", rule_id, elapsed, content_hash[:12])
                    new_state[rule_id] = content_hash
                    stats["processed"] += 1

                except httpx.HTTPStatusError as exc:
                    log.error("%-10s  FAIL  API %d: %s", rule_id, exc.response.status_code, exc.response.text[:200])
                    stats["errors"] += 1
                except Exception as exc:
                    log.error("%-10s  FAIL  %s", rule_id, exc)
                    stats["errors"] += 1

    if not dry_run and new_state != state:
        _save_state(new_state)

    log.info(
        "Done — processed=%d  skipped=%d  errors=%d",
        stats["processed"], stats["skipped"], stats["errors"],
    )
    return 1 if stats["errors"] > 0 else 0


def _validate_env() -> None:
    missing = []
    if not GENAI_API_KEY:
        missing.append("GENAI_API_KEY")
    if not INTERNAL_SECRET:
        missing.append("TITLIS_AI_INTERNAL_SECRET")
    if missing:
        log.error("Missing required env vars: %s", ", ".join(missing))
        sys.exit(1)
    if not RULES_DIR.is_dir():
        log.error("Rules directory not found: %s", RULES_DIR)
        sys.exit(1)
    log.debug("Config: provider=%s model=%s api=%s", PROVIDER, EMBED_MODEL, API_URL)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Idempotent seeder for context/rules/*.yaml → titlis-api RAG"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-seed all rules regardless of content hash (ignore state file)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be seeded without calling any API",
    )
    parser.add_argument(
        "--rule",
        metavar="RULE_ID",
        help="Seed only this specific rule (e.g. RES-001)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    exit_code = asyncio.run(
        seed(
            rule_filter=args.rule,
            force=args.force,
            dry_run=args.dry_run,
        )
    )
    sys.exit(exit_code)
