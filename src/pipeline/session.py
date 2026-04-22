import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.domain.models import ToolProposal


@dataclass
class AgentSession:
    session_id: str
    tenant_id: int
    ai_config: Dict[str, Any]
    messages: List[Dict[str, Any]] = field(default_factory=list)
    pending_proposals: List[ToolProposal] = field(default_factory=list)
    audit_log: List[Dict[str, Any]] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)

    def touch(self) -> None:
        self.last_active = time.time()

    def append_audit(self, entry: Dict[str, Any]) -> None:
        self.audit_log.append({"ts": time.time(), **entry})


_TTL_SECONDS = 3600


class SessionStore:
    def __init__(self) -> None:
        self._sessions: Dict[str, AgentSession] = {}

    def get_or_create(self, session_id: str, tenant_id: int, ai_config: Dict[str, Any]) -> AgentSession:
        if session_id not in self._sessions:
            self._sessions[session_id] = AgentSession(
                session_id=session_id,
                tenant_id=tenant_id,
                ai_config=ai_config,
            )
        session = self._sessions[session_id]
        session.touch()
        return session

    def get(self, session_id: str) -> Optional[AgentSession]:
        session = self._sessions.get(session_id)
        if session:
            session.touch()
        return session

    def cleanup_expired(self) -> None:
        now = time.time()
        expired = [sid for sid, s in self._sessions.items() if now - s.last_active > _TTL_SECONDS]
        for sid in expired:
            del self._sessions[sid]
