import time

from src.pipeline.session import AgentSession, SessionStore


def _make_session(session_id: str = "sess-1", tenant_id: int = 1) -> AgentSession:
    return AgentSession(
        session_id=session_id,
        tenant_id=tenant_id,
        ai_config={"provider": "openai"},
    )


class TestAgentSession:
    def test_touch_updates_last_active(self):
        session = _make_session()
        before = session.last_active
        time.sleep(0.01)
        session.touch()
        assert session.last_active > before

    def test_append_audit_adds_entry_with_ts(self):
        session = _make_session()
        session.append_audit({"event": "user_message", "content": "hello"})
        assert len(session.audit_log) == 1
        assert session.audit_log[0]["event"] == "user_message"
        assert "ts" in session.audit_log[0]


class TestSessionStore:
    def test_creates_session_when_not_exists(self):
        store = SessionStore()
        session = store.get_or_create("sess-1", 1, {"provider": "openai"})
        assert session.session_id == "sess-1"
        assert session.tenant_id == 1

    def test_returns_existing_session(self):
        store = SessionStore()
        s1 = store.get_or_create("sess-1", 1, {"provider": "openai"})
        s1.messages.append({"role": "user", "content": "hi"})
        s2 = store.get_or_create("sess-1", 1, {"provider": "openai"})
        assert s1 is s2
        assert len(s2.messages) == 1

    def test_updates_ai_config_on_existing_session(self):
        store = SessionStore()
        store.get_or_create("sess-1", 1, {"provider": "openai", "model": "gpt-4o"})
        s = store.get_or_create("sess-1", 1, {"provider": "anthropic", "model": "claude-3-5"})
        assert s.ai_config["provider"] == "anthropic"

    def test_get_returns_none_for_unknown_session(self):
        store = SessionStore()
        assert store.get("nonexistent") is None

    def test_get_returns_existing_session(self):
        store = SessionStore()
        store.get_or_create("sess-1", 1, {"provider": "openai"})
        session = store.get("sess-1")
        assert session is not None
        assert session.session_id == "sess-1"

    def test_cleanup_removes_expired_sessions(self):
        store = SessionStore()
        session = store.get_or_create("sess-old", 1, {"provider": "openai"})
        session.last_active = time.time() - 4000
        store.get_or_create("sess-new", 2, {"provider": "openai"})
        store.cleanup_expired()
        assert store.get("sess-old") is None
        assert store.get("sess-new") is not None

    def test_cleanup_keeps_active_sessions(self):
        store = SessionStore()
        store.get_or_create("sess-active", 1, {"provider": "openai"})
        store.cleanup_expired()
        assert store.get("sess-active") is not None
