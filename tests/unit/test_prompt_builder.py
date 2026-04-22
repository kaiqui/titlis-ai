import pytest
from src.domain.models import FindingContext
from src.services.prompt_builder import PromptBuilder, SYSTEM_PROMPT


def _finding(rule_id: str, actual: str = None, expected: str = None) -> FindingContext:
    return FindingContext(
        rule_id=rule_id,
        pillar="resilience",
        severity="error",
        actual_value=actual,
        expected_value=expected,
        deployment_name="payment-api",
        namespace="production",
    )


class TestPromptBuilderMessages:
    def test_messages_have_system_and_user(self) -> None:
        builder = PromptBuilder()
        msgs = builder.build_explain_messages(_finding("RES-001"))
        assert len(msgs) == 2
        assert msgs[0]["role"] == "system"
        assert msgs[1]["role"] == "user"

    def test_system_prompt_contains_titlis_context(self) -> None:
        assert "Titlis" in SYSTEM_PROMPT
        assert "SRE" in SYSTEM_PROMPT
        assert "Kubernetes" in SYSTEM_PROMPT

    def test_user_message_contains_rule_id(self) -> None:
        builder = PromptBuilder()
        msgs = builder.build_explain_messages(_finding("RES-003"))
        assert "RES-003" in msgs[1]["content"]

    def test_user_message_contains_deployment_name(self) -> None:
        builder = PromptBuilder()
        msgs = builder.build_explain_messages(_finding("SEC-001"))
        assert "payment-api" in msgs[1]["content"]
        assert "production" in msgs[1]["content"]

    def test_user_message_shows_actual_value(self) -> None:
        builder = PromptBuilder()
        msgs = builder.build_explain_messages(_finding("RES-003", actual="50m", expected="100m"))
        assert "50m" in msgs[1]["content"]
        assert "100m" in msgs[1]["content"]

    def test_user_message_shows_not_configured_when_actual_is_none(self) -> None:
        builder = PromptBuilder()
        msgs = builder.build_explain_messages(_finding("RES-001"))
        assert "não configurado" in msgs[1]["content"]

    def test_user_message_asks_for_portuguese_response(self) -> None:
        builder = PromptBuilder()
        msgs = builder.build_explain_messages(_finding("RES-001"))
        assert "português" in msgs[1]["content"]


class TestPromptBuilderAllRules:
    @pytest.mark.parametrize(
        "rule_id",
        [
            "RES-001",
            "RES-002",
            "RES-003",
            "RES-004",
            "RES-005",
            "RES-006",
            "RES-007",
            "RES-008",
            "RES-009",
            "RES-010",
            "RES-011",
            "RES-012",
            "RES-013",
            "RES-014",
            "RES-015",
            "RES-016",
            "RES-017",
            "RES-018",
            "RES-019",
            "SEC-001",
            "SEC-002",
            "SEC-003",
            "SEC-004",
            "PERF-001",
            "PERF-002",
            "PERF-003",
            "OPS-001",
        ],
    )
    def test_known_rule_produces_specific_title(self, rule_id: str) -> None:
        builder = PromptBuilder()
        msgs = builder.build_explain_messages(_finding(rule_id))
        assert rule_id in msgs[1]["content"]
        assert len(msgs[1]["content"]) > 200

    def test_unknown_rule_falls_back_to_generic(self) -> None:
        builder = PromptBuilder()
        msgs = builder.build_explain_messages(_finding("UNKNOWN-999"))
        assert "UNKNOWN-999" in msgs[1]["content"]
        assert "compliance" in msgs[1]["content"].lower()

    def test_get_rule_title_known(self) -> None:
        builder = PromptBuilder()
        assert "Liveness" in builder.get_rule_title("RES-001")

    def test_get_rule_title_unknown_fallback(self) -> None:
        builder = PromptBuilder()
        assert "compliance" in builder.get_rule_title("UNKNOWN-000").lower()


class TestPromptBuilderWithChunks:
    def test_chunks_injected_into_user_message(self) -> None:
        builder = PromptBuilder()
        chunks = [{"chunkText": "Liveness probe deve usar httpGet para /healthz"}]
        msgs = builder.build_explain_messages(_finding("RES-001"), chunks=chunks)
        assert "Liveness probe" in msgs[1]["content"]
        assert "base de conhecimento" in msgs[1]["content"]

    def test_multiple_chunks_all_appear(self) -> None:
        builder = PromptBuilder()
        chunks = [
            {"chunkText": "Chunk A sobre livenessProbe"},
            {"chunkText": "Chunk B sobre readinessProbe"},
        ]
        msgs = builder.build_explain_messages(_finding("RES-001"), chunks=chunks)
        assert "Chunk A" in msgs[1]["content"]
        assert "Chunk B" in msgs[1]["content"]

    def test_no_chunks_produces_same_structure(self) -> None:
        builder = PromptBuilder()
        msgs_no_chunks = builder.build_explain_messages(_finding("RES-001"))
        msgs_empty = builder.build_explain_messages(_finding("RES-001"), chunks=[])
        assert msgs_no_chunks[1]["content"] == msgs_empty[1]["content"]

    def test_none_chunks_produces_same_structure_as_no_arg(self) -> None:
        builder = PromptBuilder()
        msgs_none = builder.build_explain_messages(_finding("RES-001"), chunks=None)
        msgs_no_arg = builder.build_explain_messages(_finding("RES-001"))
        assert msgs_none[1]["content"] == msgs_no_arg[1]["content"]
