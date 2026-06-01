import pytest

from src.tools.github_tools import (
    _never_reduce_violated,
    _extract_container_resources,
    _check_never_reduce,
    _parse_repo,
    _extract_any,
)


DEPLOYMENT_YAML = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: payment-api
spec:
  template:
    spec:
      containers:
      - name: app
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
          limits:
            cpu: 200m
            memory: 256Mi
"""


class TestNeverReduceValidation:
    def test_cpu_reduction_detected(self):
        assert _never_reduce_violated("200m", "100m") is True

    def test_cpu_increase_allowed(self):
        assert _never_reduce_violated("100m", "200m") is False

    def test_memory_reduction_detected(self):
        assert _never_reduce_violated("256Mi", "128Mi") is True

    def test_memory_increase_allowed(self):
        assert _never_reduce_violated("128Mi", "256Mi") is False

    def test_empty_value_safe(self):
        assert _never_reduce_violated("", "100m") is False
        assert _never_reduce_violated("100m", "") is False

    def test_invalid_value_safe(self):
        assert _never_reduce_violated("not-a-resource", "100m") is False

    def test_gi_memory(self):
        assert _never_reduce_violated("2Gi", "1Gi") is True
        assert _never_reduce_violated("1Gi", "2Gi") is False

    def test_memory_bytes_no_unit(self):
        assert _never_reduce_violated("134217728", "67108864") is True


class TestExtractContainerResources:
    def test_extracts_requests_and_limits(self):
        resources = _extract_container_resources(DEPLOYMENT_YAML)
        assert resources[("requests", "cpu")] == "100m"
        assert resources[("requests", "memory")] == "128Mi"
        assert resources[("limits", "cpu")] == "200m"
        assert resources[("limits", "memory")] == "256Mi"

    def test_returns_empty_for_non_deployment(self):
        yaml_text = "apiVersion: v1\nkind: Service\n"
        resources = _extract_container_resources(yaml_text)
        assert resources == {}

    def test_returns_empty_for_invalid_yaml(self):
        resources = _extract_container_resources("not: [valid yaml")
        assert resources == {}

    def test_returns_empty_for_deployment_without_resources(self):
        yaml_text = """
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
      - name: app
"""
        resources = _extract_container_resources(yaml_text)
        assert resources == {}


class TestCheckNeverReduce:
    def test_no_violation_returns_none(self):
        patched = DEPLOYMENT_YAML.replace("cpu: 100m", "cpu: 200m")
        result = _check_never_reduce(DEPLOYMENT_YAML, patched)
        assert result is None

    def test_cpu_reduction_returns_message(self):
        patched = DEPLOYMENT_YAML.replace("cpu: 100m", "cpu: 50m")
        result = _check_never_reduce(DEPLOYMENT_YAML, patched)
        assert result is not None
        assert "never-reduce" in result
        assert "cpu" in result

    def test_memory_reduction_returns_message(self):
        patched = DEPLOYMENT_YAML.replace("memory: 128Mi", "memory: 64Mi")
        result = _check_never_reduce(DEPLOYMENT_YAML, patched)
        assert result is not None
        assert "memory" in result

    def test_returns_none_when_current_empty(self):
        result = _check_never_reduce("", DEPLOYMENT_YAML)
        assert result is None


class TestParseRepo:
    def test_parses_https_url(self):
        owner, name = _parse_repo("https://github.com/myorg/myrepo")
        assert owner == "myorg"
        assert name == "myrepo"

    def test_strips_trailing_slash(self):
        owner, name = _parse_repo("https://github.com/myorg/myrepo/")
        assert owner == "myorg"
        assert name == "myrepo"

    def test_raises_on_invalid_url(self):
        with pytest.raises(ValueError, match="repo_url inválido"):
            _parse_repo("https://github.com/onlyone")


class TestExtractAny:
    def test_returns_string_for_value(self):
        assert _extract_any(42) == "42"
        assert _extract_any("hello") == "hello"

    def test_returns_empty_for_none(self):
        assert _extract_any(None) == ""
