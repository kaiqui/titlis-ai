import re
from typing import Any, Dict, Optional, Tuple

import yaml

_RESOURCE_RE = re.compile(r"(\d+(?:\.\d+)?)(m|Mi|Gi|Ki|Ti|Pi|)?$")
_MEM_UNITS = {"Mi": 1, "Gi": 1024, "Ki": 1 / 1024, "Ti": 1024 * 1024, "Pi": 1024 * 1024 * 1024}


def _parse_cpu_millicores(value: str) -> float:
    m = _RESOURCE_RE.match(value.strip())
    if not m:
        return 0.0
    num, unit = float(m.group(1)), m.group(2) or ""
    return num if unit == "m" else num * 1000


def _parse_mem_mebibytes(value: str) -> float:
    m = _RESOURCE_RE.match(value.strip())
    if not m:
        return 0.0
    num, unit = float(m.group(1)), m.group(2) or ""
    if not unit:
        return num / (1024 * 1024)
    return num * _MEM_UNITS.get(unit, 1)


def _is_cpu(value: str) -> bool:
    return value.strip().endswith("m") or value.strip().replace(".", "").isdigit()


def _never_reduce_violated(current: str, suggested: str) -> bool:
    if not current or not suggested:
        return False
    try:
        if _is_cpu(current):
            return _parse_cpu_millicores(suggested) < _parse_cpu_millicores(current)
        return _parse_mem_mebibytes(suggested) < _parse_mem_mebibytes(current)
    except Exception:
        return False


def _extract_container_resources(yaml_text: str) -> Dict[Tuple[str, str], str]:
    result: Dict[Tuple[str, str], str] = {}
    try:
        for doc in yaml.safe_load_all(yaml_text):
            if not isinstance(doc, dict) or doc.get("kind") != "Deployment":
                continue
            containers = (doc.get("spec") or {}).get("template", {}).get("spec", {}).get("containers") or []
            for container in containers:
                resources = (container or {}).get("resources") or {}
                for section in ("requests", "limits"):
                    for key, val in (resources.get(section) or {}).items():
                        result[(section, key)] = str(val)
    except Exception:
        pass
    return result


def _check_never_reduce(current_yaml: str, patched_yaml: str) -> Optional[str]:
    current_res = _extract_container_resources(current_yaml)
    patched_res = _extract_container_resources(patched_yaml)
    for (section, key), patched_val in patched_res.items():
        current_val = current_res.get((section, key))
        if current_val and _never_reduce_violated(current_val, patched_val):
            return f"never-reduce violado: tentou reduzir '{section}.{key}' de {current_val} para {patched_val}"
    return None


def _parse_repo(repo_url: str) -> Tuple[str, str]:
    clean = repo_url.rstrip("/").removeprefix("https://github.com/").removeprefix("http://github.com/")
    parts = clean.split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"repo_url inválido: {repo_url}")
    return parts[0], parts[1]


def _extract_any(value: Any) -> str:
    return str(value) if value is not None else ""
