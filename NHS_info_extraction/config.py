"""Runtime configuration for the Clinical Abstract Extraction API.

All values are resolved from environment variables so the container can be
configured at deploy time without changing code. Every setting has a default
that matches the previous hard-coded behaviour, so the service keeps working
if nothing is provided.

Model configurations can be supplied in three ways (highest priority last):
  1. Built-in ``DEFAULT_MODEL_CONFIGS`` below.
  2. A JSON file pointed to by ``MODEL_CONFIG_PATH`` (merged onto the defaults).
  3. Per-setting environment overrides (paths and GPU/parallelism knobs).
"""

import json
import os
from copy import deepcopy
from typing import Any, Dict, Optional


def _get_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None and value.strip() != "" else default


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def _get_list(name: str, default: list) -> list:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return list(default)
    return [item.strip() for item in value.split(",") if item.strip()]


def _env_key(model_name: str, suffix: str) -> str:
    """Build an env var name from a model name, e.g. Llama-3.1-70B -> LLAMA_3_1_70B."""
    normalized = (
        model_name.upper()
        .replace("-", "_")
        .replace(".", "_")
        .replace(" ", "_")
    )
    return f"MODEL_{suffix}_{normalized}"


# Default model configurations (used when MODEL_CONFIG_PATH is not provided).
DEFAULT_MODEL_CONFIGS: Dict[str, Dict[str, Any]] = {
    "gemma3-27b": {
        "path": "/vast/projects/ncats-llms/gemma3-27b/",
        "tensor_parallel_size": 4,
        "gpu_memory_utilization": 0.90,
        "max_model_len": 3072,
        "temperature": 0.1,
        "max_tokens": 2048,
        "top_p": 0.95,
        "stop": ["<END_JSON>", "</s>"],
    },
    "Llama-3.1-70B-Instruct": {
        "path": "/vast/projects/ncats-llms/Llama-3.1-70B-Instruct/",
        "tensor_parallel_size": 4,
        "gpu_memory_utilization": 0.90,
        "max_model_len": 3072,
        "temperature": 0.1,
        "max_tokens": 2048,
        "top_p": 0.95,
        "stop": ["<|eot_id|>", "</s>", "<END_JSON>"],
    },
}


def load_model_configs() -> Dict[str, Dict[str, Any]]:
    """Resolve model configs from defaults, an optional JSON file, and env vars."""
    configs: Dict[str, Dict[str, Any]] = deepcopy(DEFAULT_MODEL_CONFIGS)

    # 1) Optional JSON file with full or partial model configs.
    config_path = os.getenv("MODEL_CONFIG_PATH")
    if config_path:
        with open(config_path, "r", encoding="utf-8") as handle:
            file_configs = json.load(handle)
        if not isinstance(file_configs, dict):
            raise ValueError(
                f"MODEL_CONFIG_PATH ({config_path}) must contain a JSON object "
                "mapping model names to their configuration."
            )
        for name, cfg in file_configs.items():
            if name in configs and isinstance(cfg, dict):
                configs[name].update(cfg)
            else:
                configs[name] = cfg

    # 2) Global GPU / parallelism overrides applied to every model when set.
    global_tp = os.getenv("TENSOR_PARALLEL_SIZE")
    global_gpu_mem = os.getenv("GPU_MEMORY_UTILIZATION")
    global_max_len = os.getenv("MAX_MODEL_LEN")

    # 3) Per-model overrides (path is the most deployment-specific value).
    for name, cfg in configs.items():
        if global_tp is not None and global_tp.strip() != "":
            cfg["tensor_parallel_size"] = int(global_tp)
        if global_gpu_mem is not None and global_gpu_mem.strip() != "":
            cfg["gpu_memory_utilization"] = float(global_gpu_mem)
        if global_max_len is not None and global_max_len.strip() != "":
            cfg["max_model_len"] = int(global_max_len)

        path_override = os.getenv(_env_key(name, "PATH"))
        if path_override:
            cfg["path"] = path_override

    return configs


# Model settings
MODEL_CONFIGS: Dict[str, Dict[str, Any]] = load_model_configs()
DEFAULT_MODEL: str = _get_str("DEFAULT_MODEL", "Llama-3.1-70B-Instruct")
BATCH_SIZE: int = _get_int("BATCH_SIZE", 10)

# Server settings
API_HOST: str = _get_str("API_HOST", "0.0.0.0")
API_PORT: int = _get_int("API_PORT", 8000)
API_WORKERS: int = _get_int("API_WORKERS", 1)

# CORS settings (comma-separated values; defaults are restrictive).
# Set CORS_ALLOW_ORIGINS explicitly to the trusted front-end origins.
CORS_ALLOW_ORIGINS: list = _get_list("CORS_ALLOW_ORIGINS", [])
CORS_ALLOW_CREDENTIALS: bool = _get_bool("CORS_ALLOW_CREDENTIALS", False)
CORS_ALLOW_METHODS: list = _get_list("CORS_ALLOW_METHODS", ["GET", "POST"])
CORS_ALLOW_HEADERS: list = _get_list("CORS_ALLOW_HEADERS", ["*"])

# Terminology enhancer settings
ENABLE_TERMINOLOGY_API: bool = _get_bool("ENABLE_TERMINOLOGY_API", True)
TERMINOLOGY_TIMEOUT: int = _get_int("TERMINOLOGY_TIMEOUT", 10)
TERMINOLOGY_VERBOSE: bool = _get_bool("TERMINOLOGY_VERBOSE", False)
TERMINOLOGY_PROXY_URL: Optional[str] = os.getenv("TERMINOLOGY_PROXY_URL") or None
