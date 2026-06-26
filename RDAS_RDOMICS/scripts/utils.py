from __future__ import annotations

import os
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "paths.yaml"


def load_config(config_path: str | None = None) -> dict:
    """Load the full YAML configuration."""
    selected_config = config_path or os.environ.get("RDAS_RDOMICS_CONFIG")
    config_file = Path(selected_config).expanduser() if selected_config else DEFAULT_CONFIG_PATH

    if not config_file.exists():
        raise FileNotFoundError(f"Path config not found: {config_file}")

    with config_file.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def resolve_repo_path(path_value: str | os.PathLike[str]) -> str:
    """Resolve a config path against the repo root unless already absolute."""
    candidate = Path(path_value).expanduser()
    if not candidate.is_absolute():
        candidate = REPO_ROOT / candidate
    return str(candidate)


def load_paths(config_path: str | None = None) -> dict[str, str]:
    """Load configured paths and resolve relative entries from the repo root."""
    config = load_config(config_path)

    raw_paths = config.get("paths", config)
    resolved_paths: dict[str, str] = {}

    for key, value in raw_paths.items():
        resolved_paths[key] = resolve_repo_path(value)

    return resolved_paths


def load_settings(config_path: str | None = None) -> dict:
    """Load non-path settings from the YAML configuration."""
    config = load_config(config_path)
    return config.get("settings", {})


def ensure_parent_dir(path_value: str | os.PathLike[str]) -> None:
    """Create the parent directory for a file path if needed."""
    Path(path_value).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
