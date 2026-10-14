"""Run the shared Memgraph index initializer with the GARD environment."""

import runpy
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]

ENV_FILE = PROJECT_ROOT / ".env2"

INDEX_INITIALIZATION_SCRIPT = (
    PROJECT_ROOT
    / "Z_Alert"
    / "pipelines"
    / "pipeline_0_setup"
    / "task_memgraph_index_initialization.py"
)


def main() -> None:

    """Load GARD settings, then run the shared Z_Alert index initializer."""

    if not ENV_FILE.is_file():
        raise FileNotFoundError(f"GARD environment file not found: {ENV_FILE}")

    if not INDEX_INITIALIZATION_SCRIPT.is_file():
        raise FileNotFoundError(f"Memgraph index initializer not found: {INDEX_INITIALIZATION_SCRIPT}")

    load_dotenv(ENV_FILE, override=True)
    runpy.run_path(str(INDEX_INITIALIZATION_SCRIPT), run_name="__main__")


if __name__ == "__main__":
    main()
