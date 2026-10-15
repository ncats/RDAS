"""Run the shared Memgraph index initializer with the GARD environment."""

import runpy
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INDEX_INITIALIZATION_SCRIPT = (
    PROJECT_ROOT
    / "Z_Alert"
    / "pipelines"
    / "pipeline_0_setup"
    / "task_memgraph_index_initialization.py"
)


def main() -> None:

    """Run the shared Z_Alert index initializer using the repository .env."""

    if not INDEX_INITIALIZATION_SCRIPT.is_file():
        raise FileNotFoundError(f"Memgraph index initializer not found: {INDEX_INITIALIZATION_SCRIPT}")

    runpy.run_path(str(INDEX_INITIALIZATION_SCRIPT), run_name="__main__")


if __name__ == "__main__":
    main()
