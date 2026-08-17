import os
import sys

import pytest

# Add the project root to the Python path, matching the existing test modules.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Z_Alert')))

from utils.organization_name_extractor import OrganizationNameExtractor
from utils.tools import _make_hash_key


DEFAULT_OLLAMA_MODEL = "llama3.1:latest"
DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434"


class RecordingLogger:
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.infos = []

    def error(self, message: str) -> None:
        self.errors.append(message)

    def warning(self, message: str) -> None:
        self.warnings.append(message)

    def info(self, message: str) -> None:
        self.infos.append(message)


class FakeTaskExtractor:
    def __init__(self):
        self.shutdown_model_called = False
        self.stop_model_server_called = False
        self.close_called = False

    def shutdown_model(self) -> None:
        self.shutdown_model_called = True

    def stop_model_server(self) -> None:
        self.stop_model_server_called = True

    def close(self) -> None:
        self.close_called = True


@pytest.fixture(scope="module")
def ollama_extractor():
    # These defaults make the test run against the same local Ollama API used
    # by the pipeline, while still allowing a developer to override the model
    # name, timeout, or endpoint with environment variables.
    os.environ.setdefault("ORG_NAME_EXTRACT_MODEL", DEFAULT_OLLAMA_MODEL)
    os.environ.setdefault("ORG_NAME_EXTRACT_BASE_URL", DEFAULT_OLLAMA_BASE_URL)
    os.environ.setdefault("ORG_NAME_EXTRACT_TIMEOUT_SECONDS", "300")
    os.environ.setdefault("ORG_NAME_EXTRACT_MAX_LENGTH", "200")
    os.environ.setdefault("MODEL_START_COMMAND", "ollama serve")

    logger = RecordingLogger()
    extractor = OrganizationNameExtractor(logger=logger)

    assert extractor.check_model_server_running(), (
        "Ollama model server is not running. Confirm Ollama is installed, "
        "MODEL_START_COMMAND can start it, and ORG_NAME_EXTRACT_BASE_URL points to it."
    )
    extractor.test_logger = logger

    response = extractor.session.get(f"{extractor.model_api_base_url}/api/tags", timeout=5)
    response.raise_for_status()
    available_models = response.json().get("models", [])
    available_model_names = {
        model_value
        for model in available_models
        for model_value in (model.get("name"), model.get("model"))
        if model_value
    }

    assert extractor.model_name in available_model_names, (
        f"Ollama is running, but model {extractor.model_name!r} is not installed. "
        f"Available models: {sorted(available_model_names)}"
    )

    yield extractor

    try:
        extractor.shutdown_model()
        extractor.stop_model_server()
    finally:
        extractor.close()


def test_organization_location_ror_lookup_task_close_shuts_down_model():
    pytest.importorskip("mysql.connector")

    from pipelines.pipeline_7_graph_maintenance.task_pipeline_maintenance_1 import OrganizationLocationRorLookupTask

    task = OrganizationLocationRorLookupTask.__new__(OrganizationLocationRorLookupTask)
    task.mysql = None
    task.memgraph = None
    task.logger = None
    task.org_name_extractor = FakeTaskExtractor()
    fake_extractor = task.org_name_extractor

    task.close()

    assert fake_extractor.shutdown_model_called
    assert fake_extractor.stop_model_server_called
    assert fake_extractor.close_called
    assert task.org_name_extractor is None


@pytest.fixture
def local_extractor():
    # Helper-only tests use a configured instance without starting Ollama. The
    # live extraction test above still exercises the real constructor and model.
    extractor = OrganizationNameExtractor.__new__(OrganizationNameExtractor)
    extractor.logger = None
    extractor.extracted_name_max_length = 200
    return extractor


def test_extract_organization_name_calls_ollama_model(ollama_extractor):
    original_name = "Department of Neurology, Mayo Clinic, 200 First Street SW, Rochester, Minnesota 55905"

    extracted_name = ollama_extractor.extract_organization_name(original_name)

    assert extracted_name is not None, "\n".join(ollama_extractor.test_logger.errors)
    assert extracted_name.lower() == "mayo clinic"


@pytest.mark.parametrize("response_text, expected", [
    ("Organization Name: \"Mayo Clinic.\"", "Mayo Clinic"),
    ("```text\nInstitution: 'National Institutes of Health.'\n```", "National Institutes of Health"),
    ("- University of Pennsylvania -", "University of Pennsylvania"),
    ("N/A", ""),
    ("no organization name", ""),
    (None, ""),
])
def test_clean_model_response_removes_common_model_formatting(local_extractor, response_text, expected):
    assert local_extractor.clean_model_response(response_text) == expected


def test_clean_model_response_uses_first_non_empty_line(local_extractor):
    response_text = "\n\nStanford University\nSchool of Medicine\nPalo Alto, CA"

    assert local_extractor.clean_model_response(response_text) == "Stanford University"


def test_normalize_extracted_name_collapses_whitespace_and_limits_length(local_extractor):
    local_extractor.extracted_name_max_length = 12

    assert local_extractor.normalize_extracted_name("  Johns   Hopkins   University  ") == "Johns Hopkin"


def test_normalize_extracted_name_returns_empty_string_for_blank_values(local_extractor):
    assert local_extractor.normalize_extracted_name(None) == ""
    assert local_extractor.normalize_extracted_name("") == ""


def test_make_extracted_name_hash_key_uses_normalized_name(local_extractor):
    extracted_name = "  Johns   Hopkins   University  "

    assert local_extractor.make_extracted_name_hash_key(extracted_name) == _make_hash_key("Johns Hopkins University")


def test_make_extracted_name_hash_key_returns_none_for_empty_name(local_extractor):
    assert local_extractor.make_extracted_name_hash_key("   ") is None


def test_build_prompt_includes_original_text_and_strict_output_rules(local_extractor):
    prompt = local_extractor.build_prompt("Division of Genetics, Boston Children's Hospital, Boston, MA")

    assert "Division of Genetics, Boston Children's Hospital, Boston, MA" in prompt
    assert "Return only one clean organization name." in prompt
    assert "If no organization name can be identified, return an empty string." in prompt


def test_parse_positive_int_accepts_positive_integer_strings(local_extractor):
    assert local_extractor._parse_positive_int("30", "ORG_NAME_EXTRACT_TIMEOUT_SECONDS") == 30


def test_parse_positive_int_rejects_missing_or_non_positive_values(local_extractor):
    with pytest.raises(ValueError, match="ORG_NAME_EXTRACT_MAX_LENGTH must be a positive integer"):
        local_extractor._parse_positive_int("0", "ORG_NAME_EXTRACT_MAX_LENGTH")

    with pytest.raises(ValueError, match="ORG_NAME_EXTRACT_MAX_LENGTH must be a positive integer"):
        local_extractor._parse_positive_int(None, "ORG_NAME_EXTRACT_MAX_LENGTH")


def test_required_env_returns_stripped_value(local_extractor, monkeypatch):
    monkeypatch.setenv("ORG_NAME_EXTRACT_MODEL", "  qwen2.5  ")

    assert local_extractor._required_env("ORG_NAME_EXTRACT_MODEL") == "qwen2.5"


def test_required_env_rejects_missing_value(local_extractor, monkeypatch):
    monkeypatch.delenv("ORG_NAME_EXTRACT_MODEL", raising=False)

    with pytest.raises(ValueError, match="Missing required environment variable: ORG_NAME_EXTRACT_MODEL"):
        local_extractor._required_env("ORG_NAME_EXTRACT_MODEL")
