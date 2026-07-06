import glob
import os
import re
import shlex
import shutil
import time
import requests
import subprocess
from typing import Any, List, Optional
from urllib.parse import urlparse
from utils.tools import _make_hash_key

"""
Shared helper for extracting clean organization names with a local model API.

Two organization-location flows need the same behavior:
1. Build a strict prompt that asks the model for only one organization name.
2. Call the configured model generation endpoint.
3. Clean common formatting artifacts from the model response.
4. Normalize the extracted name to fit organization_location.model_extracted_name.
5. Generate the deterministic hash saved in model_extracted_name_hash_key.
"""

class OrganizationNameExtractor:

    def __init__(self, logger: Any = None):
        """Read organization-name extraction settings from environment."""
        self.logger = logger

        self.model_name = self._required_env("ORG_NAME_EXTRACT_MODEL")
        self.model_api_base_url = self._required_env("ORG_NAME_EXTRACT_BASE_URL").rstrip("/")
        self.request_timeout = self._parse_timeout(os.getenv("ORG_NAME_EXTRACT_TIMEOUT_SECONDS"))
        self.extracted_name_max_length = self._parse_positive_int(os.getenv("ORG_NAME_EXTRACT_MAX_LENGTH"), "ORG_NAME_EXTRACT_MAX_LENGTH",)

        self._model_server_checked = False
        self._model_server_is_running = False
        self._model_server_start_attempted = False
        self._model_server_process = None
        self.session = requests.Session()
        self.check_model_server_running()


    def extract_organization_name(self, original_name: str) -> Optional[str]:
        """
        Ask the configured model for one clean organization name.

        Returns:
            str: The cleaned model response. This can be an empty string when
                the model says there is no organization name.
            None: The model request or response parsing failed. Callers use
                None as a retryable extraction failure.
        """
        if not self._model_server_is_running:
            return None

        prompt = self.build_prompt(original_name)
        url = f"{self.model_api_base_url}/api/generate"

        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0,
            },
        }

        try:
            response = self.session.post(url, json=payload, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()

            return self.clean_model_response(data.get("response", ""))

        except requests.RequestException as exc:
            self._log_error(f"Model request failed for original_name={str(original_name)[:120]}: {exc}")
            return None

        except ValueError as exc:
            self._log_error(f"Model response was not valid JSON for original_name={str(original_name)[:120]}: {exc}")
            return None


    def check_model_server_running(self) -> bool:
        """
        Check whether the configured model server is reachable.

        The check is cached for this extractor instance so a down model server
        does not trigger one extra health request for every organization row.
        """
        if self._model_server_checked:
            return self._model_server_is_running

        if self._request_model_server_health():
            self._model_server_is_running = True
            self._model_server_checked = True
            return True

        self._log_warning(f"Model server is not reachable at {self.model_api_base_url}; attempting to start it.")

        if self.start_model_server():
            self._model_server_is_running = self.wait_for_model_server_start()
        else:
            self._model_server_is_running = False

        self._model_server_checked = True

        if not self._model_server_is_running:
            self._log_error(f"Model server is not running or not reachable at {self.model_api_base_url}.")

        return self._model_server_is_running


    def start_model_server(self) -> bool:
        """
        Start the local model server with MODEL_START_COMMAND.
        Example:
            MODEL_START_COMMAND=ollama serve
        """
        if self._model_server_start_attempted:
            return self._model_server_process is not None

        self._model_server_start_attempted = True

        if not self._is_local_model_api():
            self._log_error(f"Auto-start is only supported for local model endpoints. Configured endpoint: {self.model_api_base_url}")
            return False

        start_command = self._required_env("MODEL_START_COMMAND")

        try:
            start_command_args = self.build_model_start_command_args(start_command)
            self._model_server_process = subprocess.Popen(
                start_command_args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            self._log_info(f"Started model server with: {' '.join(start_command_args)}")

            return True

        except (OSError, ValueError) as exc:
            self._model_server_process = None
            self._log_error(f"Failed to start model server with MODEL_START_COMMAND={start_command}: {exc}")
            return False


    def build_model_start_command_args(self, start_command: str) -> List[str]:
        """
        Parse MODEL_START_COMMAND and resolve a bare executable name when the
        pipeline process does not inherit the user's interactive shell PATH.
        """

        command_args = shlex.split(start_command)

        if not command_args:
            raise ValueError("MODEL_START_COMMAND must include an executable.")

        executable = command_args[0]

        if os.path.dirname(executable):
            return command_args

        resolved_executable = shutil.which(executable)

        if resolved_executable:
            return [resolved_executable] + command_args[1:]

        if executable == "ollama":
            resolved_executable = self.find_ollama_executable()

            if resolved_executable:
                self._log_info(
                    f"Resolved MODEL_START_COMMAND executable 'ollama' to {resolved_executable}. "
                    "Consider setting MODEL_START_COMMAND to the absolute path in the pipeline environment."
                )
                return [resolved_executable] + command_args[1:]

        return command_args


    def find_ollama_executable(self) -> Optional[str]:
        """Find Ollama in common macOS install locations when it is not on PATH."""

        candidate_paths = [
            "/opt/homebrew/bin/ollama",
            "/usr/local/bin/ollama",
            "/Applications/Ollama.app/Contents/Resources/ollama",
        ]
        candidate_paths.extend(sorted(glob.glob("/opt/homebrew/Cellar/ollama/*/bin/ollama"), reverse=True))
        candidate_paths.extend(sorted(glob.glob("/usr/local/Cellar/ollama/*/bin/ollama"), reverse=True))

        for candidate_path in candidate_paths:
            if os.path.isfile(candidate_path) and os.access(candidate_path, os.X_OK):
                return candidate_path

        return None


    def wait_for_model_server_start(self, attempts: int = 10, delay_seconds: float = 1.0) -> bool:
        """Poll the model server until it responds or the startup wait expires."""

        for _ in range(attempts):
            if self._request_model_server_health():
                return True

            time.sleep(delay_seconds)

        return False


    def _request_model_server_health(self) -> bool:
        """Return True when the configured model server health endpoint responds."""

        try:
            response = self.session.get(
                f"{self.model_api_base_url}/api/tags",
                timeout=min(self.request_timeout, 5),
            )
            response.raise_for_status()
            return True

        except requests.RequestException:
            return False


    def close(self) -> None:
        """Close the reusable HTTP session and release pooled connections."""

        self.session.close()


    def shutdown_model(self) -> bool:
        """
        Ask the configured Ollama-compatible server to unload this model.

        Ollama keeps recently used models resident in memory. Passing
        keep_alive=0 to /api/generate unloads only the configured model while
        leaving an existing Ollama server process available for other work.
        """
        if not self._model_server_is_running:
            self._log_info(f"Skipping model shutdown because the model server is not running: {self.model_api_base_url}")
            return False

        try:
            response = self.session.post(
                f"{self.model_api_base_url}/api/generate",
                json={
                    "model": self.model_name,
                    "keep_alive": 0,
                    "stream": False,
                },
                timeout=min(self.request_timeout, 30),
            )
            response.raise_for_status()
            self._log_info(f"Shutdown model after task completion: {self.model_name}")

            return True

        except requests.RequestException as exc:
            self._log_warning(f"Failed to shutdown model {self.model_name}: {exc}")
            return False


    def stop_model_server(self) -> None:
        """
        Stop only the model server process started by this extractor instance.

        If Ollama was already running before this task, _model_server_process is
        None and the shared server is left alone.
        """
        if self._model_server_process is None:
            return

        self._model_server_process.terminate()

        try:
            self._model_server_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._model_server_process.kill()

        self._model_server_process = None


    def _is_local_model_api(self) -> bool:
        """Return True when the configured model API URL points to this machine."""

        hostname = urlparse(self.model_api_base_url).hostname

        return hostname in {"localhost", "127.0.0.1", "::1"}


    def build_prompt(self, original_name: str) -> str:
        """Create a strict prompt so the model returns only the organization name."""

        return f'''
            Extract the primary organization or institution name from the text below.

            Rules:
            - Return only one clean organization name.
            - Keep the country name if it is part of the official organization name (e.g., "Bank of China", "University of South Africa", "Government of Canada").
            - Remove department names, divisions, offices, addresses, personal names, job titles, email addresses, phone numbers, explanations, labels, bullets, and quotation marks.
            - Do not abbreviate or expand the organization name unless the text explicitly uses that form.
            - If multiple organizations are present, return the most prominent or primary organization.
            - If no organization name can be identified, return an empty string.
            - Return only the organization name and nothing else.

            Text:
            {original_name}
        '''.strip()


    def clean_model_response(self, response_text: Any) -> str:
        """
        Normalize the raw model response before it is stored or used for ROR.

        Model responses occasionally include markdown fences, labels, trailing
        punctuation, or sentinel values such as "N/A". This cleanup keeps the
        downstream database and ROR lookup logic working with one plain string.
        """
        if response_text is None:
            return ""

        text = str(response_text).strip()
        text = re.sub(r"^```(?:text)?", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"```$", "", text).strip()

        if "\n" in text:
            text = next((line.strip() for line in text.splitlines() if line.strip()), "")

        text = re.sub(r"^(organization name|organization|institution)\s*:\s*", "", text, flags=re.IGNORECASE)
        text = text.strip(" \t\r\n\"'`*-")

        if text.endswith("."):
            text = text[:-1].strip()

        if text.lower() in {"none", "n/a", "na", "unknown", "empty string", "no organization name"}:
            return ""

        return text


    def normalize_extracted_name(self, extracted_name: Any) -> str:
        """
        Trim whitespace and fit the extracted name to the MySQL column width.

        organization_location.model_extracted_name is varchar(200), so this
        method is the single place that enforces that size limit.
        """
        if not extracted_name:
            return ""

        normalized_name = " ".join(str(extracted_name).strip().split())

        return normalized_name[:self.extracted_name_max_length].strip()


    def make_extracted_name_hash_key(self, extracted_name: str) -> Optional[str]:
        """Generate the deterministic hash key for a non-empty extracted name."""

        normalized_name = self.normalize_extracted_name(extracted_name)

        if not normalized_name:
            return None

        return _make_hash_key(normalized_name)


    def _parse_timeout(self, timeout_value: Any) -> int:
        """Parse timeout configuration as a positive integer."""

        return self._parse_positive_int(timeout_value, "ORG_NAME_EXTRACT_TIMEOUT_SECONDS")


    def _parse_positive_int(self, value: Any, setting_name: str) -> int:
        """Parse a required positive integer setting."""

        try:
            parsed_value = int(value)

            if parsed_value > 0:
                return parsed_value

        except (TypeError, ValueError):
            pass

        raise ValueError(f"{setting_name} must be a positive integer. Current value: {value}")


    def _required_env(self, setting_name: str) -> str:
        """Read a required environment setting and fail clearly if it is blank."""

        value = os.getenv(setting_name)

        if value and value.strip():
            return value.strip()

        raise ValueError(f"Missing required environment variable: {setting_name}")


    def _log_error(self, message: str) -> None:
        """Log an error when a logger is available."""

        if self.logger is not None and hasattr(self.logger, "error"):
            self.logger.error(message)


    def _log_info(self, message: str) -> None:
        """Log an informational message when a logger is available."""

        if self.logger is not None and hasattr(self.logger, "info"):
            self.logger.info(message)


    def _log_warning(self, message: str) -> None:
        """Log a warning when a logger is available."""

        if self.logger is not None and hasattr(self.logger, "warning"):
            self.logger.warning(message)
