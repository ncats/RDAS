import os
import re
from typing import Any, Optional

import requests

from utils.tools import _make_hash_key


class LlamaOrgNameExtractHelper:
    """
    Shared helper for extracting clean organization names with a local Ollama model.

    Two organization-location flows need the same behavior:
    1. Build a strict prompt that asks the model for only one organization name.
    2. Call the configured Ollama /api/generate endpoint.
    3. Clean common formatting artifacts from the model response.
    4. Normalize the extracted name to fit organization_location.model_extracted_name.
    5. Generate the deterministic hash saved in model_extracted_name_hash_key.
    """

    DEFAULT_MODEL = "llama3.1"
    DEFAULT_BASE_URL = "http://localhost:11434"
    DEFAULT_TIMEOUT_SECONDS = 120
    EXTRACTED_NAME_MAX_LENGTH = 200

    def __init__(
        self,
        logger: Any = None,
        llama_model: Optional[str] = None,
        ollama_base_url: Optional[str] = None,
        ollama_timeout: Any = None,
    ):
        """
        Read Ollama settings from explicit arguments first, then environment.

        Args:
            logger: Optional logger with error()/warning() methods.
            llama_model: Optional model override; defaults to OLLAMA_MODEL.
            ollama_base_url: Optional Ollama base URL; defaults to OLLAMA_BASE_URL.
            ollama_timeout: Optional timeout override; defaults to OLLAMA_TIMEOUT_SECONDS.
        """
        self.logger = logger
        self.llama_model = llama_model or os.getenv("OLLAMA_MODEL", self.DEFAULT_MODEL)
        self.ollama_base_url = (
            ollama_base_url
            or os.getenv("OLLAMA_BASE_URL", self.DEFAULT_BASE_URL)
        ).rstrip("/")

        timeout_value = (
            ollama_timeout
            if ollama_timeout is not None
            else os.getenv("OLLAMA_TIMEOUT_SECONDS", str(self.DEFAULT_TIMEOUT_SECONDS))
        )
        self.ollama_timeout = self._parse_timeout(timeout_value)


    def extract_organization_name(self, original_name: str) -> Optional[str]:
        """
        Ask the configured Ollama model for one clean organization name.

        Returns:
            str: The cleaned model response. This can be an empty string when
                the model says there is no organization name.
            None: The Ollama request or response parsing failed. Callers use
                None as a retryable extraction failure.
        """
        prompt = self.build_prompt(original_name)
        url = f"{self.ollama_base_url}/api/generate"

        payload = {
            "model": self.llama_model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0,
            },
        }

        try:
            response = requests.post(url, json=payload, timeout=self.ollama_timeout)
            response.raise_for_status()
            data = response.json()

            return self.clean_llama_response(data.get("response", ""))

        except requests.RequestException as exc:
            self._log_error(f"Llama request failed for original_name={str(original_name)[:120]}: {exc}")
            return None

        except ValueError as exc:
            self._log_error(f"Llama response was not valid JSON for original_name={str(original_name)[:120]}: {exc}")
            return None


    def build_prompt(self, original_name: str) -> str:
        """Create a strict prompt so the model returns only the organization name."""

        return f'''
                Extract the main organization or institution name from the text below.
                Return only one clean organization name.
                Do not include departments, addresses, people, explanations, labels, bullets, or quotes.
                If there is no organization name, return an empty string.

                Text:
                {original_name}
            '''.strip()


    def clean_llama_response(self, response_text: Any) -> str:
        """
        Normalize the raw model response before it is stored or used for ROR.

        Ollama responses occasionally include markdown fences, labels, trailing
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

        return normalized_name[:self.EXTRACTED_NAME_MAX_LENGTH].strip()


    def make_extracted_name_hash_key(self, extracted_name: str) -> Optional[str]:
        """Generate the deterministic hash key for a non-empty extracted name."""

        normalized_name = self.normalize_extracted_name(extracted_name)

        if not normalized_name:
            return None

        return _make_hash_key(normalized_name)


    def _parse_timeout(self, timeout_value: Any) -> int:
        """Parse timeout configuration and fall back to the default if invalid."""

        try:
            return int(timeout_value)

        except (TypeError, ValueError):
            self._log_warning(
                f"Invalid OLLAMA_TIMEOUT_SECONDS={timeout_value}; "
                f"using {self.DEFAULT_TIMEOUT_SECONDS} seconds."
            )
            return self.DEFAULT_TIMEOUT_SECONDS


    def _log_error(self, message: str) -> None:
        """Log an error when a logger is available."""

        if self.logger is not None and hasattr(self.logger, "error"):
            self.logger.error(message)


    def _log_warning(self, message: str) -> None:
        """Log a warning when a logger is available."""

        if self.logger is not None and hasattr(self.logger, "warning"):
            self.logger.warning(message)
