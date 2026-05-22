import os
import re
from typing import Any, Optional

import requests

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
        self.request_timeout = self._parse_timeout(os.getenv("ORG_NAME_EXTRACT_TIMEOUT_SECONDS") )
        self.extracted_name_max_length = self._parse_positive_int(os.getenv("ORG_NAME_EXTRACT_MAX_LENGTH"), "ORG_NAME_EXTRACT_MAX_LENGTH", )


    def extract_organization_name(self, original_name: str) -> Optional[str]:
        """
        Ask the configured model for one clean organization name.

        Returns:
            str: The cleaned model response. This can be an empty string when
                the model says there is no organization name.
            None: The model request or response parsing failed. Callers use
                None as a retryable extraction failure.
        """
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
            response = requests.post(url, json=payload, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()

            return self.clean_model_response(data.get("response", ""))

        except requests.RequestException as exc:
            self._log_error(f"Model request failed for original_name={str(original_name)[:120]}: {exc}")
            return None

        except ValueError as exc:
            self._log_error(f"Model response was not valid JSON for original_name={str(original_name)[:120]}: {exc}")
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


    def _log_warning(self, message: str) -> None:
        """Log a warning when a logger is available."""

        if self.logger is not None and hasattr(self.logger, "warning"):
            self.logger.warning(message)
