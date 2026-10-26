import logging
import time
from typing import Any, Callable, Dict, Optional

import requests


class HTTPSUtils:
    """Bound HTTP waits and return None, rather than raising, for unavailable data."""

    @staticmethod
    def record_failure(logger, description: str, reason: str) -> None:

        """Use one warning format for unavailable API items; no retry queue is created."""
        (logger or logging.getLogger(__name__)).warning(
            "%s unavailable (%s); leaving missing data unchanged and continuing.", description, reason
        )

    @staticmethod
    def request_with_retry(method: str, url: str, process_response: Callable[[requests.Response], Any], max_retries: int = 10, delay: float = 1, timeout=30, logger=None, description: str = "HTTP request", session=None, **request_options) -> Optional[Any]:

        '''
        Retry network errors, HTTP 408/425/429/5xx, and unusable responses with
        bounded exponential backoff. For example, two HTTP 502 responses then
        valid study JSON produce warnings followed by success, without marking
        the GARD node failed. If all attempts fail, return None and record one
        missing item in the log and continue. Do not defer or replay the GARD
        node: completed requests and normal checkpoints remain completed.

        Other HTTP failures, such as 401/403/404, cannot normally recover by
        immediate repetition; return None after one attempt. Never log URLs,
        response bodies or exception messages: they may contain API keys.
        Callbacks must only decode/validate data or write a temporary download;
        database operations belong to the caller and their errors propagate.
        False, zero, empty dictionaries and lists are valid callback results;
        only None denotes an unusable response. Close every HTTP response,
        including failed attempts, before waiting or returning to the caller.
        '''
        if max_retries < 1 or delay < 0:
            raise ValueError("HTTP attempts must be positive and retry delay must not be negative.")
        log = logger or logging.getLogger(__name__)
        if not url:
            HTTPSUtils.record_failure(log, description, "endpoint is not configured")
            return None
        client = session or requests
        for attempt in range(1, max_retries + 1):
            response = None
            retry_after = ""
            retryable = True
            try:
                response = client.request(method, url, timeout=timeout, **request_options)
                retry_after = response.headers.get("Retry-After", "").strip()
                if response.status_code >= 400:
                    reason = f"HTTP {response.status_code}"
                    retryable = response.status_code in {408, 425, 429} or 500 <= response.status_code <= 599
                else:
                    value = process_response(response)
                    if value is not None:
                        return value
                    reason = "response contained no usable value"
            except (requests.RequestException, ValueError, KeyError, TypeError, AttributeError, IndexError) as exc:
                '''
                A JSON response can be syntactically valid but have missing
                objects/list entries. Treat those decode/shape failures like
                unavailable HTTP data, so an unexpected API payload cannot
                escape into a caller that logs a fatal task error. Callbacks
                do not perform database writes; SQL exceptions stay outside
                this boundary and are not mistaken for HTTP failures.
                '''
                reason = type(exc).__name__
            finally:
                if response is not None:
                    try:
                        response.close()
                    except (requests.RequestException, OSError):
                        log.warning("%s HTTP response cleanup failed; continuing.", description)

            if not retryable or attempt == max_retries:
                HTTPSUtils.record_failure(log, description, f"{reason}; attempts={attempt}/{max_retries}")
                return None
            wait_seconds = min(30, delay * 2 ** (attempt - 1))
            if retry_after.isdigit():
                wait_seconds = min(30, max(wait_seconds, int(retry_after)))
            log.warning("%s attempt %s/%s failed (%s); retrying in %s seconds.", description, attempt, max_retries, reason, wait_seconds)
            time.sleep(wait_seconds)

    @staticmethod
    def with_api_retry(url: str, payload: Dict, process_response: Callable[[requests.Response], Any], max_retries: int = 10, delay: float = 1, timeout=30, logger=None, description: str = "HTTP POST", session=None, **request_options) -> Optional[Any]:

        """Preserve the existing POST API used by publication classification callers."""
        return HTTPSUtils.request_with_retry("POST", url, process_response, max_retries=max_retries, delay=delay, timeout=timeout, logger=logger, description=description, session=session, json=payload, **request_options)

    @staticmethod
    def with_api_retry_GET(url: str, process_response: Callable[[requests.Response], Any], max_retries: int = 10, delay: float = 1, timeout=30, logger=None, description: str = "HTTP GET", session=None, **request_options) -> Optional[Any]:

        """Preserve the existing GET API while supporting params, sessions and timeouts."""
        return HTTPSUtils.request_with_retry("GET", url, process_response, max_retries=max_retries, delay=delay, timeout=timeout, logger=logger, description=description, session=session, **request_options)
