"""Endpoint-compatible local facades for RDAS EPI/NHS calls."""

from __future__ import annotations

from typing import Dict, Sequence

from .classifier import postEpiClassifyText, post_epi_classify_text
from .extractor import postEpiExtractText, post_epi_extract_text
from .nhs import predict, predict_article

EPI_API_URI = "https://rdas.ncats.nih.gov/api/epi"
EPI_CLASSIFY_API = f"{EPI_API_URI}/postEpiClassifyText/"
EPI_EXTRACT_API = f"{EPI_API_URI}/postEpiExtractText/"
NHS_PREDICT_API = "https://rdas.ncats.nih.gov/api/article_prediction/v1/predict"


def local_post(url: str, payload: Dict) -> Dict:
    """
    Dispatch a former RDAS HTTP POST URL to the matching local implementation.

    Supported URLs:
        EPI_CLASSIFY_API
        EPI_EXTRACT_API
        NHS_PREDICT_API
    """
    normalized_url = url.rstrip("/")

    if normalized_url == EPI_CLASSIFY_API.rstrip("/"):
        return post_epi_classify_text(payload["text"])

    if normalized_url == EPI_EXTRACT_API.rstrip("/"):
        return post_epi_extract_text(
            payload["text"],
            extract_diseases=bool(payload.get("extract_diseases", False)),
        )

    if normalized_url == NHS_PREDICT_API.rstrip("/"):
        texts: Sequence[str] = payload["texts"]
        return predict_article(texts)

    raise ValueError(f"Unsupported local RDAS endpoint: {url}")
