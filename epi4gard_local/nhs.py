"""Local interface for the RDAS article_prediction endpoint."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence

PredictionResult = Dict[str, List[int]]
DEFAULT_NHS_MODEL = "NIHNCATS/NHS-BiomedNLP-BiomedBERT-hypop"
DEFAULT_NHS_MAX_LENGTH = 256
NHS_MODEL_PATH_ENV_NAMES = ("NHS_MODEL_PATH", "MODEL_PATH")


class NhsPredictionUnavailable(RuntimeError):
    """Raised when no local NHS prediction model has been configured."""


class NhsDependencyError(ImportError):
    """Raised when local NHS prediction dependencies are not installed."""


_LOCAL_NHS_PREDICTOR: Optional[Callable[[Sequence[str]], Sequence[int]]] = None


def _load_dependencies():

    try:
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
    except ImportError as exc:
        raise NhsDependencyError(
            "Local NHS prediction requires torch and transformers. Install the "
            "project requirements in the active RDAS environment."
        ) from exc

    return torch, AutoModelForSequenceClassification, AutoTokenizer


def _env_bool(name: str, default: bool) -> bool:

    value = os.getenv(name)
    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_local_files_only(local_files_only: Optional[bool]) -> bool:

    if local_files_only is not None:
        return local_files_only

    return _env_bool("NHS_LOCAL_FILES_ONLY", True)


def _resolve_model_name_or_path(model_name_or_path: Optional[str]) -> str:

    if model_name_or_path:
        return model_name_or_path

    for env_name in NHS_MODEL_PATH_ENV_NAMES:
        env_value = os.getenv(env_name)
        if env_value:
            return env_value

    return DEFAULT_NHS_MODEL


def _batch_items(items: Sequence[str], batch_size: int) -> Iterator[Sequence[str]]:

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero.")

    for start in range(0, len(items), batch_size):
        yield items[start:start + batch_size]


def _coerce_predictions(predictions: Iterable[int], expected_count: int) -> List[int]:

    coerced_predictions = [int(value) for value in predictions]
    if len(coerced_predictions) != expected_count:
        raise ValueError("The NHS predictor returned a different number of predictions than inputs.")

    return coerced_predictions


class NhsArticlePredictionPipeline:
    """Reusable local predictor matching `/api/article_prediction/v1/predict`."""

    def __init__(self, model_name_or_path: Optional[str] = None, device: Optional[str] = None, cache_dir: Optional[str] = None, local_files_only: Optional[bool] = None, max_length: int = DEFAULT_NHS_MAX_LENGTH) -> None:

        torch, model_cls, tokenizer_cls = _load_dependencies()
        self._torch = torch
        self.model_name_or_path = _resolve_model_name_or_path(model_name_or_path)
        self.local_files_only = _resolve_local_files_only(local_files_only)
        self.max_length = max_length

        try:
            self.tokenizer = tokenizer_cls.from_pretrained(
                self.model_name_or_path,
                cache_dir=cache_dir,
                local_files_only=self.local_files_only,
            )
            self.model = model_cls.from_pretrained(
                self.model_name_or_path,
                cache_dir=cache_dir,
                local_files_only=self.local_files_only,
            )
        except OSError as exc:
            raise NhsPredictionUnavailable(
                "Unable to load the local NHS prediction model. Set NHS_MODEL_PATH "
                "or MODEL_PATH to a local NaturalHistory_Transformer_API_v1.0 "
                "saved_model directory, pre-download "
                f"{DEFAULT_NHS_MODEL!r} into the Hugging Face cache, or call "
                "predict_article(..., local_files_only=False) when network model "
                "download is acceptable."
            ) from exc

        self.device = torch.device(device) if device else torch.device("cpu")
        self.model.to(self.device)
        self.model.eval()

    def predict_texts(self, texts: Sequence[str], batch_size: int = 32) -> List[int]:

        text_list = list(texts)
        if not text_list:
            return []

        predictions: List[int] = []
        for batch in _batch_items(text_list, batch_size):
            predictions.extend(self._predict_batch(batch))

        return predictions

    def _predict_batch(self, texts: Sequence[str]) -> List[int]:

        encoded = self.tokenizer(
            list(texts),
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=self.max_length,
        )
        encoded = {name: value.to(self.device) for name, value in encoded.items()}

        with self._torch.inference_mode():
            logits = self.model(**encoded).logits

        return [int(value) for value in logits.argmax(dim=-1).detach().cpu().tolist()]


def configure_nhs_predictor(predictor: Callable[[Sequence[str]], Sequence[int]]) -> None:

    """
    Register a process-local NHS predictor.

    The callable must accept a sequence of texts and return a same-length
    sequence of 0/1 predictions. It overrides the transformer model loader and
    is useful for focused tests or alternate locally managed model services.
    """
    global _LOCAL_NHS_PREDICTOR
    _LOCAL_NHS_PREDICTOR = predictor


def clear_nhs_predictor() -> None:

    global _LOCAL_NHS_PREDICTOR
    _LOCAL_NHS_PREDICTOR = None


@lru_cache(maxsize=4)
def get_nhs_pipeline(model_name_or_path: Optional[str] = None, device: Optional[str] = None, cache_dir: Optional[str] = None, local_files_only: Optional[bool] = None, max_length: int = DEFAULT_NHS_MAX_LENGTH) -> NhsArticlePredictionPipeline:

    """Return a cached NHS predictor so repeated calls reuse the loaded model."""
    return NhsArticlePredictionPipeline(
        model_name_or_path=model_name_or_path,
        device=device,
        cache_dir=cache_dir,
        local_files_only=local_files_only,
        max_length=max_length,
    )


def clear_nhs_pipeline_cache() -> None:

    """Clear cached model instances created by `get_nhs_pipeline`."""
    get_nhs_pipeline.cache_clear()


def predict_article(texts: Sequence[str], model_name_or_path: Optional[str] = None, device: Optional[str] = None, cache_dir: Optional[str] = None, local_files_only: Optional[bool] = None, batch_size: int = 32, max_length: int = DEFAULT_NHS_MAX_LENGTH) -> PredictionResult:

    """
    Endpoint-compatible local replacement for /api/article_prediction/v1/predict.

    Returns:
        {"predictions": [0 or 1, ...]}
    """
    text_list = list(texts)
    if _LOCAL_NHS_PREDICTOR is None:
        predictions = get_nhs_pipeline(
            model_name_or_path=model_name_or_path,
            device=device,
            cache_dir=cache_dir,
            local_files_only=local_files_only,
            max_length=max_length,
        ).predict_texts(text_list, batch_size=batch_size)
    else:
        predictions = _LOCAL_NHS_PREDICTOR(text_list)

    return {"predictions": _coerce_predictions(predictions, len(text_list))}


def predict(texts: Sequence[str], model_name_or_path: Optional[str] = None, device: Optional[str] = None, cache_dir: Optional[str] = None, local_files_only: Optional[bool] = None, batch_size: int = 32, max_length: int = DEFAULT_NHS_MAX_LENGTH) -> PredictionResult:

    return predict_article(
        texts,
        model_name_or_path=model_name_or_path,
        device=device,
        cache_dir=cache_dir,
        local_files_only=local_files_only,
        batch_size=batch_size,
        max_length=max_length,
    )


def iter_predict_article(texts: Sequence[str], model_name_or_path: Optional[str] = None, device: Optional[str] = None, cache_dir: Optional[str] = None, local_files_only: Optional[bool] = None, batch_size: int = 32, max_length: int = DEFAULT_NHS_MAX_LENGTH) -> Iterator[int]:

    predictions = predict_article(
        texts,
        model_name_or_path=model_name_or_path,
        device=device,
        cache_dir=cache_dir,
        local_files_only=local_files_only,
        batch_size=batch_size,
        max_length=max_length,
    )["predictions"]

    yield from predictions
