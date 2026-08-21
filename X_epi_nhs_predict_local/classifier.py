"""Importable local classifier matching postEpiClassifyText output."""

from __future__ import annotations

from functools import lru_cache
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

DEFAULT_MODEL = "ncats/EpiClassify4GARD"
EndpointResult = Dict[str, object]


class DependencyError(ImportError):
    """Raised when the local classification dependencies are not installed."""


def _load_dependencies():
    try:
        import torch
        from transformers import AutoModelForSequenceClassification, BertConfig, BertTokenizer
    except ImportError as exc:
        raise DependencyError(
            "X_epi_nhs_predict_local requires torch and transformers. Install this package with "
            "`pip install -e .` from the repository root, or install those dependencies "
            "in your existing epi4GARD environment."
        ) from exc

    return torch, AutoModelForSequenceClassification, BertConfig, BertTokenizer


def _batch_items(items: Iterable[str], batch_size: int) -> Iterator[List[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero.")

    batch: List[str] = []
    for item in items:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


class EpiClassifyTextPipeline:
    """Reusable local classifier for endpoint-compatible text classification.

    The public `post_epi_classify_text` method returns the same JSON-compatible
    shape as `/postEpiClassifyText/`:
    `{"ABSTRACT": text, "EPI_PROB": str(probability), "IsEpi": bool}`.
    """

    def __init__(
        self,
        model_name_or_path: str = DEFAULT_MODEL,
        device: Optional[str] = None,
        cache_dir: Optional[str] = None,
        local_files_only: bool = False,
    ) -> None:
        torch, model_cls, config_cls, tokenizer_cls = _load_dependencies()
        self._torch = torch
        self.model_name_or_path = model_name_or_path
        self.config = config_cls.from_pretrained(
            model_name_or_path,
            cache_dir=cache_dir,
            local_files_only=local_files_only,
        )
        self.tokenizer = tokenizer_cls.from_pretrained(
            self.config._name_or_path,
            model_max_length=self.config.max_position_embeddings,
            cache_dir=cache_dir,
            local_files_only=local_files_only,
        )
        self.device = torch.device(device) if device else torch.device("cpu")
        self.model = model_cls.from_pretrained(
            model_name_or_path,
            config=self.config,
            cache_dir=cache_dir,
            local_files_only=local_files_only,
        )
        self.model.to(self.device)
        self.model.eval()

    def __call__(self, text: str) -> Tuple[float, bool]:
        return self.predict(text)

    def predict(self, text: str) -> Tuple[float, bool]:
        """Return `(probability, is_epi)` for one abstract."""
        if len(text) <= 5:
            return 0.0, False

        return self._predict_nonempty_batch([text])[0]

    def post_epi_classify_text(self, text: str) -> EndpointResult:
        """Return the same result shape as the FastAPI classification endpoint."""
        epi_prob, is_epi = self.predict(text)
        return {"ABSTRACT": text, "EPI_PROB": str(epi_prob), "IsEpi": is_epi}

    def postEpiClassifyText(self, text: str) -> EndpointResult:
        """Endpoint-style alias for `post_epi_classify_text`."""
        return self.post_epi_classify_text(text)

    def classify_text(self, text: str) -> EndpointResult:
        """Alias for `post_epi_classify_text`."""
        return self.post_epi_classify_text(text)

    def classify_texts(self, texts: Iterable[str], batch_size: int = 32) -> List[EndpointResult]:
        """Classify many texts and return endpoint-compatible dictionaries."""
        return list(self.iter_classify_texts(texts, batch_size=batch_size))

    def iter_classify_texts(
        self,
        texts: Iterable[str],
        batch_size: int = 32,
    ) -> Iterator[EndpointResult]:
        """Classify many texts lazily, preserving input order.

        This is the preferred API for large jobs because it avoids accumulating
        all results in memory while still running model inference in batches.
        """
        for batch in _batch_items(texts, batch_size):
            results: List[EndpointResult] = [
                {"ABSTRACT": text, "EPI_PROB": "0.0", "IsEpi": False}
                for text in batch
            ]
            valid_positions = [index for index, text in enumerate(batch) if len(text) > 5]
            if valid_positions:
                valid_texts = [batch[index] for index in valid_positions]
                predictions = self._predict_nonempty_batch(valid_texts)
                for index, (epi_prob, is_epi) in zip(valid_positions, predictions):
                    results[index] = {
                        "ABSTRACT": batch[index],
                        "EPI_PROB": str(epi_prob),
                        "IsEpi": is_epi,
                    }

            yield from results

    def _predict_nonempty_batch(self, texts: Sequence[str]) -> List[Tuple[float, bool]]:
        encoded = self.tokenizer(
            text=list(texts),
            max_length=self.config.max_position_embeddings,
            padding="max_length",
            truncation=True,
            return_tensors="pt",
        )
        encoded = {name: value.to(self.device) for name, value in encoded.items()}

        with self._torch.inference_mode():
            output = self.model(**encoded)
            logits = output.logits
            is_epi_values = logits.argmax(dim=-1).detach().cpu().tolist()
            probabilities = logits.softmax(dim=-1)[:, 1].detach().cpu().tolist()

        return [
            (float(probability), bool(is_epi))
            for probability, is_epi in zip(probabilities, is_epi_values)
        ]


@lru_cache(maxsize=4)
def get_pipeline(
    model_name_or_path: str = DEFAULT_MODEL,
    device: Optional[str] = None,
    cache_dir: Optional[str] = None,
    local_files_only: bool = True,
) -> EpiClassifyTextPipeline:
    """Return a cached pipeline so repeated calls reuse the loaded model."""
    return EpiClassifyTextPipeline(
        model_name_or_path=model_name_or_path,
        device=device,
        cache_dir=cache_dir,
        local_files_only=local_files_only,
    )


def clear_pipeline_cache() -> None:
    """Clear cached model instances created by `get_pipeline`."""
    get_pipeline.cache_clear()


def classify_text(
    text: str,
    model_name_or_path: str = DEFAULT_MODEL,
    device: Optional[str] = None,
    cache_dir: Optional[str] = None,
    local_files_only: bool = True,
) -> EndpointResult:
    """Convenience function matching `/postEpiClassifyText/` output."""
    return get_pipeline(
        model_name_or_path=model_name_or_path,
        device=device,
        cache_dir=cache_dir,
        local_files_only=local_files_only,
    ).post_epi_classify_text(text)


def post_epi_classify_text(
    text: str,
    model_name_or_path: str = DEFAULT_MODEL,
    device: Optional[str] = None,
    cache_dir: Optional[str] = None,
    local_files_only: bool = True,
) -> EndpointResult:
    """Convenience function matching `/postEpiClassifyText/` output."""
    return classify_text(
        text,
        model_name_or_path=model_name_or_path,
        device=device,
        cache_dir=cache_dir,
        local_files_only=local_files_only,
    )


def postEpiClassifyText(
    text: str,
    model_name_or_path: str = DEFAULT_MODEL,
    device: Optional[str] = None,
    cache_dir: Optional[str] = None,
    local_files_only: bool = True,
) -> EndpointResult:
    """Endpoint-style convenience function matching `/postEpiClassifyText/`."""
    return post_epi_classify_text(
        text,
        model_name_or_path=model_name_or_path,
        device=device,
        cache_dir=cache_dir,
        local_files_only=local_files_only,
    )
