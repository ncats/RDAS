"""Command-line smoke test for epi4gard_local."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from .classifier import DEFAULT_MODEL, DependencyError, EpiClassifyTextPipeline
from .extractor import DEFAULT_EXTRACT_MODEL, EpiExtractPipeline, ExtractionDependencyError
from .nhs import DEFAULT_NHS_MODEL, NhsDependencyError, NhsPredictionUnavailable, predict_article

DEFAULT_TEST_TEXT = (
    "A population-based study estimated the prevalence and incidence of a rare "
    "disease using records from patients diagnosed between 2010 and 2020."
)


def _iter_input_lines(input_file: Path) -> Iterable[str]:
    with input_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if line:
                yield line


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Classify text locally with the epi4GARD endpoint-compatible model."
    )
    parser.add_argument(
        "text",
        nargs="?",
        help="Text to classify. If omitted, a short built-in test abstract is used.",
    )
    parser.add_argument(
        "--text",
        dest="text_option",
        help="Text to classify. Overrides the positional text argument.",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        help="Optional newline-delimited text file for batch classification.",
    )
    parser.add_argument(
        "--endpoint",
        choices=["classify", "extract", "nhs"],
        default="classify",
        help="Local endpoint replacement to test. Default: classify.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Batch size used with --input-file. Default: 32.",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Classification model name or local path. Default: {DEFAULT_MODEL}.",
    )
    parser.add_argument(
        "--nhs-model",
        default=None,
        help=f"NHS model name or local path. Default: NHS_MODEL_PATH/MODEL_PATH or {DEFAULT_NHS_MODEL}.",
    )
    parser.add_argument(
        "--extract-model",
        default=DEFAULT_EXTRACT_MODEL,
        help=f"Extraction model name or local path. Default: {DEFAULT_EXTRACT_MODEL}.",
    )
    parser.add_argument(
        "--device",
        default=None,
        help="Optional torch device, such as cpu, cuda, cuda:0, or mps.",
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="Optional Hugging Face cache directory.",
    )
    parser.add_argument(
        "--local-files-only",
        action="store_true",
        help="Use only local model files/cache and do not contact Hugging Face.",
    )
    parser.add_argument(
        "--extract-diseases",
        action="store_true",
        help="Include local GARD dictionary disease extraction with --endpoint extract.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    text = args.text_option or args.text or DEFAULT_TEST_TEXT

    if args.endpoint == "classify":
        try:
            classifier = EpiClassifyTextPipeline(
                model_name_or_path=args.model,
                device=args.device,
                cache_dir=args.cache_dir,
                local_files_only=args.local_files_only,
            )
        except DependencyError as exc:
            print(str(exc), file=sys.stderr)
            return 2

        if args.input_file:
            for result in classifier.iter_classify_texts(
                _iter_input_lines(args.input_file),
                batch_size=args.batch_size,
            ):
                print(json.dumps(result))
        else:
            print(json.dumps(classifier.post_epi_classify_text(text)))
        return 0

    if args.endpoint == "extract":
        try:
            extractor = EpiExtractPipeline(
                model_name_or_path=args.extract_model,
                device=args.device,
                cache_dir=args.cache_dir,
                local_files_only=args.local_files_only,
            )
        except ExtractionDependencyError as exc:
            print(str(exc), file=sys.stderr)
            return 2

        print(json.dumps(extractor.post_epi_extract_text(text, extract_diseases=args.extract_diseases)))
        return 0

    try:
        texts = list(_iter_input_lines(args.input_file)) if args.input_file else [text]
        print(json.dumps(
            predict_article(
                texts,
                model_name_or_path=args.nhs_model,
                device=args.device,
                cache_dir=args.cache_dir,
                local_files_only=args.local_files_only,
                batch_size=args.batch_size,
            )
        ))
    except (NhsDependencyError, NhsPredictionUnavailable) as exc:
        print(str(exc), file=sys.stderr)
        return 3

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
