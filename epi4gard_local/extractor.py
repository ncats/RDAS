"""Local epidemiology extraction matching postEpiExtractText output."""

from __future__ import annotations

import json
import re
from collections import OrderedDict
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from typing import Dict, List, Optional, Set, Tuple, Union

DEFAULT_EXTRACT_MODEL = "ncats/EpiExtract4GARD-v2"
ExtractionResult = Dict[str, object]


class ExtractionDependencyError(ImportError):
    """Raised when local extraction dependencies are not installed."""


def _load_extraction_dependencies():
    try:
        import nltk
        import numpy as np
        import torch
        from more_itertools import pairwise
        from nltk import tokenize as nltk_tokenize
        from torch import nn
        from transformers import AutoModelForTokenClassification, BertConfig, BertTokenizer
        from unidecode import unidecode
    except ImportError as exc:
        raise ExtractionDependencyError(
            "Local EPI extraction requires nltk, numpy, torch, transformers, "
            "more-itertools, and unidecode in the active Python environment."
        ) from exc

    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError as exc:
        raise ExtractionDependencyError("NLTK punkt data is required for local EPI extraction.") from exc

    try:
        nltk.data.find("corpora/stopwords")
    except LookupError as exc:
        raise ExtractionDependencyError("NLTK stopwords data is required for local disease extraction.") from exc

    return {
        "np": np,
        "torch": torch,
        "pairwise": pairwise,
        "nltk_tokenize": nltk_tokenize,
        "nn": nn,
        "AutoModelForTokenClassification": AutoModelForTokenClassification,
        "BertConfig": BertConfig,
        "BertTokenizer": BertTokenizer,
        "unidecode": unidecode,
    }


def order_labels(entity_classes: Union[Set[str], List[str]]) -> List[str]:
    ordered_labels: List[str] = []
    label_order = ["DIS", "ABRV", "EPI", "STAT", "LOC", "DATE", "SEX", "ETHN"]
    ordered_labels = [label for label in label_order if label in entity_classes]
    for entity in entity_classes:
        if entity not in label_order:
            ordered_labels.append(entity)
    return ordered_labels


@dataclass
class NERInput:
    guid: str
    words: List[str]
    labels: List[str]


@dataclass
class InputFeatures:
    input_ids: List[int]
    attention_mask: List[int]
    token_type_ids: Optional[List[int]] = None
    label_ids: Optional[List[int]] = None


class EpiExtractPipeline:
    """Reusable local BioBERT NER pipeline for endpoint-compatible extraction."""

    def __init__(
        self,
        model_name_or_path: str = DEFAULT_EXTRACT_MODEL,
        device: Optional[str] = None,
        cache_dir: Optional[str] = None,
        local_files_only: bool = False,
        eval_batch_size: int = 16,
    ) -> None:
        deps = _load_extraction_dependencies()
        self._deps = deps
        self._np = deps["np"]
        self._torch = deps["torch"]
        self._pairwise = deps["pairwise"]
        self._nn = deps["nn"]
        tokenizer_cls = deps["BertTokenizer"]
        config_cls = deps["BertConfig"]
        model_cls = deps["AutoModelForTokenClassification"]
        self.eval_batch_size = eval_batch_size

        self.bert_tokenizer = tokenizer_cls.from_pretrained(
            model_name_or_path,
            cache_dir=cache_dir,
            local_files_only=local_files_only,
        )
        self.config = config_cls.from_pretrained(
            model_name_or_path,
            cache_dir=cache_dir,
            local_files_only=local_files_only,
        )
        self.labels = {re.sub(".-", "", label) for label in self.config.label2id.keys() if label != "O"}
        self.device = self._torch.device(device) if device else self._torch.device("cpu")
        try:
            self.model = model_cls.from_pretrained(
                model_name_or_path,
                cache_dir=cache_dir,
                local_files_only=local_files_only,
            )
        except Exception as exc:
            mode = "local cache/path" if local_files_only else "Hugging Face or local cache/path"
            raise ExtractionDependencyError(
                f"Could not load EPI extraction model weights for {model_name_or_path!r} from {mode}. "
                "Provide a local model path/cache with model weights, or rerun with local_files_only=False "
                "in an environment that can download the model."
            ) from exc
        self.model.to(self.device)
        self.model.eval()

    def __call__(self, text: str, rd_identify: Optional["GARDSearch"] = None) -> Optional[ExtractionResult]:
        return self.get_text_extraction(text, rd_identify)

    def get_text_extraction(self, text: str, rd_identify: Optional["GARDSearch"] = None) -> Optional[ExtractionResult]:
        output_dict: ExtractionResult = {label: [] for label in self.labels}
        dataset = _NerDataset(text, self.bert_tokenizer, self.config, self._deps)
        predictions, label_ids = self._predict_dataset(dataset)
        preds_list, _ = self.align_predictions(predictions, label_ids)

        for ner_input, sent_pred_list in zip(dataset.ner_inputs, preds_list):
            ner_input.labels = sent_pred_list

        for sentence in dataset.ner_inputs:
            entity: List[str] = []
            for idx, (current, nxt) in enumerate(self._pairwise(sentence.labels)):
                if current != "O":
                    _, current_tag = self.get_tag(current)
                    if nxt == "O":
                        entity.append(sentence.words[idx])
                        output_dict[current_tag].append(" ".join(entity))
                        entity.clear()
                    else:
                        nxt_ib, nxt_tag = self.get_tag(nxt)
                        if nxt_tag == current_tag:
                            if nxt_ib == "B":
                                entity.append(sentence.words[idx])
                                output_dict[current_tag].append(" ".join(entity))
                                entity.clear()
                            else:
                                entity.append(sentence.words[idx])
                        else:
                            entity.append(sentence.words[idx])
                            output_dict[current_tag].append(" ".join(entity))
                            entity.clear()

                if idx == len(sentence.labels) - 2 and nxt != "O":
                    _, nxt_tag = self.get_tag(nxt)
                    entity.append(sentence.words[idx + 1])
                    output_dict[nxt_tag].append(" ".join(entity))
                    entity.clear()

        if "DIS" not in output_dict.keys() and rd_identify:
            output_dict["DIS"] = []
            output_dict["IDS"] = []
            for sentence in dataset.ner_inputs:
                diseases, ids = rd_identify(" ".join(sentence.words))
                output_dict["DIS"] += diseases
                output_dict["IDS"] += ids

        for entity_name, output in output_dict.items():
            if not output:
                output_dict[entity_name] = None
            elif entity_name != "STAT":
                output_dict[entity_name] = list(OrderedDict.fromkeys(output))

        if output_dict.get("EPI") and output_dict.get("STAT"):
            return output_dict

        return None

    def _predict_dataset(self, dataset: "_NerDataset") -> Tuple[object, object]:
        prediction_batches = []
        label_batches = []

        for start in range(0, len(dataset), self.eval_batch_size):
            features = dataset.features[start:start + self.eval_batch_size]
            batch = {
                "input_ids": self._torch.tensor([feature.input_ids for feature in features], dtype=self._torch.long, device=self.device),
                "attention_mask": self._torch.tensor([feature.attention_mask for feature in features], dtype=self._torch.long, device=self.device),
            }
            if features and features[0].token_type_ids is not None:
                batch["token_type_ids"] = self._torch.tensor(
                    [feature.token_type_ids for feature in features],
                    dtype=self._torch.long,
                    device=self.device,
                )

            with self._torch.inference_mode():
                output = self.model(**batch)

            prediction_batches.append(output.logits.detach().cpu().numpy())
            label_batches.append(
                self._np.array([feature.label_ids for feature in features], dtype=self._np.int64)
            )

        return self._np.concatenate(prediction_batches, axis=0), self._np.concatenate(label_batches, axis=0)

    def align_predictions(self, predictions, label_ids):
        preds = self._np.argmax(predictions, axis=2)
        batch_size, seq_len = preds.shape
        out_label_list = [[] for _ in range(batch_size)]
        preds_list = [[] for _ in range(batch_size)]
        ignore_index = self._nn.CrossEntropyLoss().ignore_index
        for i in range(batch_size):
            for j in range(seq_len):
                if label_ids[i, j] != ignore_index:
                    out_label_list[i].append(self.config.id2label[label_ids[i][j]])
                    preds_list[i].append(self.config.id2label[preds[i][j]])

        return preds_list, out_label_list

    @staticmethod
    def get_tag(entity_name: str) -> Tuple[str, str]:
        if entity_name.startswith("B-"):
            return "B", entity_name[2:]
        if entity_name.startswith("I-"):
            return "I", entity_name[2:]
        return "I", entity_name

    def post_epi_extract_text(self, text: str, extract_diseases: bool = False) -> ExtractionResult:
        return api_text_extraction(text, self, get_gard_search() if extract_diseases else None, extract_diseases)

    def postEpiExtractText(self, text: str, extract_diseases: bool = False) -> ExtractionResult:
        return self.post_epi_extract_text(text, extract_diseases=extract_diseases)


class _NerDataset:
    def __init__(self, abstract: str, tokenizer, config, deps) -> None:
        self._deps = deps
        self.pad_token_label_id = deps["nn"].CrossEntropyLoss().ignore_index
        self.ner_inputs = self.abstract2NERinputs(abstract)
        self.features = self.convert_NERinputs_to_features(
            self.ner_inputs,
            config,
            tokenizer,
            cls_token_at_end=bool(config.model_type in ["xlnet"]),
            cls_token=tokenizer.cls_token,
            cls_token_segment_id=2 if config.model_type in ["xlnet"] else 0,
            sep_token=tokenizer.sep_token,
            sep_token_extra=False,
            pad_on_left=bool(tokenizer.padding_side == "left"),
            pad_token_segment_id=tokenizer.pad_token_type_id,
            pad_token_label_id=self.pad_token_label_id,
        )

    def __len__(self) -> int:
        return len(self.features)

    def __getitem__(self, index: int) -> InputFeatures:
        return self.features[index]

    def str2sents(self, string: str) -> List[str]:
        superscripts = re.findall("<sup>.</sup>", string)
        for i in range(len(superscripts)):
            string = re.sub("<sup>.</sup>", "^" + superscripts[i][5], string)
        string = re.sub("<.{1,4}>|  *|  ", " ", string)
        string = re.sub("^ |$|™|®|•|…", "", string)
        string = re.sub("♀", "female", string)
        string = re.sub("♂", "male", string)
        string = self._deps["unidecode"](string)
        string = string.strip()
        return self._deps["nltk_tokenize"].sent_tokenize(string)

    def abstract2NERinputs(self, abstract: str) -> List[NERInput]:
        ner_inputs = []
        for guid, sentence in enumerate(self.str2sents(abstract)):
            words = self._deps["nltk_tokenize"].word_tokenize(sentence)
            ner_inputs.append(NERInput(str(guid), words, ["O" for _ in range(len(words))]))
        return ner_inputs

    def convert_NERinputs_to_features(
        self,
        ner_inputs: List[NERInput],
        model_config,
        bert_tokenizer,
        cls_token_at_end=False,
        cls_token="[CLS]",
        cls_token_segment_id=1,
        sep_token="[SEP]",
        sep_token_extra=False,
        pad_on_left=False,
        pad_token_segment_id=0,
        pad_token_label_id=-100,
        sequence_a_segment_id=0,
        mask_padding_with_zero=True,
    ) -> List[InputFeatures]:
        label2id = model_config.label2id
        pad_token = model_config.pad_token_id
        max_seq_length = model_config.max_position_embeddings
        features = []

        for ner_input in ner_inputs:
            tokens = []
            label_ids = []
            for word, label in zip(ner_input.words, ner_input.labels):
                word_tokens = bert_tokenizer.tokenize(word)
                if len(word_tokens) > 0:
                    tokens.extend(word_tokens)
                    label_ids.extend([label2id[label]] + [pad_token_label_id] * (len(word_tokens) - 1))

            special_tokens_count = bert_tokenizer.num_special_tokens_to_add()
            if len(tokens) > max_seq_length - special_tokens_count:
                tokens = tokens[: (max_seq_length - special_tokens_count)]
                label_ids = label_ids[: (max_seq_length - special_tokens_count)]

            tokens += [sep_token]
            label_ids += [pad_token_label_id]
            if sep_token_extra:
                tokens += [sep_token]
                label_ids += [pad_token_label_id]
            segment_ids = [sequence_a_segment_id] * len(tokens)

            if cls_token_at_end:
                tokens += [cls_token]
                label_ids += [pad_token_label_id]
                segment_ids += [cls_token_segment_id]
            else:
                tokens = [cls_token] + tokens
                label_ids = [pad_token_label_id] + label_ids
                segment_ids = [cls_token_segment_id] + segment_ids

            input_ids = bert_tokenizer.convert_tokens_to_ids(tokens)
            input_mask = [1 if mask_padding_with_zero else 0] * len(input_ids)
            padding_length = max_seq_length - len(input_ids)

            if pad_on_left:
                input_ids = ([pad_token] * padding_length) + input_ids
                input_mask = ([0 if mask_padding_with_zero else 1] * padding_length) + input_mask
                segment_ids = ([pad_token_segment_id] * padding_length) + segment_ids
                label_ids = ([pad_token_label_id] * padding_length) + label_ids
            else:
                input_ids += [pad_token] * padding_length
                input_mask += [0 if mask_padding_with_zero else 1] * padding_length
                segment_ids += [pad_token_segment_id] * padding_length
                label_ids += [pad_token_label_id] * padding_length

            if "token_type_ids" not in bert_tokenizer.model_input_names:
                segment_ids = None

            features.append(
                InputFeatures(
                    input_ids=input_ids,
                    attention_mask=input_mask,
                    token_type_ids=segment_ids,
                    label_ids=label_ids,
                )
            )

        return features


class GARDSearch:
    """Local disease dictionary lookup used when extract_diseases=True."""

    def __init__(self) -> None:
        deps = _load_extraction_dependencies()
        from nltk.corpus import stopwords

        with resources.files("epi4gard_local.data").joinpath("gard-id-name-synonyms.json").open("r", encoding="utf-8-sig") as handle:
            diseases = json.load(handle)

        stop_words = set(stopwords.words("english"))
        gard_id_list = [entry["gard_id"] for entry in diseases]
        gard_dict = {}
        max_length = -1

        for entry in diseases:
            names = [entry.get("name")] + list(entry.get("synonyms") or [])
            for name in names:
                if not name:
                    continue
                value = name.lower().strip()
                if value not in stop_words and len(value) > 5 and value not in gard_dict:
                    gard_dict[value] = entry["gard_id"]
                    max_length = max(max_length, len(value.split()))

        self._tokenize = deps["nltk_tokenize"]
        self.GARD_id_list = gard_id_list
        self.GARD_dict = gard_dict
        self.max_length = max_length

    def __call__(self, sentence: str) -> Tuple[List[str], List[str]]:
        return self.get_diseases(sentence)

    def get_diseases(self, sentence: str) -> Tuple[List[str], List[str]]:
        tokens = [token.lower().strip() for token in self._tokenize.word_tokenize(sentence)]
        diseases = []
        ids = []
        index = 0

        while index < len(tokens):
            compare_length = min(len(tokens) - index, self.max_length)
            while compare_length > 0:
                value = " ".join(tokens[index:index + compare_length])
                gard_id = self.GARD_dict.get(value.lower())
                if gard_id:
                    diseases.append(value)
                    ids.append(gard_id)
                    index += compare_length - 1
                    break
                compare_length -= 1
            index += 1

        return diseases, ids


@lru_cache(maxsize=4)
def get_extract_pipeline(
    model_name_or_path: str = DEFAULT_EXTRACT_MODEL,
    device: Optional[str] = None,
    cache_dir: Optional[str] = None,
    local_files_only: bool = True,
) -> EpiExtractPipeline:
    return EpiExtractPipeline(
        model_name_or_path=model_name_or_path,
        device=device,
        cache_dir=cache_dir,
        local_files_only=local_files_only,
    )


@lru_cache(maxsize=1)
def get_gard_search() -> GARDSearch:
    return GARDSearch()


def clear_extract_cache() -> None:
    get_extract_pipeline.cache_clear()
    get_gard_search.cache_clear()


def api_text_extraction(
    text: str,
    epi_ner: EpiExtractPipeline,
    gard_search: Optional[GARDSearch],
    extract_diseases: bool,
) -> ExtractionResult:
    ordered_labels = order_labels(epi_ner.labels)
    if extract_diseases:
        json_output = ["ABSTRACT", "IDS", "DIS"] + ordered_labels
    else:
        json_output = ["ABSTRACT"] + ordered_labels

    extraction = epi_ner(text, gard_search if extract_diseases else None)

    if extraction:
        return OrderedDict([(term, extraction[term]) for term in json_output if term in extraction.keys()])

    return OrderedDict([(term, []) for term in json_output])


def post_epi_extract_text(
    text: str,
    extract_diseases: bool = False,
    model_name_or_path: str = DEFAULT_EXTRACT_MODEL,
    device: Optional[str] = None,
    cache_dir: Optional[str] = None,
    local_files_only: bool = True,
) -> ExtractionResult:
    pipeline = get_extract_pipeline(
        model_name_or_path=model_name_or_path,
        device=device,
        cache_dir=cache_dir,
        local_files_only=local_files_only,
    )
    return pipeline.post_epi_extract_text(text, extract_diseases=extract_diseases)


def postEpiExtractText(
    text: str,
    extract_diseases: bool = False,
    model_name_or_path: str = DEFAULT_EXTRACT_MODEL,
    device: Optional[str] = None,
    cache_dir: Optional[str] = None,
    local_files_only: bool = True,
) -> ExtractionResult:
    return post_epi_extract_text(
        text,
        extract_diseases=extract_diseases,
        model_name_or_path=model_name_or_path,
        device=device,
        cache_dir=cache_dir,
        local_files_only=local_files_only,
    )
