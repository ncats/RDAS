# epi4gard_local

Local replacement layer for RDAS calls that used to go through HTTP:

```text
EPI_CLASSIFY_API=https://rdas.ncats.nih.gov/api/epi/postEpiClassifyText/
EPI_EXTRACT_API=https://rdas.ncats.nih.gov/api/epi/postEpiExtractText/
NHS_PREDICT_API=https://rdas.ncats.nih.gov/api/article_prediction/v1/predict
```

## EPI Classification

```python
from epi4gard_local import postEpiClassifyText

result = postEpiClassifyText("Your abstract text here.")
```

Returns the same shape as `postEpiClassifyText`:

```python
{"ABSTRACT": text, "EPI_PROB": "0.12345", "IsEpi": False}
```

For large jobs, load the model once and use batching:

```python
from epi4gard_local import EpiClassifyTextPipeline

classifier = EpiClassifyTextPipeline(local_files_only=True)

for result in classifier.iter_classify_texts(texts, batch_size=64):
    print(result)
```

## EPI Extraction

```python
from epi4gard_local import postEpiExtractText

result = postEpiExtractText("Your abstract text here.", extract_diseases=False)
```

The extraction code and GARD synonym JSON are vendored in this package. Model
weights are loaded through Hugging Face from `ncats/EpiExtract4GARD-v2` or from a
local path/cache.

## NHS Prediction

```python
from epi4gard_local import predict_article

result = predict_article(["Your abstract text here."])
```

The local predictor follows the NCATS
`NaturalHistory_Transformer_API_v1.0` implementation: a
`transformers.AutoTokenizer` and `AutoModelForSequenceClassification` model with
`max_length=256`, returning `{"predictions": [0 or 1, ...]}`.

By default, pipeline code uses local files/cache only. Pre-download the public
model once, or point to a local saved-model directory:

```bash
conda run -n rdas python - <<'PY'
from transformers import AutoModelForSequenceClassification, AutoTokenizer

model = "NIHNCATS/NHS-BiomedNLP-BiomedBERT-hypop"
AutoTokenizer.from_pretrained(model)
AutoModelForSequenceClassification.from_pretrained(model)
print(f"Downloaded {model}")
PY

export NHS_MODEL_PATH=/path/to/NaturalHistory_Transformer_API_v1.0/app/saved_model/my_BiomedNLP-BiomedBERT_model
```

For a one-off interactive download through the command line, omit
`--local-files-only`:

```bash
conda run -n rdas python -m epi4gard_local --endpoint nhs --text "Natural history study abstract."
```

Tests can still inject a process-local predictor with
`configure_nhs_predictor(...)`.

## Endpoint Dispatcher

```python
from epi4gard_local import EPI_CLASSIFY_API, local_post

result = local_post(EPI_CLASSIFY_API, {"text": "Your abstract text here."})
```

## Smoke Tests

```bash
conda run -n rdas python epi4gard_local_smoke_test.py
conda run -n rdas python epi4gard_local_smoke_test.py --run-classify --local-files-only
conda run -n rdas python epi4gard_local_smoke_test.py --run-extract --local-files-only
```

Command-line module:

```bash
conda run -n rdas python -m epi4gard_local --endpoint classify --text "A population-based study estimated disease prevalence." --local-files-only
conda run -n rdas python -m epi4gard_local --endpoint extract --text "A study reported prevalence of 1 in 100000 people." --local-files-only
conda run -n rdas python -m epi4gard_local --endpoint nhs --text "A natural history study followed patients over time." --local-files-only
```
