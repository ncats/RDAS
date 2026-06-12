# Model And Data Assets To Install

This checklist is generated from Python imports, `spacy.load(...)` calls,
`from_pretrained(...)` calls, and model-install comments in this repo.

Start with the normal Python dependencies:

```bash
pip install -r requirements.txt
```

Then install or pre-download the model/data assets below.

## spaCy Models

### `en_core_web_sm`

Used by:

- `Z_Alert/pipelines/pipeline_4_grant/task_grant_10.py`
- `Z_Alert/pipelines/pipeline_3_publication/task_publication_7.py`
- `C_publication/init_8_publication-filter-out-false-positives-of-article.py`
- `D_grant/init_9_GARD_and_Project_relationship*.py`

Install:

```bash
python -m spacy download en_core_web_sm
```

## sciSpaCy Models

These are used by grant and clinical-trial annotation tasks that load
`en_ner_bionlp13cg_md`, `en_ner_bc5cdr_md`, and the UMLS linker.

Used by:

- `Z_Alert/pipelines/pipeline_4_grant/task_grant_11.py`
- `D_grant/init_11_Project_annotation_generator.py`
- `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_6.py`
- `B_clinical_trial/init_9_clinical_trail_annotation_generator.py`
- `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_3.py`
- `B_clinical_trial/init_3_clinical_trial_step_3.py`
- `B_clinical_trial/cypher_helpers.py`

Install commands preserved from the code comments:

```bash
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_core_sci_lg-0.5.3.tar.gz
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bionlp13cg_md-0.5.3.tar.gz
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bc5cdr_md-0.5.3.tar.gz
```

Notes:

- `en_ner_bionlp13cg_md` and `en_ner_bc5cdr_md` are directly loaded by the
  current alert tasks.
- `en_core_sci_lg` appears in install comments. It is not directly loaded by
  `task_grant_11.py`, but installing it keeps the environment aligned with the
  original initializer notes.
- Keep sciSpaCy model versions compatible with the installed `spacy` and
  `scispacy` versions in `requirements.txt`. If those package versions change,
  use matching sciSpaCy model tarballs.

## sciSpaCy UMLS Linker Data

The annotation tasks add:

```python
nlp.add_pipe("scispacy_linker", config={"linker_name": "umls"})
```

The UMLS linker may download or load its knowledge base the first time it runs.
To warm the cache after installing the models:

```bash
python - <<'PY'
import spacy
import scispacy
from scispacy.linking import EntityLinker

for model_name in ("en_ner_bionlp13cg_md", "en_ner_bc5cdr_md"):
    nlp = spacy.load(model_name)
    if "scispacy_linker" not in nlp.pipe_names:
        nlp.add_pipe("scispacy_linker", config={"linker_name": "umls"})
    print(f"Loaded {model_name} with UMLS linker")
PY
```

## Hugging Face ClinicalBERT

`task_grant_10.py` and the original grant relationship scripts load:

```text
emilyalsentzer/Bio_ClinicalBERT
```

This model is loaded through `transformers.AutoTokenizer.from_pretrained(...)`
and `transformers.AutoModel.from_pretrained(...)`. It is not installed with
`pip`; it is downloaded into the Hugging Face cache.

Pre-download/check:

```bash
python - <<'PY'
from transformers import AutoModel, AutoTokenizer

model_name = "emilyalsentzer/Bio_ClinicalBERT"
AutoTokenizer.from_pretrained(model_name)
AutoModel.from_pretrained(model_name)
print(f"Downloaded {model_name}")
PY
```

If the machine needs authenticated Hugging Face access, set:

```bash
export HUGGINGFACE_TOKEN="your-token"
```

## NLTK Data

`utils/tools.py` imports and uses NLTK tokenizers/corpora:

- `punkt`
- `stopwords`
- `words`

The grant comments also preserve an `english_wordnet` download note.

Recommended install through the NLTK downloader:

```bash
python -m nltk.downloader punkt stopwords words wordnet omw-1.4
```

Original wget-style notes preserved from the grant scripts:

```bash
mkdir -p ~/nltk_data
cd ~/nltk_data
wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/tokenizers/punkt.zip
wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/stopwords.zip
wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/english_wordnet.zip
wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/words.zip
```

## Local Organization-Name Extraction Model

`utils/organization_name_extractor.py` calls a configured local model API. The
code does not hardcode a model package to install; it reads environment
variables:

```bash
export ORG_NAME_EXTRACT_MODEL="your-local-model-name"
export ORG_NAME_EXTRACT_BASE_URL="http://localhost:11434"
export MODEL_START_COMMAND="ollama serve"
```

Older update scripts include a processed flag containing `llama3.1`, so that
model may be what the previous environment used. Confirm the exact local model
name before migration.

## Quick Verification

Run this after installing the assets:

```bash
python - <<'PY'
import nltk
import spacy
from transformers import AutoModel, AutoTokenizer

for model_name in ("en_core_web_sm", "en_ner_bionlp13cg_md", "en_ner_bc5cdr_md"):
    spacy.load(model_name)
    print(f"spaCy model OK: {model_name}")

for nltk_path in (
    "tokenizers/punkt",
    "corpora/stopwords",
    "corpora/words",
):
    nltk.data.find(nltk_path)
    print(f"NLTK data OK: {nltk_path}")

model_name = "emilyalsentzer/Bio_ClinicalBERT"
AutoTokenizer.from_pretrained(model_name)
AutoModel.from_pretrained(model_name)
print(f"Hugging Face model OK: {model_name}")
PY
```

