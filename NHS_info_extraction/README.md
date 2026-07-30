# Overview
This AI-powered system consists of an API wrapper for a script designed to extract important characteristics from natural history studies and clinical trials involving rare diseases, with help from Google's Gemma3-27b large language model along with enhancements using GARD (Genetic and Rare Diseases Information Center), HPO (Human Phenotype Ontology), and RxNorm APIs. When employed, this system searches for features beyond a clinical abstract's basic features like the disease being observed or the duration of the study, with this system being able to interpret the disease's phenotypes being shown in patients, any treatments patients might have received, the study's inclusion/exclusion criteria, and many other characteristics researchers will find useful when determining a rare disease's time course and potential therapies to pursue. 

# Features
 - Extracts 11 characteristics from clinical abstracts 
    - Disease Name
    - Study Purpose
    - Study Type
    - Participant Count
    - Data Collection Period
    - Inclusion Criteria
    - Exclusion Criteria
    - Clinical Outcomes
    - Treatments Received
    - Study Duration
    - Study Results
 - Optional enhancements using GARD, HPO, and RxNorm APIs (may be inaccurate)
 - Support for processing multiple abstracts at a time
 - REST API made with FastAPI
 - Built-in statistics and monitoring

# System Requirements
This system was built in Python 3.11.14 and uses CUDA 12.8 to load the provided large language models. 

## Python packages needed for use:
- Web API Framework
    - fastapi
    - uvicorn
- Data Validation & Models
    - pydantic
- LLM & Machine Learning
    - vllm
    - torch
    - transformers
- External API Calls
    - requests
- Configuration
    - python-dotenv
- Optional/Development
    - python-multipart

## Configuration
The model path and vLLM/sampling settings are read from environment variables at
startup (via `python-dotenv`), instead of being hardcoded, so this can point to
a different model location in a different environment.

Copy `.env.example` to `.env` and edit `MODEL_PATH` (and, if needed, the other
values) to match where the gemma3-27b model lives on your machine:

```
cp .env.example .env
```

`.env` is environment-specific and should not be committed to version control.