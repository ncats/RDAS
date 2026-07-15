FROM nvidia/cuda:12.8.0-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-venv \
    python3-pip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1

COPY requirements.txt /app/requirements.txt

RUN python -m pip install --upgrade pip && \
    python -m pip install --prefer-binary -r /app/requirements.txt

COPY api_wrapper.py extraction_core.py config.py /app/

# LLM / runtime configuration (override at `docker run` with -e or --env-file).
# Model configs default to the built-in values in config.py. To fully customise
# them, mount a JSON file and point MODEL_CONFIG_PATH at it.
ENV DEFAULT_MODEL="Llama-3.1-70B-Instruct" \
    BATCH_SIZE=10 \
    API_HOST="0.0.0.0" \
    API_PORT=8000 \
    API_WORKERS=1 \
    CORS_ALLOW_ORIGINS="" \
    CORS_ALLOW_METHODS="GET,POST" \
    CORS_ALLOW_HEADERS="*" \
    ENABLE_TERMINOLOGY_API=true \
    TERMINOLOGY_TIMEOUT=10 \
    TERMINOLOGY_VERBOSE=false
# Optional, unset by default:
#   MODEL_CONFIG_PATH=/config/model_configs.json
#   MODEL_PATH_LLAMA_3_1_70B_INSTRUCT=/models/Llama-3.1-70B-Instruct
#   MODEL_PATH_GEMMA3_27B=/models/gemma3-27b
#   TENSOR_PARALLEL_SIZE=4
#   GPU_MEMORY_UTILIZATION=0.90
#   MAX_MODEL_LEN=3072
#   TERMINOLOGY_PROXY_URL=http://proxy:8080
# CORS is locked down by default (no origins allowed). Set CORS_ALLOW_ORIGINS to
# a comma-separated list of trusted front-end origins, e.g.:
#   CORS_ALLOW_ORIGINS="https://app.example.org,https://admin.example.org"
# Credentials are disabled by default; enable only with an explicit origin list:
#   CORS_ALLOW_CREDENTIALS=true

# Run as an unprivileged user instead of root.
RUN groupadd --system --gid 10001 appuser \
    && useradd --system --uid 10001 --gid 10001 --no-create-home appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

CMD ["python", "api_wrapper.py"]
