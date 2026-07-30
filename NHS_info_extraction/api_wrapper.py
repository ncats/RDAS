from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager
import os
import uvicorn
import json
import time
from dotenv import load_dotenv
from vllm import LLM, SamplingParams

from extraction_core import (
    TerminologyEnhancer,
    create_extraction_prompt,
    extract_json_from_text,
    process_abstracts
)
load_dotenv()

MODEL_NAME = "gemma3-27b"

# Model configuration, populated from environment variables (see .env.example)
MODEL_CONFIG = {
    "path": os.environ["MODEL_PATH"],
    "tensor_parallel_size": int(os.getenv("TENSOR_PARALLEL_SIZE", 4)),
    "gpu_memory_utilization": float(os.getenv("GPU_MEMORY_UTILIZATION", 0.90)),
    "max_model_len": int(os.getenv("MAX_MODEL_LEN", 3072)),
    "temperature": float(os.getenv("TEMPERATURE", 0.1)),
    "max_tokens": int(os.getenv("MAX_TOKENS", 2048)),
    "top_p": float(os.getenv("TOP_P", 0.95)),
    "stop": ["<END_JSON>", "</s>"]
}

# Default settings
BATCH_SIZE = 10

# Global variables for the model
loaded_llm = None
sampling_params = None
terminology_enhancer = None


# Pydantic models for request/response
class AbstractRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "abstract": "A retrospective cohort study...",
                "enhance_terminology": True
            }
        }
    )

    abstract: str = Field(..., description="The clinical trial or natural history study abstract to process")
    enhance_terminology: bool = Field(default=True, description="Whether to enhance with GARD, HPO, and RxNorm IDs")


class BatchAbstractRequest(BaseModel):
    abstracts: List[str] = Field(..., description="List of abstracts to process")
    enhance_terminology: bool = Field(default=True, description="Whether to enhance with GARD, HPO, and RxNorm IDs")
    batch_size: int = Field(default=BATCH_SIZE, ge=1, le=50, description="Batch size for processing")


class ExtractedCharacteristics(BaseModel):
    disease_name: Optional[str] = None
    study_purpose: Optional[str] = None
    study_type: Optional[str] = None
    participants_count: Optional[str] = None
    data_collection_period: Optional[str] = None
    inclusion_criteria: Optional[str] = None
    exclusion_criteria: Optional[str] = None
    clinical_outcomes: Optional[str] = None
    treatments_received: Optional[str] = None
    study_duration: Optional[str] = None
    results: Optional[str] = None
    parse_error: bool = False
    raw_response: Optional[str] = None


class ExtractionResponse(BaseModel):
    success: bool
    abstract: str
    extracted_characteristics: ExtractedCharacteristics
    processing_time_seconds: float
    enhanced_with_terminology: bool


class BatchExtractionResponse(BaseModel):
    success: bool
    total_abstracts: int
    successful_extractions: int
    failed_extractions: int
    results: List[ExtractionResponse]
    total_processing_time_seconds: float
    terminology_stats: Optional[Dict[str, Any]] = None


class HealthCheckResponse(BaseModel):
    status: str
    model_loaded: bool
    model_name: str
    timestamp: float


def get_or_load_model() -> tuple[LLM, SamplingParams]:
    """Get the model from cache, or load it"""
    global loaded_llm, sampling_params

    if loaded_llm is not None:
        return loaded_llm, sampling_params

    print(f"Loading model: {MODEL_NAME} from {MODEL_CONFIG['path']}...")

    try:
        loaded_llm = LLM(
            model=MODEL_CONFIG["path"],
            tensor_parallel_size=MODEL_CONFIG["tensor_parallel_size"],
            gpu_memory_utilization=MODEL_CONFIG["gpu_memory_utilization"],
            max_model_len=MODEL_CONFIG["max_model_len"]
        )

        sampling_params = SamplingParams(
            temperature=MODEL_CONFIG["temperature"],
            max_tokens=MODEL_CONFIG["max_tokens"],
            top_p=MODEL_CONFIG["top_p"],
            stop=MODEL_CONFIG["stop"],
        )

        print(f"Model {MODEL_NAME} loaded successfully!")
        return loaded_llm, sampling_params

    except Exception as e:
        print(f"Error loading model {MODEL_NAME}: {e}")
        raise


# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global terminology_enhancer, loaded_llm, sampling_params

    print("Initializing API...")
    print(f"Model: {MODEL_NAME}")

    try:
        # Load the model
        get_or_load_model()

        # Initialize terminology enhancer with verbose=False for API
        terminology_enhancer = TerminologyEnhancer(
            enable_api_calls=True,
            timeout=10,
            verbose=False
        )
        
        print("Model initialization complete!")
    except Exception as e:
        print(f"Error initializing model: {e}")
        raise
    
    yield
    
    # Shutdown
    print("Shutting down...")
    loaded_llm = None
    sampling_params = None
    terminology_enhancer = None


# Initialize FastAPI app
app = FastAPI(
    title="Clinical Abstract Extraction API",
    description="Extract structured characteristics from clinical trial and natural history study abstracts using Gemma3-27b",
    version="1.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_model=Dict[str, Any])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Clinical Abstract Extraction API",
        "version": "1.0.0",
        "model": MODEL_NAME,
        "model_loaded": loaded_llm is not None,
        "endpoints": {
            "health": "/health",
            "models": "/models",
            "extract": "/extract (POST)",
            "extract_batch": "/extract/batch (POST)",
            "docs": "/docs"
        }
    }


@app.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """Check if the API and model are ready"""
    return HealthCheckResponse(
        status="healthy" if loaded_llm is not None else "not_ready",
        model_loaded=loaded_llm is not None,
        model_name=MODEL_NAME,
        timestamp=time.time()
    )


@app.get("/models")
async def list_models():
    """Show the configured model and its loaded status"""
    return {
        "model_name": MODEL_NAME,
        "model_path": MODEL_CONFIG["path"],
        "loaded": loaded_llm is not None
    }


@app.post("/extract", response_model=ExtractionResponse)
async def extract_characteristics(request: AbstractRequest):
    """
    Extract structured characteristics from a single clinical abstract.
    
    Returns disease name, study details, clinical outcomes, treatments, and more.
    Optionally enhances disease names with GARD IDs, outcomes with HPO IDs,
    and treatments with RxNorm IDs.

    Uses the gemma3-27b model.
    """
    if not request.abstract.strip():
        raise HTTPException(status_code=400, detail="Abstract cannot be empty")

    start_time = time.time()

    try:
        llm, params = get_or_load_model()

        # Process single abstract
        results = process_abstracts(
            llm,
            [request.abstract],
            params,
            batch_size=1
        )

        result = results[0]

        # Enhance with terminology if requested
        if request.enhance_terminology and not result.get("parse_error", False):
            result = terminology_enhancer.enhance_result(result)

        processing_time = time.time() - start_time

        # Build response
        characteristics = ExtractedCharacteristics(**result)

        return ExtractionResponse(
            success=not result.get("parse_error", False),
            abstract=request.abstract,
            extracted_characteristics=characteristics,
            processing_time_seconds=round(processing_time, 2),
            enhanced_with_terminology=request.enhance_terminology
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing abstract: {str(e)}")


@app.post("/extract/batch", response_model=BatchExtractionResponse)
async def extract_batch_characteristics(request: BatchAbstractRequest):
    """
    Extract structured characteristics from multiple clinical abstracts in batch.
    
    More efficient for processing multiple abstracts at once.

    Uses the gemma3-27b model.
    """
    if not request.abstracts:
        raise HTTPException(status_code=400, detail="Abstracts list cannot be empty")

    if len(request.abstracts) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 abstracts per batch request")

    start_time = time.time()

    try:
        llm, params = get_or_load_model()

        # Process all abstracts
        results = process_abstracts(
            llm,
            request.abstracts,
            params,
            batch_size=request.batch_size
        )
        
        # Enhance with terminology if requested
        if request.enhance_terminology:
            enhanced_results = []
            for result in results:
                if not result.get("parse_error", False):
                    enhanced_results.append(terminology_enhancer.enhance_result(result))
                else:
                    enhanced_results.append(result)
            results = enhanced_results
        
        total_processing_time = time.time() - start_time
        
        # Build individual responses
        extraction_responses = []
        successful = 0
        failed = 0
        
        for abstract, result in zip(request.abstracts, results):
            individual_success = not result.get("parse_error", False)
            if individual_success:
                successful += 1
            else:
                failed += 1
            
            characteristics = ExtractedCharacteristics(**result)
            
            extraction_responses.append(ExtractionResponse(
                success=individual_success,
                abstract=abstract,
                extracted_characteristics=characteristics,
                processing_time_seconds=round(total_processing_time / len(request.abstracts), 2),
                enhanced_with_terminology=request.enhance_terminology
            ))
        
        # Get terminology stats if enhancement was used
        terminology_stats = None
        if request.enhance_terminology:
            terminology_stats = terminology_enhancer.stats.copy()
        
        return BatchExtractionResponse(
            success=True,
            total_abstracts=len(request.abstracts),
            successful_extractions=successful,
            failed_extractions=failed,
            results=extraction_responses,
            total_processing_time_seconds=round(total_processing_time, 2),
            terminology_stats=terminology_stats
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing batch: {str(e)}")


@app.get("/stats")
async def get_terminology_stats():
    """Get current terminology enhancement statistics"""
    if terminology_enhancer is None:
        raise HTTPException(status_code=503, detail="Terminology enhancer not initialized")
    
    return {
        "statistics": terminology_enhancer.stats,
        "cache_sizes": {
            "gard_cache": len(terminology_enhancer.gard_cache),
            "hpo_cache": len(terminology_enhancer.hpo_cache),
            "rxnorm_cache": len(terminology_enhancer.rxnorm_cache)
        },
        "model_loaded": loaded_llm is not None
    }


if __name__ == "__main__":
    # Run the API server
    uvicorn.run(
        "api_wrapper:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        workers=1
    )