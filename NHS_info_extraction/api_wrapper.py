from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any, Literal
from contextlib import asynccontextmanager
import uvicorn
import json
import time
from vllm import LLM, SamplingParams

from extraction_core import (
    TerminologyEnhancer,
    create_extraction_prompt,
    extract_json_from_text,
    process_abstracts
)

# Model configurations
MODEL_CONFIGS = {
    "gemma3-27b": {
        "path": "/vast/projects/ncats-llms/gemma3-27b/",
        "tensor_parallel_size": 4,
        "gpu_memory_utilization": 0.90,
        "max_model_len": 3072,
        "temperature": 0.1,
        "max_tokens": 2048,
        "top_p": 0.95,
        "stop": ["<END_JSON>", "</s>"]
    },
    "Llama-3.1-70B-Instruct": {
        "path": "/vast/projects/ncats-llms/Llama-3.1-70B-Instruct/",
        "tensor_parallel_size": 4,
        "gpu_memory_utilization": 0.90,
        "max_model_len": 3072,
        "temperature": 0.1,
        "max_tokens": 2048,
        "top_p": 0.95,
        "stop": ["<|eot_id|>", "</s>", "<END_JSON>"]
    }
}

# Default settings
DEFAULT_MODEL = "Llama-3.1-70B-Instruct"
BATCH_SIZE = 10

# Global variables for models
loaded_models = {}
sampling_params_cache = {}
terminology_enhancer = None


# Pydantic models for request/response
class AbstractRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "abstract": "A retrospective cohort study...",
                "enhance_terminology": True,
                "model_name": "Llama-3.1-70B-Instruct"
            }
        }
    )
    
    abstract: str = Field(..., description="The clinical trial or natural history study abstract to process")
    enhance_terminology: bool = Field(default=True, description="Whether to enhance with HPO and RxNorm IDs")
    model_name: Literal["Llama-3.1-70B-Instruct", "gemma3-27b"] = Field(
        default=DEFAULT_MODEL,
        description="Model to use for extraction"
    )


class BatchAbstractRequest(BaseModel):
    abstracts: List[str] = Field(..., description="List of abstracts to process")
    enhance_terminology: bool = Field(default=True, description="Whether to enhance with HPO and RxNorm IDs")
    batch_size: int = Field(default=BATCH_SIZE, ge=1, le=50, description="Batch size for processing")
    model_name: Literal["Llama-3.1-70B-Instruct", "gemma3-27b"] = Field(
        default=DEFAULT_MODEL,
        description="Model to use for extraction"
    )


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
    model_used: str


class BatchExtractionResponse(BaseModel):
    success: bool
    total_abstracts: int
    successful_extractions: int
    failed_extractions: int
    results: List[ExtractionResponse]
    total_processing_time_seconds: float
    terminology_stats: Optional[Dict[str, Any]] = None
    model_used: str


class HealthCheckResponse(BaseModel):
    status: str
    loaded_models: List[str]
    available_models: List[str]
    default_model: str
    timestamp: float


def get_or_load_model(model_name: str) -> tuple[LLM, SamplingParams]:
    """Get model from cache or load it"""
    global loaded_models, sampling_params_cache
    
    if model_name not in MODEL_CONFIGS:
        raise ValueError(f"Unknown model: {model_name}. Available: {list(MODEL_CONFIGS.keys())}")
    
    # Return cached model if available
    if model_name in loaded_models:
        print(f"Using cached model: {model_name}")
        return loaded_models[model_name], sampling_params_cache[model_name]
    
    # Load new model
    print(f"Loading model: {model_name}...")
    config = MODEL_CONFIGS[model_name]
    
    try:
        llm = LLM(
            model=config["path"],
            tensor_parallel_size=config["tensor_parallel_size"],
            gpu_memory_utilization=config["gpu_memory_utilization"],
            max_model_len=config["max_model_len"]
        )
        
        sampling_params = SamplingParams(
            temperature=config["temperature"],
            max_tokens=config["max_tokens"],
            top_p=config["top_p"],
            stop=config["stop"],
        )
        
        # Cache the model
        loaded_models[model_name] = llm
        sampling_params_cache[model_name] = sampling_params
        
        print(f"Model {model_name} loaded successfully!")
        return llm, sampling_params
        
    except Exception as e:
        print(f"Error loading model {model_name}: {e}")
        raise


# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global terminology_enhancer
    
    print("Initializing API...")
    print(f"Available models: {list(MODEL_CONFIGS.keys())}")
    print(f"Default model: {DEFAULT_MODEL}")
    
    try:
        # Load default model
        print(f"\nLoading default model: {DEFAULT_MODEL}")
        get_or_load_model(DEFAULT_MODEL)
        
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
    loaded_models.clear()
    sampling_params_cache.clear()
    terminology_enhancer = None


# Initialize FastAPI app
app = FastAPI(
    title="Clinical Abstract Extraction API",
    description="Extract structured characteristics from clinical trial and natural history study abstracts using LLM (supports Llama-3.1-70B-Instruct and Gemma3-27b)",
    version="1.0.0",
    lifespan=lifespan
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
        "available_models": list(MODEL_CONFIGS.keys()),
        "default_model": DEFAULT_MODEL,
        "loaded_models": list(loaded_models.keys()),
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
        status="healthy" if loaded_models else "not_ready",
        loaded_models=list(loaded_models.keys()),
        available_models=list(MODEL_CONFIGS.keys()),
        default_model=DEFAULT_MODEL,
        timestamp=time.time()
    )


@app.get("/models")
async def list_models():
    """List all available models and their loaded status"""
    return {
        "available_models": list(MODEL_CONFIGS.keys()),
        "loaded_models": list(loaded_models.keys()),
        "default_model": DEFAULT_MODEL,
        "model_configs": {
            name: {
                "path": config["path"],
                "loaded": name in loaded_models
            }
            for name, config in MODEL_CONFIGS.items()
        }
    }


@app.post("/extract", response_model=ExtractionResponse)
async def extract_characteristics(request: AbstractRequest):
    """
    Extract structured characteristics from a single clinical abstract.
    
    Returns disease name, study details, clinical outcomes, treatments, and more.
    Optionally enhances outcomes with HPO IDs and treatments with RxNorm IDs.
    
    Supports models: Llama-3.1-70B-Instruct (default), gemma3-27b 
    """
    if not request.abstract.strip():
        raise HTTPException(status_code=400, detail="Abstract cannot be empty")
    
    start_time = time.time()
    
    try:
        # Get or load the requested model
        llm, sampling_params = get_or_load_model(request.model_name)
        
        # Process single abstract
        results = process_abstracts(
            llm, 
            [request.abstract], 
            sampling_params, 
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
            enhanced_with_terminology=request.enhance_terminology,
            model_used=request.model_name
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
    
    Supports models: Llama-3.1-70B-Instruct (default), gemma3-27b 
    """
    if not request.abstracts:
        raise HTTPException(status_code=400, detail="Abstracts list cannot be empty")
    
    if len(request.abstracts) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 abstracts per batch request")
    
    start_time = time.time()
    
    try:
        # Get or load the requested model
        llm, sampling_params = get_or_load_model(request.model_name)
        
        # Process all abstracts
        results = process_abstracts(
            llm, 
            request.abstracts, 
            sampling_params, 
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
                enhanced_with_terminology=request.enhance_terminology,
                model_used=request.model_name
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
            terminology_stats=terminology_stats,
            model_used=request.model_name
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
            "hpo_cache": len(terminology_enhancer.hpo_cache),
            "rxnorm_cache": len(terminology_enhancer.rxnorm_cache)
        },
        "loaded_models": list(loaded_models.keys())
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