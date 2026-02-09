"""
Space B (The Factory) - AI Inference Microservice

This service handles heavy AI workloads offloaded from Space A:
- Llama-3 text summarization (GGUF quantized for CPU)
- GLiNER named entity recognition
- Edge-TTS audio generation

Optimized for: 2 vCPU, 16GB RAM, HuggingFace Free Tier
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import List, Optional, Dict

import edge_tts
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from gliner import GLiNER
from huggingface_hub import hf_hub_download
from llama_cpp import Llama
from pydantic import BaseModel, Field

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global model instances (loaded at startup)
llama_model: Optional[Llama] = None
gliner_model: Optional[GLiNER] = None
startup_time = time.time()


# ============================================================================
# Pydantic Models (Request/Response Schemas)
# ============================================================================

class SummarizeRequest(BaseModel):
    text: str = Field(..., description="Text to summarize", min_length=10)
    max_tokens: int = Field(150, description="Maximum summary length", ge=50, le=500)
    temperature: float = Field(0.7, description="Sampling temperature", ge=0.0, le=2.0)


class SummarizeResponse(BaseModel):
    summary: str
    model: str
    inference_time_ms: int


class ExtractRequest(BaseModel):
    text: str = Field(..., description="Text for entity extraction", min_length=5)
    labels: List[str] = Field(
        ["Person", "Organization", "Location"],
        description="Entity types to extract"
    )
    threshold: float = Field(0.5, description="Confidence threshold", ge=0.0, le=1.0)


class Entity(BaseModel):
    text: str
    label: str
    score: float


class ExtractResponse(BaseModel):
    entities: List[Entity]
    model: str
    inference_time_ms: int


class AudioRequest(BaseModel):
    text: str = Field(..., description="Text to convert to speech", min_length=1)
    voice: str = Field(
        "en-US-ChristopherNeural",
        description="Edge-TTS voice name"
    )
    rate: str = Field("+0%", description="Speech rate (-50% to +100%)")
    volume: str = Field("+0%", description="Volume (-50% to +50%)")


class ProcessArticleRequest(BaseModel):
    text: str = Field(..., description="Article text to process", min_length=10)
    max_tokens: int = Field(150, description="Maximum summary length", ge=50, le=500)
    temperature: float = Field(0.7, description="Sampling temperature", ge=0.0, le=2.0)
    entity_labels: List[str] = Field(
        ["Person", "Organization", "Location", "Technology", "Product"],
        description="Entity types to extract"
    )
    entity_threshold: float = Field(0.5, description="Confidence threshold", ge=0.0, le=1.0)


class ProcessArticleResponse(BaseModel):
    summary: str
    tags: List[str]
    entities: List[Entity]
    processing_time_ms: int
    model_info: Dict[str, str]


class HealthResponse(BaseModel):
    status: str
    models_loaded: bool
    uptime_seconds: int
    llama_loaded: bool
    gliner_loaded: bool


# ============================================================================
# Model Loading (Startup Event)
# ============================================================================

async def load_models():
    """
    Load all AI models into memory (LAZY LOADING)
    
    CRITICAL: Models are loaded ONLY when first accessed to avoid OOM at startup.
    This prevents Exit Code 137 on HuggingFace Free Tier (16GB RAM limit).
    """
    global llama_model, gliner_model
    
    logger.info("=" * 80)
    logger.info("🏭 [SPACE B] Model loading strategy: LAZY (on-demand)")
    logger.info("=" * 80)
    logger.info("⚡ Models will load on first request to conserve memory")
    logger.info("🔧 This prevents OOM during startup")
    logger.info("=" * 80)


def load_llama_model():
    """Load Llama-3 model on first use"""
    global llama_model
    
    if llama_model is not None:
        return llama_model
    
    try:
        logger.info("📥 [LAZY] Loading Llama-3-8B-Instruct (Q4_K_M)...")
        
        from huggingface_hub import hf_hub_download
        from llama_cpp import Llama
        
        # Download from HuggingFace Hub
        model_path = hf_hub_download(
            repo_id="QuantFactory/Meta-Llama-3-8B-Instruct-GGUF",
            filename="Meta-Llama-3-8B-Instruct.Q4_K_M.gguf",
            cache_dir="/app/models"
        )
        
        logger.info(f"✅ Model downloaded to: {model_path}")
        logger.info("🔧 Loading Llama-3 into memory...")
        
        # Load with CPU optimizations
        llama_model = Llama(
            model_path=model_path,
            n_ctx=2048,  # Context window (tokens)
            n_threads=2,  # Use both vCPUs
            n_batch=512,  # Batch size for prompt processing
            verbose=False  # Suppress llama.cpp logs
        )
        
        logger.info("✅ Llama-3 loaded successfully!")
        logger.info(f"   📊 Model size: ~4.5GB RAM")
        logger.info(f"   🔢 Context length: 2048 tokens")
        
        return llama_model
        
    except Exception as e:
        logger.error(f"❌ Failed to load Llama-3: {e}")
        raise


def load_gliner_model():
    """Load GLiNER model on first use"""
    global gliner_model
    
    if gliner_model is not None:
        return gliner_model
    
    try:
        logger.info("📥 [LAZY] Loading GLiNER (small-v2.1)...")
        
        from gliner import GLiNER
        
        gliner_model = GLiNER.from_pretrained("urchade/gliner_small-v2.1")
        
        logger.info("✅ GLiNER loaded successfully!")
        logger.info(f"   📊 Model size: ~200MB RAM")
        logger.info(f"   🎯 Zero-shot NER ready")
        
        return gliner_model
        
    except Exception as e:
        logger.error(f"❌ Failed to load GLiNER: {e}")
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager
    
    Loads models at startup and cleans up at shutdown
    """
    # Startup: Load models
    await load_models()
    
    yield  # Application runs here
    
    # Shutdown: Cleanup (if needed)
    logger.info("👋 [SPACE B] Shutting down...")


# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="Space B - The Factory",
    description="AI Inference Microservice for Segmento Pulse",
    version="1.0.0",
    lifespan=lifespan
)


# ============================================================================
# Endpoints
# ============================================================================

@app.get("/", tags=["Info"])
async def root():
    """Root endpoint with service info"""
    return {
        "service": "Space B - The Factory",
        "description": "AI inference microservice for heavy workloads",
        "version": "1.0.0",
        "endpoints": {
            "process-article": "/process-article (POST) - Composite endpoint",
            "summarize": "/summarize (POST)",
            "extract": "/extract (POST)",
            "audio": "/audio (POST)",
            "health": "/health (GET)"
        }
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint
    
    CRITICAL: This must respond quickly (<1s) for HuggingFace monitoring.
    Do NOT perform heavy operations here.
    
    With lazy loading, models may not be loaded at startup - this is expected.
    """
    uptime = int(time.time() - startup_time)
    
    return HealthResponse(
        status="healthy",
        models_loaded=llama_model is not None and gliner_model is not None,
        uptime_seconds=uptime,
        llama_loaded=llama_model is not None,
        gliner_loaded=gliner_model is not None
    )


@app.post("/summarize", response_model=SummarizeResponse, tags=["AI"])
async def summarize_text(request: SummarizeRequest):
    """
    Generate text summary using Llama-3
    
    Uses quantized GGUF model for CPU-optimized inference.
    Typical inference time: 5-10 seconds on 2 vCPU.
    """
    # Lazy load model on first request
    model = load_llama_model()
    if model is None:
        raise HTTPException(status_code=503, detail="Llama model failed to load")
    
    start_time = time.time()
    
    try:
        # Construct prompt (Llama-3-Instruct format)
        prompt = f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>

You are a professional news summarizer. Create concise, accurate summaries.<|eot_id|><|start_header_id|>user<|end_header_id|>

Summarize the following article in 2-3 sentences:

{request.text[:2000]}  

Summary:<|eot_id|><|start_header_id|>assistant<|end_header_id|>

"""
        
        logger.info(f"🔮 Generating summary (max_tokens={request.max_tokens})...")
        
        # Run inference in thread pool (llama.cpp is synchronous)
        loop = asyncio.get_event_loop()
        output = await loop.run_in_executor(
            None,  # Use default thread pool
            lambda: model(
                prompt,
                max_tokens=request.max_tokens,
                temperature=request.temperature,
                stop=["<|eot_id|>", "\n\n"],
                echo=False
            )
        )
        
        # Extract generated text
        summary = output['choices'][0]['text'].strip()
        
        inference_time = int((time.time() - start_time) * 1000)
        
        logger.info(f"✅ Summary generated in {inference_time}ms")
        
        return SummarizeResponse(
            summary=summary,
            model="Llama-3-8B-Instruct-Q4_K_M",
            inference_time_ms=inference_time
        )
        
    except Exception as e:
        logger.error(f"❌ Summarization error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/extract", response_model=ExtractResponse, tags=["AI"])
async def extract_entities(request: ExtractRequest):
    """
    Extract named entities using GLiNER
    
    Zero-shot NER - can extract any entity type without training.
    Typical inference time: 50-200ms on CPU.
    """
    # Lazy load model on first request
    model = load_gliner_model()
    if model is None:
        raise HTTPException(status_code=503, detail="GLiNER model failed to load")
    
    start_time = time.time()
    
    try:
        logger.info(f"🔍 Extracting entities: {request.labels}")
        
        # Run GLiNER inference in thread pool
        loop = asyncio.get_event_loop()
        raw_entities = await loop.run_in_executor(
            None,
            lambda: model.predict_entities(
                request.text,
                request.labels,
                threshold=request.threshold
            )
        )
        
        # Convert to response format
        entities = [
            Entity(
                text=entity['text'],
                label=entity['label'],
                score=round(entity['score'], 3)
            )
            for entity in raw_entities
        ]
        
        inference_time = int((time.time() - start_time) * 1000)
        
        logger.info(f"✅ Extracted {len(entities)} entities in {inference_time}ms")
        
        return ExtractResponse(
            entities=entities,
            model="GLiNER-small-v2.1",
            inference_time_ms=inference_time
        )
        
    except Exception as e:
        logger.error(f"❌ Entity extraction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/process-article", response_model=ProcessArticleResponse, tags=["AI"])
async def process_article(request: ProcessArticleRequest):
    """
    **COMPOSITE ENDPOINT** - Process article text in one call
    
    Combines summarization + entity extraction to reduce network latency.
    Critical for CQRS pattern: Space A makes ONE request instead of two.
    
    Performance: ~5-10 seconds total (Llama-3 + GLiNER)
    """
    # Lazy load both models on first request
    llama = load_llama_model()
    gliner = load_gliner_model()
    
    if llama is None or gliner is None:
        raise HTTPException(status_code=503, detail="Models failed to load")
    
    start_time = time.time()
    
    try:
        logger.info("🏭 [COMPOSITE] Processing article...")
        
        # -------------------------------------------------------------------------
        # Step 1: Generate Summary with Llama-3
        # -------------------------------------------------------------------------
        logger.info("📝 Step 1/2: Generating summary...")
        
        # Construct prompt (Llama-3-Instruct format)
        prompt = f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>

You are a professional news summarizer. Create concise, accurate summaries.<|eot_id|><|start_header_id|>user<|end_header_id|>

Summarize the following article in 2-3 sentences:

{request.text[:2000]}  

Summary:<|eot_id|><|start_header_id|>assistant<|end_header_id|>

"""
        
        # Run Llama-3 in thread pool
        loop = asyncio.get_event_loop()
        output = await loop.run_in_executor(
            None,
            lambda: llama(
                prompt,
                max_tokens=request.max_tokens,
                temperature=request.temperature,
                stop=["<|eot_id|>", "\n\n"],
                echo=False
            )
        )
        
        summary = output['choices'][0]['text'].strip()
        logger.info(f"✅ Summary generated: {len(summary)} chars")
        
        # -------------------------------------------------------------------------
        # Step 2: Extract Entities with GLiNER
        # -------------------------------------------------------------------------
        logger.info("🔍 Step 2/2: Extracting entities...")
        
        # Run GLiNER on the SUMMARY (more accurate than full text)
        raw_entities = await loop.run_in_executor(
            None,
            lambda: gliner.predict_entities(
                summary,  # Use summary for better precision
                request.entity_labels,
                threshold=request.entity_threshold
            )
        )
        
        # Convert to response format
        entities = [
            Entity(
                text=entity['text'],
                label=entity['label'],
                score=round(entity['score'], 3)
            )
            for entity in raw_entities
        ]
        
        # Extract unique tags (entity texts)
        tags = list(set(entity.text for entity in entities))
        
        processing_time = int((time.time() - start_time) * 1000)
        
        logger.info(f"✅ Processing complete: {len(tags)} tags in {processing_time}ms")
        
        return ProcessArticleResponse(
            summary=summary,
            tags=tags,
            entities=entities,
            processing_time_ms=processing_time,
            model_info={
                "summarizer": "Llama-3-8B-Instruct-Q4_K_M",
                "ner": "GLiNER-small-v2.1"
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Article processing error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/audio", tags=["Audio"])
async def generate_audio(request: AudioRequest):
    """
    Generate speech audio using Edge-TTS
    
    Uses Microsoft's cloud API (zero local resources).
    Returns MP3 audio stream.
    """
    try:
        logger.info(f"🔊 Generating audio with voice: {request.voice}")
        
        # Create TTS communicator
        communicate = edge_tts.Communicate(
            text=request.text,
            voice=request.voice,
            rate=request.rate,
            volume=request.volume
        )
        
        # Stream audio chunks
        async def audio_generator():
            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    yield chunk["data"]
        
        logger.info("✅ Audio generation started")
        
        return StreamingResponse(
            audio_generator(),
            media_type="audio/mpeg",
            headers={
                "Content-Disposition": f"attachment; filename=audio.mp3"
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Audio generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Application Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Run server
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=7860,
        workers=1,
        log_level="info"
    )
