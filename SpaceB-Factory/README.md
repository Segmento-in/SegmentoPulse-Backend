# Space B - The Factory 🏭

AI Inference Microservice for Segmento Pulse

## Overview

Space B is a dedicated microservice that handles heavy AI workloads:
- **Text Summarization**: Llama-3-8B-Instruct (GGUF quantized)
- **Named Entity Recognition**: GLiNER (zero-shot)
- **Audio Generation**: Edge-TTS (cloud-based)

Optimized for CPU-only execution on HuggingFace Free Tier (2 vCPU, 16GB RAM).

## Architecture

```
Space A (Main Backend) → HTTP POST → Space B (The Factory)
                                        ↓
                                   AI Models
                                   ├─ Llama-3 (4.5GB)
                                   ├─ GLiNER (200MB)
                                   └─ Edge-TTS (Cloud API)
```

## Quick Start

### 1. Deploy to HuggingFace

```bash
# Clone your HuggingFace Space repository
git clone https://huggingface.co/spaces/YOUR_USERNAME/segmento-factory
cd segmento-factory

# Copy all files
cp Dockerfile main.py requirements.txt ./

# Push to HuggingFace
git add .
git commit -m "Initial deployment"
git push
```

### 2. Monitor Build

- Go to your Space URL: `https://huggingface.co/spaces/YOUR_USERNAME/segmento-factory`
- Click "Logs" tab
- Wait for build to complete (~15-20 minutes first time)

### 3. Test Endpoints

```bash
# Health check
curl https://YOUR_USERNAME-segmento-factory.hf.space/health

# Summarization
curl -X POST https://YOUR_USERNAME-segmento-factory.hf.space/summarize \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Your article text here...",
    "max_tokens": 150
  }'

# Entity extraction
curl -X POST https://YOUR_USERNAME-segmento-factory.hf.space/extract \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Apple Inc. announced that Tim Cook will visit Paris.",
    "labels": ["Person", "Organization", "Location"]
  }'

# Audio generation
curl -X POST https://YOUR_USERNAME-segmento-factory.hf.space/audio \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Hello world",
    "voice": "en-US-ChristopherNeural"
  }' --output audio.mp3
```

## API Reference

### POST /summarize

**Request**:
```json
{
  "text": "Long article content...",
  "max_tokens": 150,
  "temperature": 0.7
}
```

**Response**:
```json
{
  "summary": "AI-generated summary...",
  "model": "Llama-3-8B-Instruct-Q4_K_M",
  "inference_time_ms": 4728
}
```

### POST /extract

**Request**:
```json
{
  "text": "Apple Inc. announced...",
  "labels": ["Person", "Organization", "Location"],
  "threshold": 0.5
}
```

**Response**:
```json
{
  "entities": [
    {
      "text": "Apple Inc.",
      "label": "Organization",
      "score": 0.96
    }
  ],
  "model": "GLiNER-small-v2.1",
  "inference_time_ms": 127
}
```

### POST /audio

**Request**:
```json
{
  "text": "Hello world",
  "voice": "en-US-ChristopherNeural",
  "rate": "+0%",
  "volume": "+0%"
}
```

**Response**: Binary MP3 audio stream

### GET /health

**Response**:
```json
{
  "status": "healthy",
  "models_loaded": true,
  "uptime_seconds": 3600,
  "llama_loaded": true,
  "gliner_loaded": true
}
```

## Performance

### Resource Usage

- **RAM**: ~5-6GB (Llama-3: 4.5GB, GLiNER: 200MB, Overhead: 300MB)
- **CPU**: 2 vCPU (100% during inference)
- **Disk**: ~6GB (model cache)

### Latency

- **Summarization**: 5-10 seconds (depends on text length)
- **NER**: 50-200ms
- **Audio**: 2-4 seconds

### Throughput

- **Summarization**: 8-12 requests/minute
- **NER**: 60-80 requests/minute
- **Audio**: 20-30 requests/minute

## Integration with Space A

Add this to your Space A backend:

```python
import httpx

SPACE_B_URL = "https://YOUR_USERNAME-segmento-factory.hf.space"

async def get_ai_summary(article_text: str):
    """Call Space B for AI summarization"""
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{SPACE_B_URL}/summarize",
            json={
                "text": article_text,
                "max_tokens": 150,
                "temperature": 0.7
            }
        )
        return response.json()

async def extract_entities(article_text: str):
    """Call Space B for NER"""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"{SPACE_B_URL}/extract",
            json={
                "text": article_text,
                "labels": ["Person", "Organization", "Location"]
            }
        )
        return response.json()
```

## Monitoring

### Keep Space Awake

HuggingFace free tier spaces sleep after 48 hours. Use UptimeRobot:

1. Go to https://uptimerobot.com
2. Create monitor:
   - Type: HTTP(s)
   - URL: `https://YOUR_USERNAME-segmento-factory.hf.space/health`
   - Interval: 5 minutes
3. Done! Your space will never sleep.

### Check Logs

```bash
# View logs in HuggingFace UI
# Or use CLI:
huggingface-cli space logs YOUR_USERNAME/segmento-factory --follow
```

## Troubleshooting

### Models not loading

**Symptom**: `503 Service Unavailable` on API calls

**Solution**: Check logs for model download errors. First startup takes 10-15 minutes.

### OOM (Out of Memory) errors

**Symptom**: Space crashes during inference

**Solution**: 
- Reduce `n_ctx` in Llama initialization (line 127 in main.py)
- Use smaller quantization (Q3_K_M instead of Q4_K_M)

### Slow inference

**Symptom**: Summarization takes >20 seconds

**Solution**: 
- Verify OpenBLAS is installed: Check Dockerfile build logs
- Reduce `max_tokens` in requests

## Development

### Local Testing (Docker)

```bash
# Build image
docker build -t space-b .

# Run container
docker run -p 7860:7860 space-b

# Test
curl http://localhost:7860/health
```

### Local Testing (Python)

```bash
# Install dependencies
pip install -r requirements.txt

# Run server
python main.py

# Access at http://localhost:7860
```

## Tech Stack

- **FastAPI**: Web framework
- **llama-cpp-python**: CPU-optimized LLM inference
- **GLiNER**: Fast zero-shot NER
- **Edge-TTS**: Cloud-based TTS
- **Docker**: Containerization

## License

MIT License - See LICENSE file

## Support

For issues, contact: [Your contact info]

---

**Status**: ✅ Production Ready

Deploy to HuggingFace Space and start processing!
