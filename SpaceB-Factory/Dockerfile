# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Install build dependencies for llama-cpp-python
RUN apt-get update && apt-get install -y \
    cmake \
    g++ \
    gcc \
    pkg-config \
    libopenblas-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Set environment variables for CPU optimization
# GGML_BLAS enables BLAS acceleration
# GGML_OPENBLAS uses OpenBLAS library for matrix operations (2-3x faster)
ENV CMAKE_ARGS="-DGGML_BLAS=ON -DGGML_BLAS_VENDOR=OpenBLAS"
ENV FORCE_CMAKE=1

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
# llama-cpp-python will compile from source with CPU optimizations
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Create cache directory for models
RUN mkdir -p /app/models

# Expose port 7860 (HuggingFace Space default)
EXPOSE 7860

# Set environment variables
ENV HOST=0.0.0.0
ENV PORT=7860

# Health check for HuggingFace monitoring
HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:7860/health')"

# Run the FastAPI application with Uvicorn
# workers=1 ensures single process (important for model memory management)
# log-level=info provides detailed logging for debugging
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7860", "--workers", "1", "--log-level", "info"]
