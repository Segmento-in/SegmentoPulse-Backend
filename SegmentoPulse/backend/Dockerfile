FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app ./app

# Expose Hugging Face Spaces default port
EXPOSE 7860

# Run application on port 7860 (HF Spaces standard)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "7860"]
