FROM mcr.microsoft.com/playwright/python:v1.41.0-jammy

# Set working directory
WORKDIR /app

# Install system dependencies
# Playwright image already has browsers, but might need basic build tools for some python packages
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
# Use --no-cache-dir to keep image small
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium

# Copy application code
COPY app ./app

# Expose Hugging Face Spaces default port
EXPOSE 7860

# Run application on port 7860 (HF Spaces standard)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "7860"]
