# api.py
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import pandas as pd
import io
import json

# Import your existing backend orchestrator
# Adjusted from user snippet 'from core.backend' to 'from backend.backend'
import sys
import os
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Segmento Sense API")

# --- ADD THIS BLOCK ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins (good for testing)
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (POST, GET, etc.)
    allow_headers=["*"],  # Allows all headers
)

# Ensure backend directory is in path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

try:
    from backend.backend import RegexClassifier
except ImportError:
    # Fallback/Try direct import if package structure is different
    try:
        from backend import RegexClassifier
    except ImportError:
        print("Could not import RegexClassifier. Make sure backend/backend.py exists.")
        # Mock for now if import fails to allow server start
        class RegexClassifier:
             def __init__(self): pass

app = FastAPI(title="Segmento Sense API")

# Enable CORS for Frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, replace with frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize the Brain
try:
    backend = RegexClassifier()
except Exception as e:
    print(f"Error initializing backend: {e}")
    backend = None # Handle gracefully

# --- Pydantic Models for Requests ---
class DbConnection(BaseModel):
    type: str # postgres, mysql, mongo
    host: str
    port: str
    user: str
    password: str
    database: str
    collection: Optional[str] = None

class CloudConnection(BaseModel):
    service: str # aws, azure, gcp
    key_1: str   # access_key or conn_string
    key_2: Optional[str] = None # secret_key
    region: Optional[str] = None
    bucket: str
    file_name: str

class AppConnection(BaseModel):
    service: str # gmail, slack, confluence
    token_or_path: str # token or credentials.json content
    target: str # channel_id, page_id, or num_emails

# --- ENDPOINTS ---

@app.get("/")
def health_check():
    return {"status": "Segmento Sense is running"}

@app.post("/scan/file")
async def scan_file(file: UploadFile = File(...)):
    """
    Handles PDF, CSV, JSON, Parquet, Avro, Image uploads.
    """
    if backend is None:
        raise HTTPException(status_code=503, detail="Backend not initialized")

    file_bytes = await file.read()
    filename = file.filename.lower()
    
    df = pd.DataFrame()
    raw_text = ""
    
    # 1. Route to correct handler in backend.py
    if filename.endswith(".pdf"):
        # For demo, scan page 0
        raw_text = backend.get_pdf_page_text(file_bytes, 0)
        # Scan text
        inspection = backend.run_full_inspection(raw_text)
        matches = backend.analyze_text_hybrid(raw_text)
        return {
            "type": "unstructured",
            "content": raw_text,
            "matches": matches,
            "stats": inspection.to_dict(orient="records")
        }
    
    elif filename.endswith((".png", ".jpg", ".jpeg")):
        raw_text = backend.get_ocr_text_from_image(file_bytes)
        inspection = backend.run_full_inspection(raw_text)
        matches = backend.analyze_text_hybrid(raw_text)
        return {
            "type": "unstructured",
            "content": raw_text,
            "matches": matches,
            "stats": inspection.to_dict(orient="records")
        }

    else:
        # Structured Data
        try:
            if filename.endswith(".csv"):
                df = pd.read_csv(io.BytesIO(file_bytes))
            elif filename.endswith(".json"):
                df = backend.get_json_data(io.BytesIO(file_bytes))
            elif filename.endswith(".parquet"):
                df = backend.get_parquet_data(file_bytes)
            elif filename.endswith(".avro"):
                df = backend.get_avro_data(file_bytes)
        except Exception as e:
             raise HTTPException(status_code=400, detail=f"Error parsing file: {str(e)}")
            
        # Get PII Counts
        if df.empty:
             return {"type": "structured", "pii_counts": [], "preview": [], "schema": []}
             
        pii_counts = backend.get_pii_counts_dataframe(df)
        masked_preview = backend.mask_dataframe(df.head(20))
        
        return {
            "type": "structured",
            "pii_counts": pii_counts.to_dict(orient="records"),
            "preview": masked_preview.to_dict(orient="records"),
            "schema": backend.get_data_schema(df).to_dict(orient="records")
        }

@app.post("/scan/database")
async def scan_db(conn: DbConnection):
    if backend is None:
        raise HTTPException(status_code=503, detail="Backend not initialized")

    df = pd.DataFrame()
    try:
        if conn.type == "postgres":
            df = backend.get_postgres_data(conn.host, conn.port, conn.database, conn.user, conn.password, conn.collection)
        elif conn.type == "mysql":
            df = backend.get_mysql_data(conn.host, conn.port, conn.database, conn.user, conn.password, conn.collection)
        elif conn.type == "mongo":
            df = backend.get_mongodb_data(conn.host, conn.port, conn.database, conn.user, conn.password, conn.collection)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Connection failed or no data found")

    pii_counts = backend.get_pii_counts_dataframe(df)
    return {
        "source": conn.type,
        "pii_counts": pii_counts.to_dict(orient="records"),
        "preview": backend.mask_dataframe(df.head(10)).to_dict(orient="records")
    }

@app.post("/scan/app")
async def scan_app(conn: AppConnection):
    if backend is None:
        raise HTTPException(status_code=503, detail="Backend not initialized")

    df = pd.DataFrame()
    
    try:
        if conn.service == "slack":
            df = backend.get_slack_messages(conn.token_or_path, conn.target)
        elif conn.service == "confluence":
            # Split target "url|user|page_id" if needed or adjust model
            # Simplified for demo: assuming backend handles auth
            pass 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"App connection error: {str(e)}")
        
    if df.empty:
        raise HTTPException(status_code=400, detail="No data fetched")
        
    pii_counts = backend.get_pii_counts_dataframe(df)
    return {
        "source": conn.service,
        "pii_counts": pii_counts.to_dict(orient="records"),
        "preview": backend.mask_dataframe(df.head(10)).to_dict(orient="records")
    }
