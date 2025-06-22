from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch
from typing import List, Dict, Any, Optional
import logging
from pydantic import BaseModel
from datetime import datetime
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic models for request/response
class SearchRequest(BaseModel):
    query: str = ""
    page: int = 1
    size: int = 10
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    author: Optional[str] = None

class Article(BaseModel):
    url: str
    headline: str
    author: str
    location: str
    published_at: str
    content: str
    scraped_at: str
    word_count: int

class SearchResponse(BaseModel):
    articles: List[Article]
    total: int
    page: int
    size: int
    total_pages: int

# Elasticsearch configuration
ES_HOST = "http://localhost:9200"
ES_USER = "elastic"
ES_PASSWORD = "JvQhvZYl"
ES_INDEX = "prothomalo_politics"

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize Elasticsearch connection when starting up"""
    global es_client
    try:
        es_client = Elasticsearch(
            hosts=[ES_HOST],
            basic_auth=(ES_USER, ES_PASSWORD),
            verify_certs=False
        )
        
        if es_client.ping():
            logger.info("Successfully connected to Elasticsearch")
        else:
            logger.error("Failed to connect to Elasticsearch")
            raise Exception("Elasticsearch connection failed")
            
    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise
    
    yield
    
    # Optional: Close connection on shutdown
    await es_client.close()

# Initialize FastAPI with lifespan
app = FastAPI(
    title="Prothom Alo News API",
    description="API for accessing scraped Prothom Alo news articles",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development only, restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ... (keep all your existing route handlers unchanged) ...

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend_server:app", host="0.0.0.0", port=8000, reload=True)