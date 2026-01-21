"""
FastAPI Enrichment Service

HTTP API for news enrichment that detects companies and analyzes sentiment.
Matches the API specification in context.md.
"""

import os
import logging
import time
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import uvicorn

from models import EnrichmentRequest, EnrichmentResponse, HealthResponse, ErrorResponse, BatchEnrichmentRequest, BatchEnrichmentResponse
from enrichment_service import EnrichmentService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "service": "enrichment", "event": "%(message)s"}'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Supply Chain Risk Predictor - Enrichment Service",
    description="News enrichment service that detects companies and analyzes sentiment",
    version="1.0.0"
)

# Initialize enrichment service
enrichment_service = EnrichmentService()

# Prometheus metrics (clear registry to avoid conflicts)
from prometheus_client import REGISTRY, CollectorRegistry

# Use a custom registry to avoid conflicts
CUSTOM_REGISTRY = CollectorRegistry()

REQUEST_COUNT = Counter(
    'enrich_requests_total',
    'Total enrichment requests',
    ['outcome'],  # success, error, timeout
    registry=CUSTOM_REGISTRY
)

REQUEST_LATENCY = Histogram(
    'enrich_latency_seconds',
    'Enrichment request latency',
    buckets=[0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0],
    registry=CUSTOM_REGISTRY
)

COMPANIES_DETECTED = Counter(
    'companies_detected_total',
    'Total companies detected',
    ['ticker'],
    registry=CUSTOM_REGISTRY
)

# Configuration
SERVICE_ENV = os.getenv("SERVICE_ENV", "production")
PROMETHEUS_PORT = int(os.getenv("PROM_PORT", "9101"))

@app.middleware("http")
async def add_trace_id(request: Request, call_next):
    """Add trace_id to all requests for observability."""
    # Generate trace_id or get from header
    trace_id = request.headers.get("x-trace-id", f"enrich-{int(time.time() * 1000000)}")
    
    # Add to request state
    request.state.trace_id = trace_id
    
    # Process request
    response = await call_next(request)
    
    # Add trace_id to response headers
    response.headers["x-trace-id"] = trace_id
    
    return response

@app.post("/v1/enrich", response_model=EnrichmentResponse)
async def enrich_news(request: EnrichmentRequest, http_request: Request) -> EnrichmentResponse:
    """
    Enrich news article with company detection and sentiment analysis.
    
    This endpoint matches the specification in context.md:
    - Takes news data (headline, body, etc.)  
    - Returns companies with ticker, role, and sentiment
    """
    start_time = time.time()
    trace_id = getattr(http_request.state, 'trace_id', 'unknown')
    
    try:
        logger.info(f'{{"event": "enrich_start", "trace_id": "{trace_id}", "news_id": "{request.news_id}"}}')
        
        # Validate request
        if not request.news_id or not request.news_id.strip():
            raise HTTPException(status_code=422, detail="News ID cannot be empty")
        if not request.headline or not request.headline.strip():
            raise HTTPException(status_code=400, detail="Headline cannot be empty")
        
        # Perform enrichment with tiered sentiment analysis
        companies = await enrichment_service.enrich_news(request)
        
        # Update metrics
        REQUEST_COUNT.labels(outcome='success').inc()
        REQUEST_LATENCY.observe(time.time() - start_time)
        
        # Count companies detected
        for company in companies:
            COMPANIES_DETECTED.labels(ticker=company.ticker).inc()
        
        # Log success
        logger.info(f'{{"event": "enrich_success", "trace_id": "{trace_id}", "news_id": "{request.news_id}", '
                   f'"companies_found": {len(companies)}, "latency_ms": {(time.time() - start_time) * 1000:.1f}}}')
        
        return EnrichmentResponse(
            news_id=request.news_id,
            companies=companies
        )
        
    except HTTPException:
        REQUEST_COUNT.labels(outcome='error').inc()
        raise
    except Exception as e:
        REQUEST_COUNT.labels(outcome='error').inc()
        logger.error(f'{{"event": "enrich_error", "trace_id": "{trace_id}", "news_id": "{request.news_id}", '
                    f'"error": "{str(e)}", "latency_ms": {(time.time() - start_time) * 1000:.1f}}}')
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/v1/enrich/batch", response_model=BatchEnrichmentResponse)
async def enrich_batch(batch_request: BatchEnrichmentRequest, http_request: Request) -> BatchEnrichmentResponse:
    """
    Batch enrich multiple news articles for higher throughput.
    
    This endpoint processes multiple articles in parallel, using tiered sentiment
    analysis to route articles to fast (VADER) or slow (DistilBERT) paths.
    
    Benefits:
    - 5-10x higher throughput than individual requests
    - Intelligent routing: 90% fast path, 10% slow path
    - Parallel processing with asyncio
    
    Args:
        batch_request: List of articles to enrich
        
    Returns:
        BatchEnrichmentResponse with all results
    """
    start_time = time.time()
    trace_id = getattr(http_request.state, 'trace_id', 'unknown')
    
    try:
        batch_size = len(batch_request.articles)
        logger.info(f'{{"event": "batch_enrich_start", "trace_id": "{trace_id}", "batch_size": {batch_size}}}')
        
        # Validate batch size
        if batch_size == 0:
            raise HTTPException(status_code=400, detail="Batch cannot be empty")
        if batch_size > 100:
            raise HTTPException(status_code=400, detail="Batch size cannot exceed 100 articles")
        
        # Perform batch enrichment
        response = await enrichment_service.enrich_batch(batch_request)
        
        # Update metrics
        REQUEST_COUNT.labels(outcome='success').inc()
        REQUEST_LATENCY.observe(time.time() - start_time)
        
        # Count companies detected
        for result in response.results:
            for company in result.companies:
                COMPANIES_DETECTED.labels(ticker=company.ticker).inc()
        
        # Log success
        total_companies = sum(len(r.companies) for r in response.results)
        logger.info(f'{{"event": "batch_enrich_success", "trace_id": "{trace_id}", '
                   f'"batch_size": {batch_size}, "total_companies": {total_companies}, '
                   f'"latency_ms": {(time.time() - start_time) * 1000:.1f}, '
                   f'"avg_per_article_ms": {response.processing_time_ms / batch_size:.1f}}}')
        
        return response
        
    except HTTPException:
        REQUEST_COUNT.labels(outcome='error').inc()
        raise
    except Exception as e:
        REQUEST_COUNT.labels(outcome='error').inc()
        logger.error(f'{{"event": "batch_enrich_error", "trace_id": "{trace_id}", '
                    f'"error": "{str(e)}", "latency_ms": {(time.time() - start_time) * 1000:.1f}}}')
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/healthz", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat() + "Z",
        version="1.0.0"
    )

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return JSONResponse(
        content=generate_latest(CUSTOM_REGISTRY).decode('utf-8'),
        media_type=CONTENT_TYPE_LATEST
    )

@app.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """Get enrichment service statistics."""
    stats = enrichment_service.get_stats()
    return {
        **stats,
        "service": "enrichment",
        "version": "1.0.0",
        "environment": SERVICE_ENV
    }

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Custom HTTP exception handler with trace_id."""
    trace_id = getattr(request.state, 'trace_id', 'unknown')
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "trace_id": trace_id
        }
    )

if __name__ == "__main__":
    # Run the FastAPI server
    port = int(os.getenv("PORT", "8081"))
    host = os.getenv("HOST", "0.0.0.0")
    
    logger.info(f'{{"event": "service_start", "port": {port}, "host": "{host}", "environment": "{SERVICE_ENV}"}}')
    
    uvicorn.run(
        "app:app",
        host=host,
        port=port,
        log_level="info" if SERVICE_ENV == "development" else "warning",
        reload=SERVICE_ENV == "development"
    )
