"""
Pydantic models for the Enrichment Service

These models define the request/response schemas for the enrichment API,
matching the data contracts defined in context.md.
"""

from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime

class EnrichmentRequest(BaseModel):
    """Request model for /v1/enrich endpoint."""
    news_id: str = Field(..., description="Unique identifier for the news article")
    headline: str = Field(..., description="News article headline")
    body: Optional[str] = Field(None, description="Optional full text body")
    url: Optional[str] = Field(None, description="Optional article URL")
    published: Optional[str] = Field(None, description="Optional publication timestamp (ISO-8601)")

class CompanyMention(BaseModel):
    """A company mentioned in the news with sentiment analysis."""
    ticker: str = Field(..., description="Stock ticker symbol (e.g., 'TSLA')")
    role: str = Field(..., description="Role in the article: 'primary' or 'mentioned'")
    sentiment: float = Field(..., ge=-1.0, le=1.0, description="Sentiment score from -1.0 (negative) to +1.0 (positive)")

class EnrichmentResponse(BaseModel):
    """Response model for /v1/enrich endpoint."""
    news_id: str = Field(..., description="Same news_id from the request")
    companies: List[CompanyMention] = Field(default_factory=list, description="List of companies detected in the news")

class HealthResponse(BaseModel):
    """Response model for /healthz endpoint."""
    status: str = Field(..., description="Health status")
    timestamp: str = Field(..., description="Current timestamp")
    version: str = Field(..., description="Service version")

class ErrorResponse(BaseModel):
    """Error response model."""
    error: str = Field(..., description="Error message")
    news_id: Optional[str] = Field(None, description="News ID if available")
