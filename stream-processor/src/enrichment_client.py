"""
Async HTTP client for calling the enrichment API.
"""
import asyncio
import json
import logging
from typing import Optional
import aiohttp
from models import NewsMessage, EnrichmentResponse, CompanyMention


logger = logging.getLogger(__name__)


class EnrichmentClient:
    """Async HTTP client for enrichment API calls."""
    
    def __init__(self, base_url: str, timeout: float = 0.8):
        """
        Initialize enrichment client.
        
        Args:
            base_url: Base URL for enrichment service (e.g., "http://enrichment:8080")
            timeout: Timeout in seconds (default 0.8s as per context)
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    async def close(self):
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def enrich_news(self, news: NewsMessage) -> EnrichmentResponse:
        """
        Call enrichment API to extract companies and sentiment.
        
        Args:
            news: News message to enrich
            
        Returns:
            EnrichmentResponse with companies and sentiment
            
        Raises:
            TimeoutError: If API call times out
            Exception: For other HTTP errors
        """
        session = await self.get_session()
        url = f"{self.base_url}/v1/enrich"
        
        try:
            logger.debug(f"Calling enrichment API for news_id={news.news_id}")
            
            async with session.post(url, json=news.to_enrichment_request()) as response:
                if response.status == 200:
                    response_text = await response.text()
                    result = EnrichmentResponse.from_json(response_text)
                    
                    logger.debug(
                        f"Enrichment success: news_id={news.news_id}, "
                        f"companies={len(result.companies)}"
                    )
                    return result
                else:
                    logger.warning(
                        f"Enrichment API error: news_id={news.news_id}, "
                        f"status={response.status}"
                    )
                    # Return empty result on error
                    return EnrichmentResponse(news_id=news.news_id, companies=[])
                    
        except asyncio.TimeoutError:
            logger.warning(f"Enrichment timeout for news_id={news.news_id}")
            # Return empty result on timeout (as per context document)
            return EnrichmentResponse(news_id=news.news_id, companies=[])
            
        except Exception as e:
            logger.error(f"Enrichment error for news_id={news.news_id}: {e}")
            # Return empty result on any error
            return EnrichmentResponse(news_id=news.news_id, companies=[])


# For testing without actual enrichment service
class MockEnrichmentClient(EnrichmentClient):
    """Mock enrichment client for testing."""
    
    def __init__(self):
        super().__init__("http://mock", timeout=0.1)
    
    async def enrich_news(self, news: NewsMessage) -> EnrichmentResponse:
        """Mock enrichment that extracts simple patterns from headlines."""
        companies = []
        
        # Import company database
        from company_database import get_company_keywords
        company_keywords = get_company_keywords()
        
        headline_lower = news.headline.lower()
        
        # Sort keywords by length (descending) to prefer longer, more specific matches
        sorted_keywords = sorted(company_keywords.items(), key=lambda x: len(x[0]), reverse=True)
        
        for keyword, ticker in sorted_keywords:
            if keyword in headline_lower:
                # Simple sentiment based on keywords
                sentiment = 0.0
                if any(neg in headline_lower for neg in ["halt", "shortage", "delay", "cut", "drop"]):
                    sentiment = -0.7
                elif any(pos in headline_lower for pos in ["expand", "beat", "surge", "growth"]):
                    sentiment = 0.7
                
                companies.append({
                    "ticker": ticker,
                    "role": "primary",
                    "sentiment": sentiment
                })
                break  # Only first match for simplicity
        
        # Simulate some processing delay
        await asyncio.sleep(0.01)
        
        return EnrichmentResponse(
            news_id=news.news_id,
            companies=[
                CompanyMention(
                    ticker=c["ticker"],
                    role=c["role"], 
                    sentiment=c["sentiment"]
                ) for c in companies
            ]
        )