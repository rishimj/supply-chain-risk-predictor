"""
Async HTTP client for calling the enrichment API.
"""
import asyncio
import json
import logging
from typing import Optional, List
import aiohttp
from models import NewsMessage, EnrichmentResponse, CompanyMention


logger = logging.getLogger(__name__)


class EnrichmentClient:
    """Async HTTP client for enrichment API calls."""
    
    def __init__(self, base_url: str, timeout: float = 0.8, batch_timeout: float = 5.0):
        """
        Initialize enrichment client.
        
        Args:
            base_url: Base URL for enrichment service (e.g., "http://enrichment:8080")
            timeout: Timeout in seconds for single requests (default 0.8s)
            batch_timeout: Timeout in seconds for batch requests (default 5.0s)
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.batch_timeout = aiohttp.ClientTimeout(total=batch_timeout)
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
    
    async def enrich_batch(self, news_batch: List[NewsMessage]) -> List[EnrichmentResponse]:
        """
        Call enrichment API to process multiple articles in batch for better throughput.
        
        Args:
            news_batch: List of news messages to enrich
            
        Returns:
            List of EnrichmentResponse objects (one per article)
            
        Raises:
            TimeoutError: If batch API call times out
            Exception: For other HTTP errors
        """
        if not news_batch:
            return []
        
        session = await self.get_session()
        url = f"{self.base_url}/v1/enrich/batch"
        
        try:
            logger.debug(f"Calling batch enrichment API for {len(news_batch)} articles")
            
            # Build batch request payload
            batch_payload = {
                "articles": [news.to_enrichment_request() for news in news_batch]
            }
            
            # Use longer timeout for batch requests
            async with session.post(url, json=batch_payload, timeout=self.batch_timeout) as response:
                if response.status == 200:
                    response_data = await response.json()
                    
                    # Parse batch response
                    results = []
                    for result_data in response_data.get('results', []):
                        result = EnrichmentResponse(
                            news_id=result_data['news_id'],
                            companies=[
                                CompanyMention(
                                    ticker=c['ticker'],
                                    role=c['role'],
                                    sentiment=c['sentiment']
                                )
                                for c in result_data.get('companies', [])
                            ]
                        )
                        results.append(result)
                    
                    total_companies = sum(len(r.companies) for r in results)
                    logger.debug(
                        f"Batch enrichment success: {len(news_batch)} articles, "
                        f"{total_companies} companies found, "
                        f"{response_data.get('processing_time_ms', 0):.1f}ms"
                    )
                    return results
                else:
                    logger.warning(
                        f"Batch enrichment API error: status={response.status}"
                    )
                    # Return empty results for all articles on error
                    return [EnrichmentResponse(news_id=news.news_id, companies=[]) for news in news_batch]
                    
        except asyncio.TimeoutError:
            logger.warning(f"Batch enrichment timeout for {len(news_batch)} articles")
            # Return empty results on timeout
            return [EnrichmentResponse(news_id=news.news_id, companies=[]) for news in news_batch]
            
        except Exception as e:
            logger.error(f"Batch enrichment error for {len(news_batch)} articles: {e}")
            # Return empty results on any error
            return [EnrichmentResponse(news_id=news.news_id, companies=[]) for news in news_batch]


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