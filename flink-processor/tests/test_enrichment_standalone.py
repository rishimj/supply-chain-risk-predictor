"""
Standalone tests for enrichment client - no Flink dependencies.
"""
import pytest
import json
import asyncio
import aiohttp
from unittest.mock import AsyncMock, patch
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from enrichment_client import EnrichmentClient  
from models import NewsMessage, EnrichmentResponse


class TestEnrichmentClientStandalone:
    """Standalone tests for enrichment client."""
    
    @pytest.fixture
    def client(self):
        """Create enrichment client."""
        return EnrichmentClient("http://mock-api:8000")
    
    @pytest.fixture
    def sample_news(self):
        """Sample news message."""
        return NewsMessage(
            news_id="test-123",
            headline="Tesla reports strong Q4 earnings",
            url="https://example.com/news/tesla",
            published="2024-01-01T10:00:00Z",
            full_text="Tesla reported strong Q4 earnings with revenue beating expectations."
        )
    
    @pytest.mark.asyncio
    async def test_successful_enrichment(self, client, sample_news):
        """Test successful API call."""
        mock_response = {
            "companies": [
                {"ticker": "TSLA", "sentiment": -0.3},
                {"ticker": "GM", "sentiment": 0.1}
            ]
        }
        
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.text = AsyncMock(return_value=json.dumps(mock_response))
            mock_post.return_value.__aenter__.return_value = mock_resp
            
            response = await client.enrich_news(sample_news)
            
            assert len(response.companies) == 2
            assert response.companies[0].ticker == "TSLA"
            assert response.companies[0].sentiment == -0.3
    
    @pytest.mark.asyncio
    async def test_api_timeout(self, client, sample_news):
        """Test API timeout handling."""
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_post.side_effect = asyncio.TimeoutError()
            
            response = await client.enrich_news(sample_news)
            
            assert len(response.companies) == 0
    
    @pytest.mark.asyncio
    async def test_api_error_status(self, client, sample_news):
        """Test API error status handling."""
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_resp = AsyncMock()
            mock_resp.status = 500
            mock_post.return_value.__aenter__.return_value = mock_resp
            
            response = await client.enrich_news(sample_news)
            
            assert len(response.companies) == 0
    
    @pytest.mark.asyncio
    async def test_invalid_json_response(self, client, sample_news):
        """Test invalid JSON response handling."""
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(side_effect=json.JSONDecodeError("Invalid", "", 0))
            mock_post.return_value.__aenter__.return_value = mock_resp
            
            response = await client.enrich_news(sample_news)
            
            assert len(response.companies) == 0