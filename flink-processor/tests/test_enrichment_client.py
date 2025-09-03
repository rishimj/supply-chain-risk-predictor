"""
Tests for enrichment client with mocks and error handling.
"""
import pytest
import asyncio
import aiohttp
from unittest.mock import AsyncMock, patch, MagicMock
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from enrichment_client import EnrichmentClient, MockEnrichmentClient
from models import NewsMessage, EnrichmentResponse, CompanyMention


class TestEnrichmentClient:
    """Tests for EnrichmentClient."""
    
    @pytest.fixture
    def news_message(self):
        """Sample news message for testing."""
        return NewsMessage(
            news_id="test-123",
            headline="Tesla halts production due to chip shortage",
            url="https://example.com/news",
            published="2024-01-01T10:00:00Z",
            full_text="Tesla announced production delays at Shanghai facility."
        )
    
    @pytest.fixture
    def client(self):
        """EnrichmentClient instance."""
        return EnrichmentClient("http://test-api:8080", timeout=1.0)
    
    @pytest.mark.asyncio
    async def test_successful_enrichment(self, client, news_message):
        """Test successful API call."""
        mock_response = {
            "news_id": "test-123",
            "companies": [
                {"ticker": "TSLA", "role": "primary", "sentiment": -0.7}
            ]
        }
        
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.text = AsyncMock(return_value='{"news_id": "test-123", "companies": [{"ticker": "TSLA", "role": "primary", "sentiment": -0.7}]}')
            mock_post.return_value.__aenter__.return_value = mock_resp
            
            result = await client.enrich_news(news_message)
            
            assert result.news_id == "test-123"
            assert len(result.companies) == 1
            assert result.companies[0].ticker == "TSLA"
            assert result.companies[0].sentiment == -0.7
    
    @pytest.mark.asyncio 
    async def test_api_error_status(self, client, news_message):
        """Test handling of API error status codes."""
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_resp = AsyncMock()
            mock_resp.status = 500  # Server error
            mock_post.return_value.__aenter__.return_value = mock_resp
            
            result = await client.enrich_news(news_message)
            
            # Should return empty result on error
            assert result.news_id == "test-123"
            assert len(result.companies) == 0
    
    @pytest.mark.asyncio
    async def test_timeout_handling(self, news_message):
        """Test timeout handling."""
        client = EnrichmentClient("http://test-api:8080", timeout=0.001)  # Very short timeout
        
        with patch('aiohttp.ClientSession.post') as mock_post:
            # Simulate timeout
            mock_post.side_effect = asyncio.TimeoutError()
            
            result = await client.enrich_news(news_message)
            
            # Should return empty result on timeout
            assert result.news_id == "test-123"
            assert len(result.companies) == 0
    
    @pytest.mark.asyncio
    async def test_network_error_handling(self, client, news_message):
        """Test network error handling.""" 
        with patch('aiohttp.ClientSession.post') as mock_post:
            # Simulate network error
            mock_post.side_effect = aiohttp.ClientError("Connection failed")
            
            result = await client.enrich_news(news_message)
            
            # Should return empty result on network error
            assert result.news_id == "test-123"
            assert len(result.companies) == 0
    
    @pytest.mark.asyncio
    async def test_invalid_json_response(self, client, news_message):
        """Test handling of invalid JSON response."""
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.text = AsyncMock(return_value='invalid json{')
            mock_post.return_value.__aenter__.return_value = mock_resp
            
            result = await client.enrich_news(news_message)
            
            # Should return empty result on JSON parse error
            assert result.news_id == "test-123"
            assert len(result.companies) == 0
    
    @pytest.mark.asyncio
    async def test_session_management(self, client):
        """Test session creation and reuse.""" 
        # Session should be created on first call
        assert client._session is None
        
        session1 = await client.get_session()
        assert session1 is not None
        assert client._session is session1
        
        # Should reuse same session
        session2 = await client.get_session()
        assert session2 is session1
        
        # Clean up
        await client.close()


class TestMockEnrichmentClient:
    """Tests for MockEnrichmentClient."""
    
    @pytest.fixture
    def mock_client(self):
        """MockEnrichmentClient instance."""
        return MockEnrichmentClient()
    
    @pytest.mark.asyncio
    async def test_tesla_detection(self, mock_client):
        """Test Tesla keyword detection."""
        news = NewsMessage(
            news_id="test-123",
            headline="Tesla halts production in Shanghai",
            url="https://example.com",
            published="2024-01-01T10:00:00Z"
        )
        
        result = await mock_client.enrich_news(news)
        
        assert result.news_id == "test-123"
        assert len(result.companies) == 1
        assert result.companies[0].ticker == "TSLA"
        assert result.companies[0].role == "primary"
        assert result.companies[0].sentiment == -0.7  # "halt" is negative
    
    @pytest.mark.asyncio
    async def test_apple_positive_sentiment(self, mock_client):
        """Test Apple with positive sentiment."""
        news = NewsMessage(
            news_id="test-456",
            headline="Apple expands manufacturing capacity",
            url="https://example.com",
            published="2024-01-01T10:00:00Z"
        )
        
        result = await mock_client.enrich_news(news)
        
        assert len(result.companies) == 1
        assert result.companies[0].ticker == "AAPL"
        assert result.companies[0].sentiment == 0.7  # "expand" is positive
    
    @pytest.mark.asyncio
    async def test_no_companies_detected(self, mock_client):
        """Test when no companies are detected."""
        news = NewsMessage(
            news_id="test-789",
            headline="General economic news about inflation",
            url="https://example.com",
            published="2024-01-01T10:00:00Z"
        )
        
        result = await mock_client.enrich_news(news)
        
        assert result.news_id == "test-789"
        assert len(result.companies) == 0
    
    @pytest.mark.asyncio
    async def test_neutral_sentiment(self, mock_client):
        """Test neutral sentiment when no sentiment keywords found."""
        news = NewsMessage(
            news_id="test-999",
            headline="Tesla announces new factory location",
            url="https://example.com",
            published="2024-01-01T10:00:00Z"
        )
        
        result = await mock_client.enrich_news(news)
        
        assert len(result.companies) == 1
        assert result.companies[0].ticker == "TSLA"
        assert result.companies[0].sentiment == 0.0  # No sentiment keywords
    
    @pytest.mark.asyncio
    async def test_foxconn_apple_mapping(self, mock_client):
        """Test that Foxconn maps to AAPL (supplier relationship)."""
        news = NewsMessage(
            news_id="test-foxconn",
            headline="Foxconn production delays affect supply chain",
            url="https://example.com", 
            published="2024-01-01T10:00:00Z"
        )
        
        result = await mock_client.enrich_news(news)
        
        assert len(result.companies) == 1
        assert result.companies[0].ticker == "AAPL"  # Foxconn mapped to Apple
        assert result.companies[0].sentiment == -0.7  # "delay" is negative
    
    @pytest.mark.asyncio
    async def test_case_insensitive_matching(self, mock_client):
        """Test case insensitive company matching."""
        news = NewsMessage(
            news_id="test-case",
            headline="TESLA and Apple form partnership",
            url="https://example.com",
            published="2024-01-01T10:00:00Z"
        )
        
        result = await mock_client.enrich_news(news)
        
        # Should match first company found (Tesla)
        assert len(result.companies) == 1
        assert result.companies[0].ticker == "TSLA"
    
    @pytest.mark.asyncio
    async def test_processing_delay(self, mock_client):
        """Test that mock client includes processing delay."""
        import time
        
        start_time = time.time()
        
        news = NewsMessage(
            news_id="test-delay",
            headline="Tesla news",
            url="https://example.com",
            published="2024-01-01T10:00:00Z"
        )
        
        await mock_client.enrich_news(news)
        
        elapsed = time.time() - start_time
        assert elapsed >= 0.01  # Should have at least 10ms delay


class TestRobustness:
    """Test robustness and error recovery."""
    
    @pytest.mark.asyncio
    async def test_concurrent_requests(self):
        """Test handling multiple concurrent requests."""
        client = EnrichmentClient("http://test-api:8080")
        
        news_messages = [
            NewsMessage(f"test-{i}", f"Headline {i}", "https://example.com", "2024-01-01T10:00:00Z")
            for i in range(10)
        ]
        
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_resp = AsyncMock()
            mock_resp.status = 200 
            mock_resp.text = AsyncMock(return_value='{"news_id": "test", "companies": []}')
            mock_post.return_value.__aenter__.return_value = mock_resp
            
            # Send concurrent requests
            tasks = [client.enrich_news(news) for news in news_messages]
            results = await asyncio.gather(*tasks)
            
            assert len(results) == 10
            for result in results:
                assert isinstance(result, EnrichmentResponse)
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_memory_leaks_prevention(self):
        """Test that client properly closes sessions to prevent memory leaks."""
        client = EnrichmentClient("http://test-api:8080")
        
        # Create session
        session = await client.get_session()
        assert not session.closed
        
        # Close client
        await client.close()
        assert session.closed
    
    @pytest.mark.asyncio
    async def test_malformed_request_handling(self):
        """Test handling of malformed requests."""
        client = EnrichmentClient("http://test-api:8080")
        
        # News with None values
        news = NewsMessage(
            news_id=None,  # This might cause issues
            headline="Test",
            url="https://example.com",
            published="2024-01-01T10:00:00Z"
        )
        
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_post.side_effect = Exception("Request error")
            
            result = await client.enrich_news(news)
            
            # Should handle gracefully and return empty result
            assert isinstance(result, EnrichmentResponse)
            assert len(result.companies) == 0
        
        await client.close()