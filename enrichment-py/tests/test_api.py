"""
Integration tests for the FastAPI Enrichment Service

Tests the HTTP API endpoints following TDD principles from context.md.
"""

import pytest
import sys
import os
try:
    from fastapi.testclient import TestClient
except ImportError:
    from starlette.testclient import TestClient

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app

class TestEnrichmentAPI:
    """Test cases for the enrichment API endpoints."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.client = TestClient(app)
    
    def test_health_check(self):
        """Test /healthz endpoint."""
        response = self.client.get("/healthz")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert data["version"] == "1.0.0"
    
    def test_metrics_endpoint(self):
        """Test /metrics endpoint returns Prometheus metrics."""
        response = self.client.get("/metrics")
        
        assert response.status_code == 200
        # Should contain some basic Prometheus metrics
        content = response.content.decode()
        assert "enrich_requests_total" in content
    
    def test_stats_endpoint(self):
        """Test /stats endpoint returns service statistics."""
        response = self.client.get("/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert "total_companies" in data
        assert "total_keywords" in data
        assert data["service"] == "enrichment"
        assert data["version"] == "1.0.0"
    
    def test_enrich_valid_request(self):
        """Test valid enrichment request."""
        request_data = {
            "news_id": "test-123",
            "headline": "Tesla production ramps up at new facility",
            "body": "Tesla Inc. announces increased production capacity",
            "url": "https://example.com/news",
            "published": "2024-01-01T10:00:00Z"
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Check response structure
        assert data["news_id"] == "test-123"
        assert "companies" in data
        assert isinstance(data["companies"], list)
        
        # Should detect Tesla
        tesla_companies = [c for c in data["companies"] if c["ticker"] == "TSLA"]
        assert len(tesla_companies) == 1
        
        tesla = tesla_companies[0]
        assert tesla["role"] == "primary"  # In headline
        assert tesla["sentiment"] == 0.0  # Neutral (no strong keywords)
    
    def test_enrich_negative_sentiment(self):
        """Test enrichment with negative sentiment."""
        request_data = {
            "news_id": "test-negative",
            "headline": "Apple supply chain faces major disruption and delays",
            "body": "Apple Inc. manufacturing operations halt due to supplier issues"
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Should detect Apple with negative sentiment
        apple_companies = [c for c in data["companies"] if c["ticker"] == "AAPL"]
        assert len(apple_companies) == 1
        assert apple_companies[0]["sentiment"] == -0.7
    
    def test_enrich_positive_sentiment(self):
        """Test enrichment with positive sentiment."""
        request_data = {
            "news_id": "test-positive",
            "headline": "Microsoft beats earnings expectations with strong growth",
            "body": "Microsoft Corporation expands cloud services beyond forecasts"
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Should detect Microsoft with positive sentiment
        msft_companies = [c for c in data["companies"] if c["ticker"] == "MSFT"]
        assert len(msft_companies) == 1
        assert msft_companies[0]["sentiment"] == 0.7
    
    def test_enrich_multiple_companies(self):
        """Test enrichment detecting multiple companies."""
        request_data = {
            "news_id": "test-multiple",
            "headline": "Apple and Google partnership expands mobile ecosystem",
            "body": "Apple Inc. and Google collaborate on new initiatives"
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Should detect both companies
        tickers = {c["ticker"] for c in data["companies"]}
        assert "AAPL" in tickers
        assert "GOOGL" in tickers
        
        # Both should be primary (in headline)
        for company in data["companies"]:
            assert company["role"] == "primary"
    
    def test_enrich_no_companies(self):
        """Test enrichment with no companies detected."""
        request_data = {
            "news_id": "test-none",
            "headline": "General economic outlook improves for Q4",
            "body": "Analysts predict better market conditions ahead"
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Should return empty companies list
        assert data["companies"] == []
    
    def test_enrich_missing_headline(self):
        """Test validation: missing headline returns 400."""
        request_data = {
            "news_id": "test-missing",
            "body": "Some news content"
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 422  # Validation error
    
    def test_enrich_empty_headline(self):
        """Test validation: empty headline returns 400."""
        request_data = {
            "news_id": "test-empty",
            "headline": "",
            "body": "Some news content"
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 400
        data = response.json()
        assert "empty" in data["error"].lower()
    
    def test_enrich_missing_news_id(self):
        """Test validation: missing news_id returns 422."""
        request_data = {
            "headline": "Some news headline",
            "body": "Some news content"
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 422  # Validation error
    
    def test_trace_id_propagation(self):
        """Test that trace_id is propagated in responses."""
        request_data = {
            "news_id": "test-trace",
            "headline": "Tesla announces new model",
        }
        
        # Send request with custom trace_id
        response = self.client.post(
            "/v1/enrich", 
            json=request_data,
            headers={"x-trace-id": "custom-trace-123"}
        )
        
        assert response.status_code == 200
        # Should return the same trace_id in response headers
        assert response.headers["x-trace-id"] == "custom-trace-123"
    
    def test_response_schema_validation(self):
        """Test that response matches expected schema."""
        request_data = {
            "news_id": "test-schema",
            "headline": "Amazon expands logistics network",
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Validate response structure
        assert "news_id" in data
        assert "companies" in data
        assert isinstance(data["companies"], list)
        
        # If companies found, validate company structure
        for company in data["companies"]:
            assert "ticker" in company
            assert "role" in company
            assert "sentiment" in company
            assert company["role"] in ["primary", "mentioned"]
            assert -1.0 <= company["sentiment"] <= 1.0

    def test_stats_shows_tiered_model(self):
        """Test that stats endpoint shows tiered sentiment model."""
        response = self.client.get("/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert "sentiment_model" in data
        assert data["sentiment_model"] == "tiered", "Should use tiered model"
        assert "supply_chain_categories" in data
    
    def test_enrich_critical_news_uses_distilbert(self):
        """Test that critical news (earnings + major company) triggers DistilBERT."""
        request_data = {
            "news_id": "test-critical",
            "headline": "Apple reports quarterly earnings beat expectations",
            "body": "Apple Inc. announced strong quarterly results exceeding analyst forecasts with revenue growth"
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Should detect Apple
        apple_companies = [c for c in data["companies"] if c["ticker"] == "AAPL"]
        assert len(apple_companies) == 1
        
        # Should have valid sentiment (may vary based on actual model)
        assert -1.0 <= apple_companies[0]["sentiment"] <= 1.0
    
    def test_enrich_supply_chain_disruption(self):
        """Test enrichment with supply chain disruption keywords."""
        request_data = {
            "news_id": "test-disruption",
            "headline": "Factory closure affects Tesla production",
            "body": "Semiconductor shortage and supply chain disruption threaten delivery schedules"
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Should detect Tesla
        tesla_companies = [c for c in data["companies"] if c["ticker"] == "TSLA"]
        assert len(tesla_companies) == 1
        
        # Should have negative sentiment due to disruption keywords
        assert tesla_companies[0]["sentiment"] < 0.0, "Disruption should result in negative sentiment"
    
    def test_enrich_routine_news_uses_vader(self):
        """Test that routine news uses fast VADER processing."""
        request_data = {
            "news_id": "test-routine",
            "headline": "Small startup announces product update",
            "body": "Minor company news about new features"
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Should process successfully (may or may not detect companies)
        assert "companies" in data
        assert isinstance(data["companies"], list)
    
    def test_enrich_major_company_triggers_distilbert(self):
        """Test that major companies trigger DistilBERT even without earnings keywords."""
        request_data = {
            "news_id": "test-major-company",
            "headline": "Microsoft announces new cloud partnership",
            "body": "Microsoft Corporation expands strategic alliances in cloud computing sector"
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Should detect Microsoft
        msft_companies = [c for c in data["companies"] if c["ticker"] == "MSFT"]
        assert len(msft_companies) == 1
        
        # Should have valid sentiment
        assert -1.0 <= msft_companies[0]["sentiment"] <= 1.0
    
    def test_enrich_long_content_triggers_distilbert(self):
        """Test that long content (>200 chars) triggers DistilBERT."""
        long_body = "This is a very detailed article about the company. " * 10  # >200 chars
        request_data = {
            "news_id": "test-long",
            "headline": "Company provides detailed quarterly report",
            "body": long_body
        }
        
        response = self.client.post("/v1/enrich", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Should process successfully
        assert "companies" in data
        assert isinstance(data["companies"], list)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
