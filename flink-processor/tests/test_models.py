"""
Unit tests for data models and serialization.
"""
import json
import pytest
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from models import NewsMessage, CompanyMention, EnrichmentResponse, CompanyMentionEvent, CompanyFeatures


class TestNewsMessage:
    """Tests for NewsMessage model."""
    
    def test_from_json_valid(self):
        """Test parsing valid JSON."""
        json_str = '''{
            "news_id": "test-123",
            "headline": "Test headline",
            "url": "https://example.com",
            "published": "2024-01-01T10:00:00Z",
            "full_text": "Test content"
        }'''
        
        news = NewsMessage.from_json(json_str)
        
        assert news.news_id == "test-123"
        assert news.headline == "Test headline"
        assert news.url == "https://example.com"
        assert news.published == "2024-01-01T10:00:00Z"
        assert news.full_text == "Test content"
    
    def test_from_json_no_full_text(self):
        """Test parsing JSON without full_text (optional field)."""
        json_str = '''{
            "news_id": "test-456", 
            "headline": "Test headline",
            "url": "https://example.com",
            "published": "2024-01-01T10:00:00Z"
        }'''
        
        news = NewsMessage.from_json(json_str)
        
        assert news.news_id == "test-456"
        assert news.full_text is None
    
    def test_from_json_invalid(self):
        """Test parsing invalid JSON."""
        with pytest.raises(json.JSONDecodeError):
            NewsMessage.from_json('{"invalid": json}')
    
    def test_from_json_missing_required_field(self):
        """Test parsing JSON missing required fields."""
        json_str = '{"news_id": "test", "headline": "test"}'
        
        with pytest.raises(KeyError):
            NewsMessage.from_json(json_str)
    
    def test_to_enrichment_request(self):
        """Test conversion to enrichment API request format."""
        news = NewsMessage(
            news_id="test-123",
            headline="Test headline",
            url="https://example.com", 
            published="2024-01-01T10:00:00Z",
            full_text="Test content"
        )
        
        request = news.to_enrichment_request()
        
        expected = {
            "news_id": "test-123",
            "headline": "Test headline", 
            "body": "Test content",
            "url": "https://example.com",
            "published": "2024-01-01T10:00:00Z"
        }
        
        assert request == expected


class TestEnrichmentResponse:
    """Tests for EnrichmentResponse model."""
    
    def test_from_json_valid(self):
        """Test parsing valid enrichment response."""
        json_str = '''{
            "news_id": "test-123",
            "companies": [
                {"ticker": "TSLA", "role": "primary", "sentiment": -0.7},
                {"ticker": "AAPL", "role": "mentioned", "sentiment": 0.3}
            ]
        }'''
        
        response = EnrichmentResponse.from_json(json_str)
        
        assert response.news_id == "test-123"
        assert len(response.companies) == 2
        
        assert response.companies[0].ticker == "TSLA"
        assert response.companies[0].role == "primary" 
        assert response.companies[0].sentiment == -0.7
        
        assert response.companies[1].ticker == "AAPL"
        assert response.companies[1].role == "mentioned"
        assert response.companies[1].sentiment == 0.3
    
    def test_from_json_empty_companies(self):
        """Test parsing response with no companies."""
        json_str = '{"news_id": "test-456", "companies": []}'
        
        response = EnrichmentResponse.from_json(json_str)
        
        assert response.news_id == "test-456"
        assert len(response.companies) == 0


class TestCompanyMentionEvent:
    """Tests for CompanyMentionEvent model."""
    
    def test_to_json(self):
        """Test serialization to JSON."""
        event = CompanyMentionEvent(
            news_id="test-123",
            event_ts="2024-01-01T10:00:00Z",
            ticker="TSLA", 
            role="primary",
            sentiment=-0.5
        )
        
        json_str = event.to_json()
        parsed = json.loads(json_str)
        
        expected = {
            "news_id": "test-123",
            "event_ts": "2024-01-01T10:00:00Z", 
            "ticker": "TSLA",
            "role": "primary",
            "sentiment": -0.5
        }
        
        assert parsed == expected


class TestCompanyFeatures:
    """Tests for CompanyFeatures model."""
    
    def test_to_json(self):
        """Test serialization to JSON."""
        features = CompanyFeatures(
            ticker="TSLA",
            window_end="2024-01-01T11:00:00Z",
            neg_news_count_24h=3,
            pos_news_count_24h=1,
            sentiment_ewm_7d=-0.2
        )
        
        json_str = features.to_json()
        parsed = json.loads(json_str)
        
        expected = {
            "ticker": "TSLA",
            "window_end": "2024-01-01T11:00:00Z",
            "neg_news_count_24h": 3, 
            "pos_news_count_24h": 1,
            "sentiment_ewm_7d": -0.2
        }
        
        assert parsed == expected
    
    def test_to_redis_key(self):
        """Test Redis key generation."""
        features = CompanyFeatures(
            ticker="TSLA",
            window_end="2024-01-01T10:00:00Z",
            neg_news_count_24h=1,
            pos_news_count_24h=0,
            sentiment_ewm_7d=-0.5
        )
        
        key = features.to_redis_key()
        
        # Should be feat:TSLA:1704103200 (epoch for 2024-01-01T10:00:00Z)
        assert key.startswith("feat:TSLA:")
        assert key.endswith("1704103200")  # epoch timestamp
    
    def test_to_redis_value(self):
        """Test Redis value generation with metadata."""
        features = CompanyFeatures(
            ticker="TSLA",
            window_end="2024-01-01T10:00:00Z",
            neg_news_count_24h=1,
            pos_news_count_24h=0,
            sentiment_ewm_7d=-0.5
        )
        
        value_str = features.to_redis_value()
        parsed = json.loads(value_str)
        
        # Should have all original fields plus metadata
        assert parsed["ticker"] == "TSLA"
        assert parsed["window_end"] == "2024-01-01T10:00:00Z"
        assert parsed["neg_news_count_24h"] == 1
        assert parsed["pos_news_count_24h"] == 0
        assert parsed["sentiment_ewm_7d"] == -0.5
        assert parsed["_ver"] == "v1"
        assert "_ingest_ts" in parsed
    
    def test_redis_key_different_times(self):
        """Test that different timestamps produce different keys."""
        features1 = CompanyFeatures(
            ticker="TSLA",
            window_end="2024-01-01T10:00:00Z",
            neg_news_count_24h=1, pos_news_count_24h=0, sentiment_ewm_7d=0.0
        )
        
        features2 = CompanyFeatures(
            ticker="TSLA", 
            window_end="2024-01-01T10:05:00Z",
            neg_news_count_24h=1, pos_news_count_24h=0, sentiment_ewm_7d=0.0
        )
        
        key1 = features1.to_redis_key()
        key2 = features2.to_redis_key()
        
        assert key1 != key2
        assert key1.startswith("feat:TSLA:")
        assert key2.startswith("feat:TSLA:")


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_empty_strings(self):
        """Test handling of empty strings."""
        news = NewsMessage(
            news_id="",
            headline="",
            url="",
            published="2024-01-01T10:00:00Z"
        )
        
        # Should not raise exception
        request = news.to_enrichment_request()
        assert request["news_id"] == ""
        assert request["headline"] == ""
    
    def test_special_characters(self):
        """Test handling of special characters."""
        news = NewsMessage(
            news_id="test-123",
            headline="Tesla's \"breakthrough\" in AI (50% improvement)",
            url="https://example.com/news?id=123&lang=en",
            published="2024-01-01T10:00:00Z",
            full_text="Content with émojis 🚗 and symbols $$$ & <tags>"
        )
        
        json_str = json.dumps(news.__dict__)
        # Should not raise exception
        parsed = json.loads(json_str)
        assert "Tesla's" in parsed["headline"]
        assert "🚗" in parsed["full_text"]
    
    def test_extreme_sentiment_values(self):
        """Test handling of extreme sentiment values."""
        features = CompanyFeatures(
            ticker="TEST",
            window_end="2024-01-01T10:00:00Z",
            neg_news_count_24h=999999,
            pos_news_count_24h=0,
            sentiment_ewm_7d=-1.0  # Minimum sentiment
        )
        
        json_str = features.to_json()
        parsed = json.loads(json_str)
        
        assert parsed["neg_news_count_24h"] == 999999
        assert parsed["sentiment_ewm_7d"] == -1.0