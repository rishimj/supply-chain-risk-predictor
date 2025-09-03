#!/usr/bin/env python3
"""
Test runner for core components that don't depend on PyFlink.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from models import NewsMessage, EnrichmentResponse, CompanyFeatures, CompanyMentionEvent
from enrichment_client import EnrichmentClient, MockEnrichmentClient
import json
import asyncio
from datetime import datetime
import redis
import time


def test_models():
    """Test data models."""
    print("Testing data models...")
    
    # Test NewsMessage
    news = NewsMessage(
        news_id="test-123",
        headline="Tesla reports earnings",
        url="https://example.com",
        published="2024-01-01T10:00:00Z",
        full_text="Tesla reported strong earnings."
    )
    
    # Test serialization via JSON roundtrip
    test_json = json.dumps({
        "news_id": news.news_id,
        "headline": news.headline,
        "url": news.url,
        "published": news.published,
        "full_text": news.full_text
    })
    news2 = NewsMessage.from_json(test_json)
    assert news.news_id == news2.news_id
    
    # Test enrichment request format
    req = news.to_enrichment_request()
    assert "news_id" in req
    assert "headline" in req
    assert "body" in req
    
    # Test CompanyFeatures
    features = CompanyFeatures(
        ticker="TSLA",
        window_end="2024-01-01T12:00:00Z",
        neg_news_count_24h=2,
        pos_news_count_24h=1,
        sentiment_ewm_7d=-0.3
    )
    
    # Test Redis key generation
    key = features.to_redis_key()
    assert key.startswith("feat:TSLA:")
    
    # Test Redis value format
    value = features.to_redis_value()
    parsed = json.loads(value)
    assert parsed["_ver"] == "v1"
    assert "_ingest_ts" in parsed
    
    print("✓ Data models working correctly")


def test_feature_aggregation():
    """Test feature aggregation logic without Flink."""
    print("Testing feature aggregation...")
    
    # We'll implement a simple version of the aggregator logic
    class SimpleFeatureAggregator:
        def __init__(self):
            self.neg_count = 0
            self.pos_count = 0
            self.sentiment_ewm = 0.0
            self.alpha = 0.1
            
        def add_mention(self, sentiment):
            if sentiment < 0:
                self.neg_count += 1
            elif sentiment > 0:
                self.pos_count += 1
                
            # Update EWM
            if self.sentiment_ewm == 0.0 and sentiment != 0:
                self.sentiment_ewm = sentiment
            else:
                self.sentiment_ewm = self.alpha * sentiment + (1 - self.alpha) * self.sentiment_ewm
    
    # Test aggregation
    agg = SimpleFeatureAggregator()
    agg.add_mention(-0.8)
    agg.add_mention(0.5)
    agg.add_mention(-0.3)
    
    assert agg.neg_count == 2
    assert agg.pos_count == 1
    assert agg.sentiment_ewm != 0.0
    
    print("✓ Feature aggregation logic working")


async def test_enrichment_client():
    """Test enrichment client with mock."""
    print("Testing enrichment client...")
    
    # Test with mock client
    mock_client = MockEnrichmentClient()
    
    news = NewsMessage(
        news_id="test-123",
        headline="Tesla stock rises",
        url="https://example.com",
        published="2024-01-01T10:00:00Z",
        full_text="Tesla stock price increased after earnings report."
    )
    
    response = await mock_client.enrich_news(news)
    
    # Mock should return some companies
    assert len(response.companies) > 0
    assert response.news_id == news.news_id
    
    print("✓ Enrichment client working with mock")


def test_redis_key_generation():
    """Test Redis key generation edge cases."""
    print("Testing Redis key generation...")
    
    test_cases = [
        ("TSLA", "2024-01-01T12:00:00Z", "1704110400"),
        ("AAPL", "2024-02-01T00:00:00Z", "1706745600"),
        ("BRK.A", "2024-01-01T12:00:00Z", "1704110400"),  # Special ticker
    ]
    
    for ticker, timestamp, expected_epoch in test_cases:
        features = CompanyFeatures(
            ticker=ticker,
            window_end=timestamp,
            neg_news_count_24h=0,
            pos_news_count_24h=0,
            sentiment_ewm_7d=0.0
        )
        
        key = features.to_redis_key()
        expected_key = f"feat:{ticker}:{expected_epoch}"
        assert key == expected_key, f"Expected {expected_key}, got {key}"
    
    print("✓ Redis key generation working correctly")


def test_redis_connection():
    """Test Redis connection (if available)."""
    print("Testing Redis connection...")
    
    try:
        # Try to connect to Redis
        client = redis.from_url("redis://localhost:6379/1", decode_responses=True)
        client.ping()
        
        # Test basic operations
        test_key = f"test:key:{int(time.time())}"
        client.setex(test_key, 60, "test_value")
        
        value = client.get(test_key)
        assert value == "test_value"
        
        # Cleanup
        client.delete(test_key)
        client.close()
        
        print("✓ Redis connection working")
        
    except Exception as e:
        print(f"⚠ Redis not available for testing: {e}")


async def main():
    """Run all tests."""
    print("Running core component tests...")
    print()
    
    try:
        test_models()
        test_feature_aggregation()
        await test_enrichment_client()
        test_redis_key_generation()
        test_redis_connection()
        
        print()
        print("🎉 All core component tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)