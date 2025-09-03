#!/usr/bin/env python3
"""
Test script to simulate the Flink pipeline and verify Redis integration.
"""
import json
import redis
from datetime import datetime
import sys
sys.path.append('flink-processor/src')

from models import NewsMessage, CompanyFeatures
from enrichment_client import MockEnrichmentClient
import asyncio


async def test_pipeline():
    """Test the complete pipeline flow."""
    # Connect to Redis
    r = redis.from_url("redis://localhost:6379/0", decode_responses=True)
    
    # Test news message (from Kafka)
    news_json = '{"news_id":"test-123","headline":"Tesla halts Shanghai production due to chip shortage","url":"https://example.com/tesla-halt","published":"2024-09-02T15:30:00Z","full_text":"Tesla announced a temporary halt in production."}'
    
    print("🧪 Testing Pipeline Flow:")
    print(f"📰 Input News: {news_json}")
    
    # Step 1: Parse news message
    news = NewsMessage.from_json(news_json)
    print(f"✅ Parsed news: {news.headline}")
    
    # Step 2: Mock enrichment (simulate what Flink would do)
    client = MockEnrichmentClient()
    enrichment = await client.enrich_news(news)
    print(f"🔍 Enrichment found {len(enrichment.companies)} companies:")
    
    for company in enrichment.companies:
        print(f"  - {company.ticker}: {company.sentiment} sentiment")
    
    # Step 3: Simulate feature aggregation (normally done by Flink windowing)
    if enrichment.companies:
        company = enrichment.companies[0]  # Take first company
        
        # Create mock features
        features = CompanyFeatures(
            ticker=company.ticker,
            window_end=datetime.utcnow().isoformat() + "Z",
            neg_news_count_24h=1 if company.sentiment < 0 else 0,
            pos_news_count_24h=1 if company.sentiment > 0 else 0,
            sentiment_ewm_7d=company.sentiment
        )
        
        print(f"📊 Generated Features:")
        print(f"  - Ticker: {features.ticker}")
        print(f"  - Negative count: {features.neg_news_count_24h}")
        print(f"  - Positive count: {features.pos_news_count_24h}")
        print(f"  - Sentiment EWM: {features.sentiment_ewm_7d}")
        
        # Step 4: Write to Redis (what Flink sink would do)
        redis_key = features.to_redis_key()
        redis_value = features.to_redis_value()
        
        r.setex(redis_key, 604800, redis_value)  # 7 day TTL
        
        print(f"💾 Written to Redis:")
        print(f"  - Key: {redis_key}")
        print(f"  - Value: {redis_value}")
        
        # Step 5: Verify we can read it back
        stored_value = r.get(redis_key)
        print(f"✅ Read back from Redis: {stored_value}")
        
        # Also set a "latest" key for easy access
        latest_key = f"feat:{features.ticker}:latest"
        r.setex(latest_key, 604800, redis_value)
        print(f"💾 Also stored as: {latest_key}")
        
    print("\n🎉 Pipeline test complete!")
    
    # Show all keys in Redis
    keys = r.keys("feat:*")
    print(f"\n📋 All feature keys in Redis: {keys}")

if __name__ == "__main__":
    asyncio.run(test_pipeline())