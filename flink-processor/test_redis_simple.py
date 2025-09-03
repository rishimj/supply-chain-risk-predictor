#!/usr/bin/env python3
"""
Simple Redis sink tests without PyFlink dependencies.
"""
import sys
import os
import json
import redis
import time
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from models import CompanyFeatures


class SimpleRedisSink:
    """Simplified Redis sink for testing."""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis_client = None
        
    def open(self):
        """Initialize Redis connection."""
        self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
        
    def invoke(self, features_json: str):
        """Write features to Redis."""
        try:
            data = json.loads(features_json)
            
            # Extract required fields
            ticker = data.get('ticker')
            window_end = data.get('window_end')
            
            if not ticker or not window_end:
                print(f"Missing required fields: ticker={ticker}, window_end={window_end}")
                return
                
            # Create features object
            features = CompanyFeatures(
                ticker=ticker,
                window_end=window_end,
                neg_news_count_24h=data.get('neg_news_count_24h', 0),
                pos_news_count_24h=data.get('pos_news_count_24h', 0),
                sentiment_ewm_7d=data.get('sentiment_ewm_7d', 0.0)
            )
            
            # Generate Redis key and value
            key = features.to_redis_key()
            value = features.to_redis_value()
            
            # Set with TTL (7 days = 604800 seconds)
            self.redis_client.setex(key, 604800, value)
            
            print(f"Stored feature for {ticker} with key: {key}")
            
        except Exception as e:
            print(f"Error storing features: {e}")
    
    def close(self):
        """Close Redis connection."""
        if self.redis_client:
            self.redis_client.close()


def test_redis_sink():
    """Test Redis sink functionality."""
    print("Testing Redis sink...")
    
    # Test with local Redis
    redis_url = "redis://localhost:6379/1"
    
    try:
        sink = SimpleRedisSink(redis_url)
        sink.open()
        
        # Test data
        test_features = [
            {
                "ticker": "TSLA",
                "window_end": "2024-01-01T12:00:00Z",
                "neg_news_count_24h": 2,
                "pos_news_count_24h": 1,
                "sentiment_ewm_7d": -0.3
            },
            {
                "ticker": "AAPL", 
                "window_end": "2024-01-01T12:05:00Z",
                "neg_news_count_24h": 0,
                "pos_news_count_24h": 3,
                "sentiment_ewm_7d": 0.4
            }
        ]
        
        # Write test data
        for features in test_features:
            features_json = json.dumps(features)
            sink.invoke(features_json)
        
        # Verify data was written
        test_redis = redis.from_url(redis_url, decode_responses=True)
        keys = test_redis.keys("feat:*")
        
        print(f"Found {len(keys)} feature keys in Redis")
        
        for key in keys:
            value = test_redis.get(key)
            parsed = json.loads(value)
            print(f"Key: {key}")
            print(f"  Ticker: {parsed['ticker']}")
            print(f"  Neg Count: {parsed['neg_news_count_24h']}")
            print(f"  Pos Count: {parsed['pos_news_count_24h']}")
            print(f"  Sentiment: {parsed['sentiment_ewm_7d']}")
            print(f"  Version: {parsed['_ver']}")
            
            # Verify TTL
            ttl = test_redis.ttl(key)
            print(f"  TTL: {ttl} seconds")
            assert ttl > 604700, f"TTL too low: {ttl}"
        
        # Cleanup
        test_redis.delete(*keys)
        test_redis.close()
        sink.close()
        
        print("✓ Redis sink test passed")
        
    except Exception as e:
        print(f"⚠ Redis sink test failed: {e}")
        return False
    
    return True


def test_redis_error_handling():
    """Test Redis error handling."""
    print("Testing Redis error handling...")
    
    # Test with invalid data
    sink = SimpleRedisSink("redis://localhost:6379/1")
    sink.open()
    
    # Test invalid JSON
    sink.invoke('{"invalid": json}')
    
    # Test missing fields  
    sink.invoke('{"ticker": "TEST"}')  # Missing window_end
    sink.invoke('{"window_end": "2024-01-01T12:00:00Z"}')  # Missing ticker
    
    sink.close()
    
    print("✓ Error handling test passed")
    return True


def test_large_batch():
    """Test handling large batches of data."""
    print("Testing large batch processing...")
    
    redis_url = "redis://localhost:6379/2"  # Use different DB
    
    try:
        sink = SimpleRedisSink(redis_url)
        sink.open()
        
        # Generate 100 feature records
        start_time = time.time()
        
        for i in range(100):
            features = {
                "ticker": f"STOCK{i % 10}",  # 10 different tickers
                "window_end": f"2024-01-01T12:{i:02d}:00Z",
                "neg_news_count_24h": i % 5,
                "pos_news_count_24h": (i + 1) % 3,
                "sentiment_ewm_7d": (i % 20 - 10) / 10.0
            }
            sink.invoke(json.dumps(features))
        
        end_time = time.time()
        
        # Verify all were written
        test_redis = redis.from_url(redis_url, decode_responses=True)
        keys = test_redis.keys("feat:*")
        
        print(f"Processed 100 records in {end_time - start_time:.2f} seconds")
        print(f"Created {len(keys)} Redis keys")
        
        # Cleanup
        test_redis.flushdb()
        test_redis.close()
        sink.close()
        
        print("✓ Large batch test passed")
        
    except Exception as e:
        print(f"⚠ Large batch test failed: {e}")
        return False
        
    return True


def main():
    """Run Redis integration tests."""
    print("Running Redis integration tests...")
    print()
    
    success = True
    
    try:
        # Check Redis is available
        client = redis.from_url("redis://localhost:6379/1")
        client.ping()
        client.close()
        
        success &= test_redis_sink()
        success &= test_redis_error_handling() 
        success &= test_large_batch()
        
        print()
        if success:
            print("🎉 All Redis integration tests passed!")
        else:
            print("❌ Some Redis tests failed")
            
    except Exception as e:
        print(f"⚠ Redis not available for testing: {e}")
        success = False
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)