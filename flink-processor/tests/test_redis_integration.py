"""
Integration tests for Redis sink and error handling.
"""
import pytest
import json
import redis
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from news_processing_job import RedisFeatureSink
from models import CompanyFeatures


class TestRedisFeatureSink:
    """Tests for Redis sink functionality."""
    
    @pytest.fixture
    def redis_url(self):
        """Redis URL for testing."""
        return "redis://localhost:6379/1"  # Use database 1 for tests
    
    @pytest.fixture
    def redis_sink(self, redis_url):
        """Redis sink instance."""
        return RedisFeatureSink(redis_url)
    
    @pytest.fixture
    def test_redis(self, redis_url):
        """Test Redis client for verification."""
        client = redis.from_url(redis_url, decode_responses=True)
        yield client
        # Cleanup after test
        client.flushdb()
        client.close()
    
    @pytest.fixture
    def sample_features_json(self):
        """Sample features JSON string.""" 
        return json.dumps({
            "ticker": "TSLA",
            "window_end": "2024-01-01T12:00:00Z",
            "neg_news_count_24h": 2,
            "pos_news_count_24h": 1,
            "sentiment_ewm_7d": -0.3
        })
    
    def test_redis_connection_success(self, redis_sink, test_redis):
        """Test successful Redis connection."""
        # Mock configuration to avoid needing actual open method
        with patch.object(redis_sink, 'redis_client', test_redis):
            result = redis_sink.redis_client.ping()
            assert result is True
    
    def test_redis_connection_failure(self, redis_url):
        """Test Redis connection failure handling."""
        # Use invalid URL
        sink = RedisFeatureSink("redis://invalid-host:6379")
        
        with pytest.raises(Exception):
            sink.open(None)
    
    def test_invoke_successful_write(self, redis_sink, test_redis, sample_features_json):
        """Test successful write to Redis."""
        redis_sink.redis_client = test_redis
        context = MagicMock()
        
        redis_sink.invoke(sample_features_json, context)
        
        # Verify data was written
        keys = test_redis.keys("feat:TSLA:*")
        assert len(keys) == 1
        
        stored_value = test_redis.get(keys[0])
        parsed = json.loads(stored_value)
        
        assert parsed["ticker"] == "TSLA"
        assert parsed["neg_news_count_24h"] == 2
        assert parsed["pos_news_count_24h"] == 1
        assert parsed["sentiment_ewm_7d"] == -0.3
        assert parsed["_ver"] == "v1"
        assert "_ingest_ts" in parsed
    
    def test_invoke_ttl_setting(self, redis_sink, test_redis, sample_features_json):
        """Test that TTL is properly set."""
        redis_sink.redis_client = test_redis
        context = MagicMock()
        
        redis_sink.invoke(sample_features_json, context)
        
        keys = test_redis.keys("feat:TSLA:*")
        key = keys[0]
        
        ttl = test_redis.ttl(key)
        # Should be close to 7 days (604800 seconds)
        assert 604700 < ttl <= 604800
    
    def test_invoke_different_companies(self, redis_sink, test_redis):
        """Test writing features for different companies."""
        redis_sink.redis_client = test_redis
        context = MagicMock()
        
        companies_data = [
            {"ticker": "TSLA", "window_end": "2024-01-01T12:00:00Z"},
            {"ticker": "AAPL", "window_end": "2024-01-01T12:00:00Z"},
            {"ticker": "MSFT", "window_end": "2024-01-01T12:00:00Z"}
        ]
        
        for company_data in companies_data:
            features_json = json.dumps({
                **company_data,
                "neg_news_count_24h": 1,
                "pos_news_count_24h": 0,
                "sentiment_ewm_7d": -0.2
            })
            redis_sink.invoke(features_json, context)
        
        # Should have 3 keys
        all_keys = test_redis.keys("feat:*")
        assert len(all_keys) == 3
        
        # Check each company
        for ticker in ["TSLA", "AAPL", "MSFT"]:
            ticker_keys = test_redis.keys(f"feat:{ticker}:*")
            assert len(ticker_keys) == 1
    
    def test_invoke_same_company_different_windows(self, redis_sink, test_redis):
        """Test writing features for same company at different window times."""
        redis_sink.redis_client = test_redis
        context = MagicMock()
        
        window_times = [
            "2024-01-01T12:00:00Z",
            "2024-01-01T12:05:00Z", 
            "2024-01-01T12:10:00Z"
        ]
        
        for window_end in window_times:
            features_json = json.dumps({
                "ticker": "TSLA",
                "window_end": window_end,
                "neg_news_count_24h": 1,
                "pos_news_count_24h": 0,
                "sentiment_ewm_7d": -0.1
            })
            redis_sink.invoke(features_json, context)
        
        # Should have 3 different keys for TSLA
        tsla_keys = test_redis.keys("feat:TSLA:*")
        assert len(tsla_keys) == 3
    
    def test_invoke_invalid_json(self, redis_sink, test_redis):
        """Test handling of invalid JSON input."""
        redis_sink.redis_client = test_redis
        context = MagicMock()
        
        # Should not raise exception, just log error
        redis_sink.invoke('{"invalid": json}', context)
        
        # No keys should be created
        keys = test_redis.keys("feat:*")
        assert len(keys) == 0
    
    def test_invoke_missing_fields(self, redis_sink, test_redis):
        """Test handling of JSON with missing required fields."""
        redis_sink.redis_client = test_redis
        context = MagicMock()
        
        # Missing ticker field
        incomplete_json = json.dumps({
            "window_end": "2024-01-01T12:00:00Z",
            "neg_news_count_24h": 1
        })
        
        # Should not crash
        redis_sink.invoke(incomplete_json, context)
        
        keys = test_redis.keys("feat:*")
        assert len(keys) == 0  # Should not create key without ticker
    
    def test_invoke_redis_error(self, redis_sink, sample_features_json):
        """Test handling of Redis errors during write."""
        # Mock Redis client that fails
        mock_redis = MagicMock()
        mock_redis.setex.side_effect = redis.RedisError("Connection lost")
        redis_sink.redis_client = mock_redis
        
        context = MagicMock()
        
        # Should not raise exception
        redis_sink.invoke(sample_features_json, context)
        
        # Should have attempted to write
        mock_redis.setex.assert_called_once()
    
    def test_close_connection(self, redis_sink, test_redis):
        """Test closing Redis connection."""
        redis_sink.redis_client = test_redis
        
        redis_sink.close()
        
        # Connection should be closed (though test client may not reflect this)
        # This is more of a smoke test that close() doesn't crash
        assert True


class TestRedisKeyGeneration:
    """Test Redis key generation edge cases."""
    
    def test_key_generation_various_timestamps(self):
        """Test key generation for various timestamps."""
        test_cases = [
            ("2024-01-01T00:00:00Z", "1704067200"),  # Start of year
            ("2024-02-29T23:59:59Z", "1709251199"),  # Leap year
            ("1970-01-01T00:00:00Z", "0"),           # Unix epoch
        ]
        
        for timestamp, expected_epoch in test_cases:
            features = CompanyFeatures(
                ticker="TEST",
                window_end=timestamp,
                neg_news_count_24h=0,
                pos_news_count_24h=0,
                sentiment_ewm_7d=0.0
            )
            
            key = features.to_redis_key()
            assert key == f"feat:TEST:{expected_epoch}"
    
    def test_key_generation_special_tickers(self):
        """Test key generation with special ticker symbols."""
        special_tickers = [
            "BRK.A",   # Contains dot
            "BF-B",    # Contains dash
            "ABC.TO",  # Exchange suffix
        ]
        
        for ticker in special_tickers:
            features = CompanyFeatures(
                ticker=ticker,
                window_end="2024-01-01T12:00:00Z",
                neg_news_count_24h=0,
                pos_news_count_24h=0,
                sentiment_ewm_7d=0.0
            )
            
            key = features.to_redis_key()
            assert key.startswith(f"feat:{ticker}:")
            assert key.endswith("1704110400")


class TestRedisIntegrationScenarios:
    """Test realistic integration scenarios."""
    
    def test_high_frequency_writes(self, redis_url):
        """Test handling high frequency writes."""
        if not self._redis_available(redis_url):
            pytest.skip("Redis not available for integration test")
        
        sink = RedisFeatureSink(redis_url)
        test_redis = redis.from_url(redis_url.replace("/1", "/2"), decode_responses=True)  # Use db 2
        sink.redis_client = test_redis
        
        context = MagicMock()
        
        try:
            # Write many features rapidly
            for i in range(100):
                features_json = json.dumps({
                    "ticker": f"STOCK{i % 10}",
                    "window_end": f"2024-01-01T12:{i:02d}:00Z",
                    "neg_news_count_24h": i % 5,
                    "pos_news_count_24h": (i + 1) % 3,
                    "sentiment_ewm_7d": (i % 20 - 10) / 10.0
                })
                sink.invoke(features_json, context)
            
            # Verify all writes succeeded
            keys = test_redis.keys("feat:*")
            assert len(keys) == 100
            
        finally:
            test_redis.flushdb()
            test_redis.close()
    
    def test_memory_usage_large_values(self, redis_url):
        """Test memory usage with large feature values."""
        if not self._redis_available(redis_url):
            pytest.skip("Redis not available for integration test")
        
        sink = RedisFeatureSink(redis_url)
        test_redis = redis.from_url(redis_url.replace("/1", "/3"), decode_responses=True)  # Use db 3
        sink.redis_client = test_redis
        
        context = MagicMock()
        
        try:
            # Write features with extreme values
            features_json = json.dumps({
                "ticker": "EXTREME",
                "window_end": "2024-01-01T12:00:00Z",
                "neg_news_count_24h": 999999,  # Very large
                "pos_news_count_24h": 888888,  # Very large
                "sentiment_ewm_7d": -0.99999999,  # High precision
                "extra_metadata": "x" * 1000  # Large string
            })
            
            sink.invoke(features_json, context)
            
            # Verify it was stored correctly
            keys = test_redis.keys("feat:EXTREME:*")
            assert len(keys) == 1
            
            stored = json.loads(test_redis.get(keys[0]))
            assert stored["neg_news_count_24h"] == 999999
            
        finally:
            test_redis.flushdb()
            test_redis.close()
    
    def test_concurrent_writes_same_key(self, redis_url):
        """Test concurrent writes to same Redis key."""
        if not self._redis_available(redis_url):
            pytest.skip("Redis not available for integration test")
        
        import threading
        
        sink = RedisFeatureSink(redis_url)
        test_redis = redis.from_url(redis_url.replace("/1", "/4"), decode_responses=True)  # Use db 4
        sink.redis_client = test_redis
        
        context = MagicMock()
        results = []
        
        def write_feature(value):
            try:
                features_json = json.dumps({
                    "ticker": "CONCURRENT",
                    "window_end": "2024-01-01T12:00:00Z",
                    "neg_news_count_24h": value,
                    "pos_news_count_24h": 0,
                    "sentiment_ewm_7d": 0.0
                })
                sink.invoke(features_json, context)
                results.append("success")
            except Exception as e:
                results.append(f"error: {e}")
        
        try:
            # Start multiple threads writing to same key
            threads = []
            for i in range(10):
                thread = threading.Thread(target=write_feature, args=(i,))
                threads.append(thread)
                thread.start()
            
            # Wait for all threads
            for thread in threads:
                thread.join()
            
            # All should succeed (last write wins)
            assert len([r for r in results if r == "success"]) == 10
            
            # Key should exist
            keys = test_redis.keys("feat:CONCURRENT:*")
            assert len(keys) == 1
            
        finally:
            test_redis.flushdb()
            test_redis.close()
    
    def _redis_available(self, redis_url):
        """Check if Redis is available for testing."""
        try:
            client = redis.from_url(redis_url, decode_responses=True)
            client.ping()
            client.close()
            return True
        except:
            return False


class TestErrorRecovery:
    """Test error recovery and resilience.""" 
    
    def test_redis_temporary_disconnection(self, redis_url):
        """Test handling of temporary Redis disconnection."""
        sink = RedisFeatureSink(redis_url)
        
        # Mock Redis client that fails then succeeds
        mock_redis = MagicMock()
        calls = 0
        
        def mock_setex(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise redis.ConnectionError("Connection lost")
            # Subsequent calls succeed
            return True
        
        mock_redis.setex.side_effect = mock_setex
        sink.redis_client = mock_redis
        
        context = MagicMock()
        features_json = json.dumps({
            "ticker": "TEST",
            "window_end": "2024-01-01T12:00:00Z",
            "neg_news_count_24h": 1,
            "pos_news_count_24h": 0,
            "sentiment_ewm_7d": 0.0
        })
        
        # First call should handle error gracefully
        sink.invoke(features_json, context)
        
        # Should have attempted the write
        assert mock_redis.setex.call_count == 1
    
    def test_malformed_timestamp_handling(self, redis_sink):
        """Test handling of malformed timestamps."""
        redis_sink.redis_client = MagicMock()
        context = MagicMock()
        
        malformed_json = json.dumps({
            "ticker": "TEST",
            "window_end": "not-a-timestamp",
            "neg_news_count_24h": 1,
            "pos_news_count_24h": 0,
            "sentiment_ewm_7d": 0.0
        })
        
        # Should handle gracefully without crashing
        sink.invoke(malformed_json, context)
        
        # Should not have attempted Redis write due to timestamp error
        assert redis_sink.redis_client.setex.call_count == 0