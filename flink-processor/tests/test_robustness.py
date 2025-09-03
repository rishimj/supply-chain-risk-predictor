"""
Tests for error handling, robustness, and edge cases in the Flink processor.
"""
import pytest
import json
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from news_processing_job import NewsEnrichmentFunction, CompanyFeaturesWindowFunction, RedisFeatureSink
from enrichment_client import MockEnrichmentClient
from models import NewsMessage, CompanyFeatures


class TestNewsEnrichmentFunctionRobustness:
    """Test robustness of NewsEnrichmentFunction."""
    
    @pytest.fixture
    def enrichment_function(self):
        """NewsEnrichmentFunction with mock client."""
        return NewsEnrichmentFunction("http://test:8080", use_mock=True)
    
    @pytest.fixture
    def mock_context(self):
        """Mock Flink context."""
        return MagicMock()
    
    def test_invalid_json_input(self, enrichment_function, mock_context):
        """Test handling of invalid JSON input."""
        output = []
        
        # Should not crash on invalid JSON
        enrichment_function.process_element('{"invalid": json}', mock_context, output)
        
        assert len(output) == 0  # No output on error
    
    def test_missing_required_fields(self, enrichment_function, mock_context):
        """Test handling of JSON with missing required fields."""
        output = []
        
        # Missing 'published' field
        incomplete_json = '{"news_id": "test", "headline": "test", "url": "test"}'
        
        enrichment_function.process_element(incomplete_json, mock_context, output)
        
        assert len(output) == 0  # No output on error
    
    def test_empty_string_input(self, enrichment_function, mock_context):
        """Test handling of empty string input."""
        output = []
        
        enrichment_function.process_element("", mock_context, output)
        enrichment_function.process_element('{}', mock_context, output)
        
        assert len(output) == 0  # No output on empty/invalid input
    
    def test_very_large_input(self, enrichment_function, mock_context):
        """Test handling of very large input."""
        output = []
        
        # Create large news message
        large_text = "x" * 100000  # 100KB of text
        large_json = json.dumps({
            "news_id": "large-test",
            "headline": "Large news",
            "url": "https://example.com",
            "published": "2024-01-01T10:00:00Z",
            "full_text": large_text
        })
        
        # Should handle large input without issues
        enrichment_function.process_element(large_json, mock_context, output)
        
        # Should produce output (mock enrichment should work)
        assert len(output) == 0  # No companies detected in "Large news"
    
    def test_unicode_and_special_characters(self, enrichment_function, mock_context):
        """Test handling of unicode and special characters."""
        output = []
        
        unicode_json = json.dumps({
            "news_id": "unicode-test",
            "headline": "Tesla 🚗 announces émission-free production (50% CO₂ reduction)",
            "url": "https://example.com/news?id=123&lang=en",
            "published": "2024-01-01T10:00:00Z",
            "full_text": "Content with unicode: café, naïve, résumé, 北京, 東京, emojis: 🚀🔋⚡"
        })
        
        enrichment_function.process_element(unicode_json, mock_context, output)
        
        # Should detect Tesla and produce output
        assert len(output) == 1
        mention = json.loads(output[0])
        assert mention["ticker"] == "TSLA"
    
    def test_null_and_none_values(self, enrichment_function, mock_context):
        """Test handling of null/None values in JSON."""
        output = []
        
        # JSON with null values
        null_json = json.dumps({
            "news_id": "null-test",
            "headline": "Tesla news",
            "url": "https://example.com",
            "published": "2024-01-01T10:00:00Z",
            "full_text": None
        })
        
        enrichment_function.process_element(null_json, mock_context, output)
        
        # Should handle null full_text gracefully
        assert len(output) == 1  # Tesla detected
    
    def test_enrichment_client_exception(self, mock_context):
        """Test handling when enrichment client throws exception."""
        # Create function with mock that throws exception
        function = NewsEnrichmentFunction("http://test:8080", use_mock=False)
        
        with patch('news_processing_job.EnrichmentClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.enrich_news.side_effect = Exception("Enrichment failed")
            mock_client_class.return_value = mock_client
            
            function.client = mock_client
            
            output = []
            valid_json = json.dumps({
                "news_id": "exception-test",
                "headline": "Tesla news",
                "url": "https://example.com",
                "published": "2024-01-01T10:00:00Z"
            })
            
            # Should not crash despite exception
            function.process_element(valid_json, mock_context, output)
            
            assert len(output) == 0  # No output due to exception


class TestWindowFunctionRobustness:
    """Test robustness of window function."""
    
    @pytest.fixture
    def window_function(self):
        """CompanyFeaturesWindowFunction instance."""
        return CompanyFeaturesWindowFunction()
    
    @pytest.fixture
    def mock_context(self):
        """Mock window context."""
        context = MagicMock()
        context.window.return_value.end = 1704110400000  # 2024-01-01T12:00:00Z
        return context
    
    def test_empty_elements_iterator(self, window_function, mock_context):
        """Test handling empty iterator."""
        output = []
        
        window_function.process(mock_context, iter([]), output)
        
        assert len(output) == 0
    
    def test_mixed_valid_invalid_elements(self, window_function, mock_context):
        """Test processing mix of valid and invalid elements."""
        elements = [
            '{"ticker": "TSLA", "sentiment": -0.7}',  # Valid
            '{"invalid": json}',                       # Invalid JSON
            '{"ticker": "AAPL"}',                     # Missing sentiment
            '{"sentiment": 0.5}',                     # Missing ticker
            '{"ticker": "MSFT", "sentiment": 0.3}',   # Valid
            '',                                       # Empty string
            'null',                                   # JSON null
            '[]',                                     # Wrong JSON type
        ]
        
        output = []
        window_function.process(mock_context, elements, output)
        
        # Should process only the 2 valid elements
        assert len(output) == 2
        
        tickers = []
        for feature_json in output:
            features = json.loads(feature_json)
            tickers.append(features["ticker"])
        
        assert "TSLA" in tickers
        assert "MSFT" in tickers
    
    def test_extreme_sentiment_values(self, window_function, mock_context):
        """Test handling of extreme sentiment values.""" 
        elements = [
            '{"ticker": "EXTREME", "sentiment": -999.9}',  # Way outside range
            '{"ticker": "EXTREME", "sentiment": 999.9}',   # Way outside range
            '{"ticker": "EXTREME", "sentiment": -1.0}',    # Min valid
            '{"ticker": "EXTREME", "sentiment": 1.0}',     # Max valid
            '{"ticker": "EXTREME", "sentiment": 0.0}',     # Zero
        ]
        
        output = []
        window_function.process(mock_context, elements, output)
        
        assert len(output) == 1
        features = json.loads(output[0])
        
        # All values should be processed (even extreme ones)
        assert features["ticker"] == "EXTREME"
        assert features["neg_news_count_24h"] == 3  # -999.9, -1.0, plus extreme positive counted as negative?
        assert features["pos_news_count_24h"] == 2  # 999.9, 1.0
    
    def test_very_large_window(self, window_function, mock_context):
        """Test processing very large window with many elements."""
        # Generate 10,000 elements
        elements = []
        for i in range(10000):
            ticker = f"STOCK{i % 100}"  # 100 different stocks
            sentiment = (i % 7 - 3) * 0.2  # Range from -0.6 to 0.6
            elements.append(f'{{"ticker": "{ticker}", "sentiment": {sentiment}}}')
        
        output = []
        window_function.process(mock_context, elements, output)
        
        # Should produce features for 100 unique stocks
        assert len(output) == 100
        
        # Verify no duplicate tickers
        tickers = set()
        for feature_json in output:
            features = json.loads(feature_json)
            ticker = features["ticker"]
            assert ticker not in tickers  # No duplicates
            tickers.add(ticker)
    
    def test_window_timestamp_edge_cases(self, window_function):
        """Test window with edge case timestamps."""
        # Test various edge case timestamps
        edge_timestamps = [
            0,                    # Unix epoch start
            2147483647000,        # Near 32-bit int limit
            1704067200000,        # Start of 2024
        ]
        
        for timestamp_ms in edge_timestamps:
            context = MagicMock()
            context.window.return_value.end = timestamp_ms
            
            elements = ['{"ticker": "TEST", "sentiment": 0.5}']
            output = []
            
            window_function.process(context, elements, output)
            
            assert len(output) == 1
            features = json.loads(output[0])
            assert "window_end" in features
            assert features["window_end"].endswith("Z")  # ISO format
    
    def test_concurrent_processing_simulation(self, window_function, mock_context):
        """Test simulation of concurrent processing scenarios."""
        import threading
        
        # Simulate multiple threads processing same window
        elements = [f'{{"ticker": "CONCURRENT", "sentiment": {i * 0.1}}}' for i in range(100)]
        outputs = []
        
        def process_subset(start, end):
            subset = elements[start:end]
            output = []
            window_function.process(mock_context, subset, output)
            outputs.extend(output)
        
        threads = []
        for i in range(0, 100, 20):  # 5 threads, 20 elements each
            thread = threading.Thread(target=process_subset, args=(i, i + 20))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should have 5 outputs (one per thread)
        assert len(outputs) == 5
        
        # All should be for same ticker
        for output_json in outputs:
            features = json.loads(output_json)
            assert features["ticker"] == "CONCURRENT"


class TestSystemErrorRecovery:
    """Test system-level error recovery."""
    
    def test_memory_pressure_simulation(self):
        """Test behavior under simulated memory pressure."""
        # Create many large objects to simulate memory pressure
        large_objects = []
        
        try:
            # Allocate memory
            for i in range(100):
                large_objects.append("x" * 100000)  # 100KB each
            
            # Try to process normal workload under "pressure"
            function = CompanyFeaturesWindowFunction()
            context = MagicMock()
            context.window.return_value.end = 1704110400000
            
            elements = ['{"ticker": "MEMORY", "sentiment": 0.1}'] * 1000
            output = []
            
            function.process(context, elements, output)
            
            assert len(output) == 1
            features = json.loads(output[0])
            assert features["ticker"] == "MEMORY"
            assert features["pos_news_count_24h"] == 1000
            
        finally:
            # Clean up
            large_objects.clear()
    
    def test_corrupted_data_recovery(self):
        """Test recovery from corrupted data scenarios."""
        function = CompanyFeaturesWindowFunction()
        context = MagicMock()
        context.window.return_value.end = 1704110400000
        
        # Mix of good data with various corruption scenarios
        elements = [
            '{"ticker": "GOOD", "sentiment": 0.5}',           # Good
            b'\x80\x81\x82'.decode('latin1'),                 # Binary garbage
            '{"ticker": "GOOD2", "sentiment": 0.3}',          # Good
            '{"ticker": null, "sentiment": 0.1}',             # Null ticker
            '{"ticker": "GOOD3", "sentiment": 0.2}',          # Good
            '{"ticker": "", "sentiment": 0.4}',               # Empty ticker
            '\x00\x00\x00\x00',                              # Null bytes
            '{"ticker": "GOOD4", "sentiment": -0.1}',         # Good
        ]
        
        output = []
        function.process(context, elements, output)
        
        # Should process only the good elements
        good_tickers = {"GOOD", "GOOD2", "GOOD3", "GOOD4"}
        assert len(output) == len(good_tickers)
        
        found_tickers = set()
        for output_json in output:
            features = json.loads(output_json)
            found_tickers.add(features["ticker"])
        
        # Should have processed all good tickers
        assert found_tickers == good_tickers
    
    def test_resource_cleanup(self):
        """Test proper resource cleanup in error scenarios.""" 
        client = MockEnrichmentClient()
        
        # Simulate processing that might leak resources
        async def leak_simulation():
            tasks = []
            try:
                for i in range(100):
                    news = NewsMessage(
                        news_id=f"test-{i}",
                        headline="Test news",
                        url="https://example.com",
                        published="2024-01-01T10:00:00Z"
                    )
                    # Create many concurrent tasks
                    task = asyncio.create_task(client.enrich_news(news))
                    tasks.append(task)
                
                # Process some, then simulate error
                results = await asyncio.gather(*tasks[:50])
                raise Exception("Simulated error")
                
            except Exception:
                # Cancel remaining tasks (simulate cleanup)
                for task in tasks[50:]:
                    if not task.done():
                        task.cancel()
                
                # Wait for cancellation
                await asyncio.gather(*tasks[50:], return_exceptions=True)
            
            return len([r for r in results if r is not None])
        
        # Should handle cleanup gracefully
        result = asyncio.run(leak_simulation())
        assert result == 50  # First 50 should complete


class TestDataConsistency:
    """Test data consistency and integrity."""
    
    def test_feature_calculation_consistency(self):
        """Test that feature calculations are consistent across runs."""
        from news_processing_job import FeatureAggregator
        
        # Same input should produce same output
        test_sentiments = [-0.8, 0.5, -0.3, 0.7, -0.1, 0.2, -0.9]
        
        results = []
        for run in range(5):  # Run 5 times
            aggregator = FeatureAggregator()
            for sentiment in test_sentiments:
                aggregator.add_mention(sentiment)
            
            features = aggregator.get_features("CONSISTENT", datetime(2024, 1, 1, 12, 0))
            results.append({
                'neg': features.neg_news_count_24h,
                'pos': features.pos_news_count_24h,
                'ewm': features.sentiment_ewm_7d
            })
        
        # All runs should produce identical results
        first_result = results[0]
        for result in results[1:]:
            assert result['neg'] == first_result['neg']
            assert result['pos'] == first_result['pos']
            assert abs(result['ewm'] - first_result['ewm']) < 0.000001  # Allow for tiny floating point differences
    
    def test_json_serialization_roundtrip(self):
        """Test JSON serialization/deserialization doesn't lose data."""
        original_features = CompanyFeatures(
            ticker="TEST.SERIALIZE",
            window_end="2024-01-01T12:30:45.123456Z",  # High precision
            neg_news_count_24h=12345,
            pos_news_count_24h=67890,
            sentiment_ewm_7d=-0.123456789  # High precision
        )
        
        # Serialize and deserialize
        json_str = original_features.to_json()
        parsed = json.loads(json_str)
        
        # Recreate from parsed data
        reconstructed = CompanyFeatures(
            ticker=parsed["ticker"],
            window_end=parsed["window_end"],
            neg_news_count_24h=parsed["neg_news_count_24h"],
            pos_news_count_24h=parsed["pos_news_count_24h"],
            sentiment_ewm_7d=parsed["sentiment_ewm_7d"]
        )
        
        # Should be identical
        assert original_features.ticker == reconstructed.ticker
        assert original_features.window_end == reconstructed.window_end
        assert original_features.neg_news_count_24h == reconstructed.neg_news_count_24h
        assert original_features.pos_news_count_24h == reconstructed.pos_news_count_24h
        assert abs(original_features.sentiment_ewm_7d - reconstructed.sentiment_ewm_7d) < 0.000001
    
    def test_redis_key_uniqueness(self):
        """Test that Redis keys are unique across different scenarios."""
        keys = set()
        
        # Different tickers, same time
        for ticker in ["AAPL", "TSLA", "MSFT", "GOOGL"]:
            features = CompanyFeatures(
                ticker=ticker,
                window_end="2024-01-01T12:00:00Z",
                neg_news_count_24h=1, pos_news_count_24h=0, sentiment_ewm_7d=0.0
            )
            key = features.to_redis_key()
            assert key not in keys
            keys.add(key)
        
        # Same ticker, different times
        for minute in range(0, 60, 5):  # Every 5 minutes
            features = CompanyFeatures(
                ticker="SAME",
                window_end=f"2024-01-01T12:{minute:02d}:00Z",
                neg_news_count_24h=1, pos_news_count_24h=0, sentiment_ewm_7d=0.0
            )
            key = features.to_redis_key()
            assert key not in keys
            keys.add(key)
        
        # Should have 4 tickers + 12 time slots = 16 unique keys
        assert len(keys) == 16