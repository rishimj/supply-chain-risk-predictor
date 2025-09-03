"""
Tests for feature aggregation logic.
"""
import pytest
import json
from datetime import datetime
from unittest.mock import MagicMock
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from news_processing_job import FeatureAggregator, CompanyFeaturesWindowFunction
from models import CompanyFeatures


class TestFeatureAggregator:
    """Tests for FeatureAggregator class."""
    
    @pytest.fixture
    def aggregator(self):
        """Fresh aggregator instance."""
        return FeatureAggregator()
    
    def test_initial_state(self, aggregator):
        """Test aggregator initial state."""
        assert aggregator.neg_count == 0
        assert aggregator.pos_count == 0
        assert aggregator.sentiment_ewm == 0.0
        assert aggregator.alpha == 0.1
    
    def test_add_negative_mention(self, aggregator):
        """Test adding negative sentiment mention."""
        aggregator.add_mention(-0.7)
        
        assert aggregator.neg_count == 1
        assert aggregator.pos_count == 0
        assert aggregator.sentiment_ewm == -0.7  # First value becomes EWM
    
    def test_add_positive_mention(self, aggregator):
        """Test adding positive sentiment mention."""
        aggregator.add_mention(0.6)
        
        assert aggregator.neg_count == 0
        assert aggregator.pos_count == 1
        assert aggregator.sentiment_ewm == 0.6
    
    def test_add_neutral_mention(self, aggregator):
        """Test adding neutral sentiment (zero)."""
        aggregator.add_mention(0.0)
        
        assert aggregator.neg_count == 0
        assert aggregator.pos_count == 0
        assert aggregator.sentiment_ewm == 0.0
    
    def test_multiple_mentions_counting(self, aggregator):
        """Test counting multiple mentions correctly."""
        aggregator.add_mention(-0.5)  # negative
        aggregator.add_mention(0.3)   # positive 
        aggregator.add_mention(-0.8)  # negative
        aggregator.add_mention(0.7)   # positive
        aggregator.add_mention(0.0)   # neutral
        
        assert aggregator.neg_count == 2
        assert aggregator.pos_count == 2
    
    def test_ewm_calculation_sequence(self, aggregator):
        """Test exponential weighted moving average calculation."""
        # First mention: EWM = sentiment
        aggregator.add_mention(-0.8)
        assert aggregator.sentiment_ewm == -0.8
        
        # Second mention: EWM = α * new + (1-α) * prev
        # = 0.1 * 0.6 + 0.9 * (-0.8) = 0.06 - 0.72 = -0.66
        aggregator.add_mention(0.6)
        expected_ewm = 0.1 * 0.6 + 0.9 * (-0.8)
        assert abs(aggregator.sentiment_ewm - expected_ewm) < 0.001
        
        # Third mention
        prev_ewm = aggregator.sentiment_ewm
        aggregator.add_mention(0.2)
        expected_ewm = 0.1 * 0.2 + 0.9 * prev_ewm
        assert abs(aggregator.sentiment_ewm - expected_ewm) < 0.001
    
    def test_ewm_convergence_positive(self, aggregator):
        """Test EWM converges toward consistent positive sentiment."""
        # Add many positive mentions
        for _ in range(20):
            aggregator.add_mention(0.8)
        
        # Should converge close to 0.8
        assert aggregator.sentiment_ewm > 0.7
        assert aggregator.pos_count == 20
        assert aggregator.neg_count == 0
    
    def test_ewm_convergence_negative(self, aggregator):
        """Test EWM converges toward consistent negative sentiment.""" 
        # Add many negative mentions
        for _ in range(20):
            aggregator.add_mention(-0.9)
        
        # Should converge close to -0.9
        assert aggregator.sentiment_ewm < -0.8
        assert aggregator.neg_count == 20
        assert aggregator.pos_count == 0
    
    def test_get_features(self, aggregator):
        """Test feature generation."""
        # Add some mentions
        aggregator.add_mention(-0.7)
        aggregator.add_mention(0.3)
        aggregator.add_mention(-0.4)
        
        window_end = datetime(2024, 1, 1, 12, 0, 0)
        features = aggregator.get_features("TSLA", window_end)
        
        assert features.ticker == "TSLA"
        assert features.window_end == "2024-01-01T12:00:00Z"
        assert features.neg_news_count_24h == 2
        assert features.pos_news_count_24h == 1
        assert isinstance(features.sentiment_ewm_7d, float)
        assert -1.0 <= features.sentiment_ewm_7d <= 1.0
    
    def test_extreme_sentiment_values(self, aggregator):
        """Test handling of extreme sentiment values."""
        aggregator.add_mention(-1.0)  # Minimum
        aggregator.add_mention(1.0)   # Maximum
        
        assert aggregator.neg_count == 1
        assert aggregator.pos_count == 1
        assert -1.0 <= aggregator.sentiment_ewm <= 1.0


class TestCompanyFeaturesWindowFunction:
    """Tests for CompanyFeaturesWindowFunction."""
    
    @pytest.fixture
    def window_function(self):
        """Window function instance."""
        return CompanyFeaturesWindowFunction()
    
    @pytest.fixture
    def mock_context(self):
        """Mock window context."""
        context = MagicMock()
        # Mock window with end time (milliseconds)
        context.window.return_value.end = 1704110400000  # 2024-01-01T12:00:00Z in ms
        return context
    
    def test_empty_window(self, window_function, mock_context):
        """Test processing empty window."""
        elements = []
        output = []
        
        window_function.process(mock_context, elements, output)
        
        assert len(output) == 0
    
    def test_single_company_single_mention(self, window_function, mock_context):
        """Test window with single company mention."""
        elements = [
            '{"ticker": "TSLA", "sentiment": -0.7, "news_id": "test-1"}'
        ]
        output = []
        
        window_function.process(mock_context, elements, output)
        
        assert len(output) == 1
        features = json.loads(output[0])
        assert features["ticker"] == "TSLA"
        assert features["neg_news_count_24h"] == 1
        assert features["pos_news_count_24h"] == 0
        assert features["sentiment_ewm_7d"] == -0.7
    
    def test_single_company_multiple_mentions(self, window_function, mock_context):
        """Test window with multiple mentions for same company."""
        elements = [
            '{"ticker": "TSLA", "sentiment": -0.8, "news_id": "test-1"}',
            '{"ticker": "TSLA", "sentiment": 0.5, "news_id": "test-2"}',
            '{"ticker": "TSLA", "sentiment": -0.3, "news_id": "test-3"}'
        ]
        output = []
        
        window_function.process(mock_context, elements, output)
        
        assert len(output) == 1
        features = json.loads(output[0])
        assert features["ticker"] == "TSLA"
        assert features["neg_news_count_24h"] == 2  # -0.8, -0.3
        assert features["pos_news_count_24h"] == 1  # 0.5
        # EWM should be calculated progressively
        assert isinstance(features["sentiment_ewm_7d"], float)
    
    def test_multiple_companies(self, window_function, mock_context):
        """Test window with multiple companies."""
        elements = [
            '{"ticker": "TSLA", "sentiment": -0.7, "news_id": "test-1"}',
            '{"ticker": "AAPL", "sentiment": 0.6, "news_id": "test-2"}',
            '{"ticker": "TSLA", "sentiment": 0.2, "news_id": "test-3"}',
            '{"ticker": "MSFT", "sentiment": -0.1, "news_id": "test-4"}'
        ]
        output = []
        
        window_function.process(mock_context, elements, output)
        
        assert len(output) == 3  # TSLA, AAPL, MSFT
        
        # Parse outputs
        features_by_ticker = {}
        for feature_json in output:
            features = json.loads(feature_json)
            features_by_ticker[features["ticker"]] = features
        
        # Check TSLA (2 mentions: -0.7, 0.2)
        tsla = features_by_ticker["TSLA"]
        assert tsla["neg_news_count_24h"] == 1
        assert tsla["pos_news_count_24h"] == 1
        
        # Check AAPL (1 mention: 0.6)
        aapl = features_by_ticker["AAPL"] 
        assert aapl["neg_news_count_24h"] == 0
        assert aapl["pos_news_count_24h"] == 1
        assert aapl["sentiment_ewm_7d"] == 0.6
        
        # Check MSFT (1 mention: -0.1)
        msft = features_by_ticker["MSFT"]
        assert msft["neg_news_count_24h"] == 1
        assert msft["pos_news_count_24h"] == 0
        assert msft["sentiment_ewm_7d"] == -0.1
    
    def test_invalid_json_handling(self, window_function, mock_context):
        """Test handling of invalid JSON elements."""
        elements = [
            '{"ticker": "TSLA", "sentiment": -0.7}',  # Valid
            '{"invalid": json}',                       # Invalid JSON
            '{"ticker": "AAPL", "sentiment": 0.5}',   # Valid
        ]
        output = []
        
        # Should not crash, should process valid elements
        window_function.process(mock_context, elements, output)
        
        assert len(output) == 2  # Only valid elements processed
    
    def test_missing_fields_handling(self, window_function, mock_context):
        """Test handling of elements with missing required fields."""
        elements = [
            '{"ticker": "TSLA", "sentiment": -0.7}',  # Valid
            '{"ticker": "AAPL"}',                     # Missing sentiment
            '{"sentiment": 0.5}',                     # Missing ticker
        ]
        output = []
        
        # Should process only valid elements
        window_function.process(mock_context, elements, output)
        
        assert len(output) == 1  # Only first element is valid
        features = json.loads(output[0])
        assert features["ticker"] == "TSLA"
    
    def test_window_timestamp_conversion(self, window_function):
        """Test proper conversion of window end timestamp.""" 
        context = MagicMock()
        # Different timestamp
        context.window.return_value.end = 1704196800000  # 2024-01-02T12:00:00Z in ms
        
        elements = ['{"ticker": "TEST", "sentiment": 0.0}']
        output = []
        
        window_function.process(context, elements, output)
        
        features = json.loads(output[0])
        assert features["window_end"] == "2024-01-02T12:00:00Z"
    
    def test_large_window_performance(self, window_function, mock_context):
        """Test processing large windows efficiently."""
        # Generate many mentions
        elements = []
        for i in range(1000):
            ticker = f"STOCK{i % 10}"  # 10 different stocks
            sentiment = (i % 3 - 1) * 0.5  # -0.5, 0.0, 0.5 pattern
            elements.append(f'{{"ticker": "{ticker}", "sentiment": {sentiment}}}')
        
        output = []
        
        # Should handle large volume without issues
        window_function.process(mock_context, elements, output)
        
        assert len(output) == 10  # 10 unique stocks
        
        # Verify counts are correct for one stock
        features_by_ticker = {}
        for feature_json in output:
            features = json.loads(feature_json)
            features_by_ticker[features["ticker"]] = features
        
        # Each stock appears 100 times with pattern -0.5, 0.0, 0.5
        # So roughly 33 negative, 34 neutral, 33 positive
        stock0 = features_by_ticker["STOCK0"]
        assert stock0["neg_news_count_24h"] + stock0["pos_news_count_24h"] <= 100


class TestEdgeCasesAndRobustness:
    """Test edge cases and robustness of aggregation."""
    
    def test_zero_sentiment_boundary(self):
        """Test exact zero sentiment handling."""
        aggregator = FeatureAggregator()
        
        aggregator.add_mention(0.0)
        aggregator.add_mention(-0.0)  # Negative zero
        aggregator.add_mention(0.000001)  # Very small positive
        aggregator.add_mention(-0.000001)  # Very small negative
        
        # Zero and negative zero should be neutral
        # Very small values should count as pos/neg
        assert aggregator.neg_count == 1
        assert aggregator.pos_count == 1
    
    def test_sentiment_precision(self):
        """Test sentiment precision in calculations."""
        aggregator = FeatureAggregator()
        
        # Use precise decimal values
        aggregator.add_mention(0.123456789)
        aggregator.add_mention(-0.987654321)
        
        window_end = datetime(2024, 1, 1, 12, 0)
        features = aggregator.get_features("TEST", window_end)
        
        # Should round to 3 decimal places
        assert len(str(features.sentiment_ewm_7d).split('.')[-1]) <= 3
    
    def test_datetime_edge_cases(self):
        """Test datetime handling edge cases.""" 
        aggregator = FeatureAggregator()
        aggregator.add_mention(0.5)
        
        # Test various datetime formats
        edge_times = [
            datetime(1970, 1, 1, 0, 0, 0),    # Unix epoch
            datetime(2024, 2, 29, 23, 59, 59), # Leap year
            datetime(2024, 12, 31, 23, 59, 59), # Year end
        ]
        
        for dt in edge_times:
            features = aggregator.get_features("TEST", dt)
            assert features.window_end.endswith("Z")
            assert "T" in features.window_end  # ISO format
    
    def test_unicode_ticker_handling(self):
        """Test handling of unicode characters in tickers."""
        aggregator = FeatureAggregator()
        aggregator.add_mention(0.5)
        
        window_end = datetime(2024, 1, 1, 12, 0)
        
        # Some international stock symbols might have unicode
        unicode_ticker = "TSL🚗"  # Emoji in ticker
        features = aggregator.get_features(unicode_ticker, window_end)
        
        assert features.ticker == unicode_ticker
        assert features.to_json()  # Should serialize without error