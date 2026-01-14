"""
Tests for Redis-backed alert cooldown tracking.
Following TDD approach - tests written first.
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from alerting import AlertThresholdChecker, AlertConfig
from models import CompanyFeatures


class TestAlertThresholdCheckerWithRedis:
    """Test suite for Redis-backed alert cooldown tracking."""
    
    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = Mock()
        redis.exists = Mock(return_value=False)
        redis.get = Mock(return_value=None)
        redis.setex = Mock(return_value=True)
        return redis
    
    @pytest.fixture
    def alert_config(self):
        """Create alert config with 0.7 threshold."""
        return AlertConfig(
            company_thresholds={},
            default_threshold=0.7,
            cooldown_minutes=30
        )
    
    @pytest.fixture
    def high_risk_features(self):
        """Create high-risk features (above 0.7 threshold)."""
        return CompanyFeatures(
            ticker="AAPL",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=5,
            pos_news_count_5m=1,
            sentiment_score_5m=-0.3,
            risk_score_5m=0.8
        )
    
    @pytest.fixture
    def low_risk_features(self):
        """Create low-risk features (below 0.7 threshold)."""
        return CompanyFeatures(
            ticker="AAPL",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=1,
            pos_news_count_5m=5,
            sentiment_score_5m=0.5,
            risk_score_5m=0.5
        )
    
    def test_should_trigger_first_alert(self, mock_redis, alert_config, high_risk_features):
        """Test 1: No Redis key exists, should trigger alert."""
        # Setup: No Redis key exists
        mock_redis.exists.return_value = False
        
        checker = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        
        # Act
        should_trigger = checker.should_trigger_alert(high_risk_features)
        
        # Assert
        assert should_trigger is True, "Should trigger alert when no cooldown exists"
        mock_redis.exists.assert_called_once_with("alert:last_sent:AAPL")
    
    def test_should_not_trigger_during_cooldown(self, mock_redis, alert_config, high_risk_features):
        """Test 2: Redis key exists (recent alert), should NOT trigger."""
        # Setup: Redis key exists with recent timestamp
        recent_timestamp = (datetime.utcnow() - timedelta(minutes=5)).isoformat() + "Z"
        mock_redis.exists.return_value = True
        mock_redis.get.return_value = recent_timestamp
        
        checker = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        
        # Act
        should_trigger = checker.should_trigger_alert(high_risk_features)
        
        # Assert
        assert should_trigger is False, "Should NOT trigger alert during cooldown period"
        mock_redis.exists.assert_called_once_with("alert:last_sent:AAPL")
    
    def test_should_trigger_after_cooldown_expires(self, mock_redis, alert_config, high_risk_features):
        """Test 3: Redis key expired/doesn't exist, should trigger again."""
        # Setup: Redis key doesn't exist (expired via TTL)
        mock_redis.exists.return_value = False
        
        checker = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        
        # Act
        should_trigger = checker.should_trigger_alert(high_risk_features)
        
        # Assert
        assert should_trigger is True, "Should trigger alert after cooldown expires"
    
    def test_record_alert_sets_redis_key(self, mock_redis, alert_config):
        """Test 4: Verify record_alert() sets Redis key with correct TTL."""
        checker = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        
        # Act
        checker.record_alert("AAPL")
        
        # Assert
        mock_redis.setex.assert_called_once()
        call_args = mock_redis.setex.call_args
        
        # Verify key, TTL, and value format
        assert call_args[0][0] == "alert:last_sent:AAPL", "Key should be alert:last_sent:AAPL"
        assert call_args[0][1] == 1800, "TTL should be 1800 seconds (30 minutes)"
        assert "Z" in call_args[0][2], "Value should be ISO timestamp with Z suffix"
    
    def test_multiple_tickers_independent_cooldowns(self, mock_redis, alert_config):
        """Test 5: Different tickers have independent cooldowns."""
        # Setup: AAPL has cooldown, TSLA does not
        def exists_side_effect(key):
            return key == "alert:last_sent:AAPL"
        
        mock_redis.exists.side_effect = exists_side_effect
        
        checker = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        
        # AAPL features (in cooldown)
        aapl_features = CompanyFeatures(
            ticker="AAPL",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=5,
            pos_news_count_5m=1,
            sentiment_score_5m=-0.3,
            risk_score_5m=0.8
        )
        
        # TSLA features (not in cooldown)
        tsla_features = CompanyFeatures(
            ticker="TSLA",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=5,
            pos_news_count_5m=1,
            sentiment_score_5m=-0.3,
            risk_score_5m=0.8
        )
        
        # Act & Assert
        assert checker.should_trigger_alert(aapl_features) is False, "AAPL should be in cooldown"
        assert checker.should_trigger_alert(tsla_features) is True, "TSLA should trigger (no cooldown)"
    
    def test_below_threshold_never_triggers(self, mock_redis, alert_config, low_risk_features):
        """Test 6: Risk score below threshold never triggers (no Redis check needed)."""
        checker = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        
        # Act
        should_trigger = checker.should_trigger_alert(low_risk_features)
        
        # Assert
        assert should_trigger is False, "Should NOT trigger when risk score below threshold"
        # Redis should not be checked if threshold not exceeded
        mock_redis.exists.assert_not_called()
    
    def test_redis_connection_failure_graceful(self, alert_config, high_risk_features):
        """Test 7: Redis failure is handled gracefully (fail-safe mode)."""
        # Setup: Redis client that raises exceptions
        mock_redis = Mock()
        mock_redis.exists.side_effect = Exception("Redis connection failed")
        
        checker = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        
        # Act - should not raise exception
        should_trigger = checker.should_trigger_alert(high_risk_features)
        
        # Assert - fail safe: allow alert on Redis error (better to send than miss critical alert)
        assert should_trigger is True, "Should fail safe (allow alert) on Redis error to not miss critical alerts"
    
    def test_cooldown_persists_across_restarts(self, mock_redis, alert_config, high_risk_features):
        """Test 8: Cooldown persists across AlertThresholdChecker restarts (Redis persistence)."""
        # Simulate first instance recording an alert
        checker1 = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        checker1.record_alert("AAPL")
        
        # Verify Redis was written
        assert mock_redis.setex.called, "First instance should write to Redis"
        
        # Simulate second instance (restart) checking cooldown
        mock_redis.exists.return_value = True
        checker2 = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        
        # Act
        should_trigger = checker2.should_trigger_alert(high_risk_features)
        
        # Assert
        assert should_trigger is False, "Second instance should see cooldown from Redis"
        mock_redis.exists.assert_called_with("alert:last_sent:AAPL")
    
    def test_no_redis_client_skips_cooldown_check(self, alert_config, high_risk_features):
        """Test: When no Redis client provided, cooldown checks are skipped."""
        checker = AlertThresholdChecker(alert_config, redis_client=None)
        
        # Act
        should_trigger = checker.should_trigger_alert(high_risk_features)
        
        # Assert - without Redis, still checks threshold
        assert should_trigger is True, "Should still check threshold without Redis"
    
    def test_record_alert_without_redis_logs_warning(self, alert_config):
        """Test: Recording alert without Redis client logs warning but doesn't crash."""
        checker = AlertThresholdChecker(alert_config, redis_client=None)
        
        # Act - should not raise exception
        checker.record_alert("AAPL")
        
        # Assert - no exception raised, function completes
        assert True, "Should handle missing Redis gracefully"

