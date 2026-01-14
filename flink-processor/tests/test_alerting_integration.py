"""
Integration tests for complete alert flow with Redis cooldown.
Tests the full pipeline from CompanyFeatures to Slack notification.
"""
import pytest
import asyncio
import json
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from alerting import (
    AlertManager,
    AlertThresholdChecker,
    SlackNotificationService,
    AlertConfig,
    create_alert_manager
)
from models import CompanyFeatures


class TestEndToEndAlertFlow:
    """Test complete alert flow with Redis cooldown."""
    
    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client that simulates real behavior."""
        redis = Mock()
        redis_state = {}  # Simulate Redis key-value store
        
        def exists_impl(key):
            return key in redis_state
        
        def get_impl(key):
            return redis_state.get(key)
        
        def setex_impl(key, ttl, value):
            redis_state[key] = value
            return True
        
        def delete_impl(key):
            if key in redis_state:
                del redis_state[key]
            return True
        
        redis.exists = Mock(side_effect=exists_impl)
        redis.get = Mock(side_effect=get_impl)
        redis.setex = Mock(side_effect=setex_impl)
        redis.delete = Mock(side_effect=delete_impl)
        
        return redis
    
    @pytest.fixture
    def mock_slack_service(self):
        """Create a mock Slack service that always succeeds."""
        service = Mock(spec=SlackNotificationService)
        service.send_alert = AsyncMock(return_value=True)
        return service
    
    @pytest.fixture
    def alert_config(self):
        """Alert config with 0.7 threshold and 30min cooldown."""
        return AlertConfig(
            company_thresholds={},
            default_threshold=0.7,
            cooldown_minutes=30
        )
    
    @pytest.fixture
    def high_risk_features(self):
        """High-risk features that should trigger alert."""
        return CompanyFeatures(
            ticker="AAPL",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=5,
            pos_news_count_5m=1,
            sentiment_score_5m=-0.3,
            risk_score_5m=0.85
        )
    
    @pytest.mark.asyncio
    async def test_end_to_end_alert_flow(self, mock_redis, mock_slack_service, alert_config, high_risk_features):
        """
        Test 1: Complete alert flow
        - Send high-risk features (0.85)
        - Verify: Alert sent, Redis key created
        - Send again within 30min
        - Verify: Alert suppressed (cooldown)
        """
        # Setup
        threshold_checker = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        alert_manager = AlertManager(threshold_checker, mock_slack_service)
        
        # Act 1: Send first alert
        alert_sent_1 = await alert_manager.process_features(high_risk_features)
        
        # Assert 1: Alert should be sent
        assert alert_sent_1 is True, "First alert should be sent"
        mock_slack_service.send_alert.assert_called_once()
        
        # Verify Redis key was created
        assert mock_redis.setex.called, "Redis key should be created"
        redis_key_call = mock_redis.setex.call_args
        assert redis_key_call[0][0] == "alert:last_sent:AAPL"
        assert redis_key_call[0][1] == 1800  # 30 minutes in seconds
        
        # Act 2: Try to send alert again (within cooldown)
        alert_sent_2 = await alert_manager.process_features(high_risk_features)
        
        # Assert 2: Alert should be suppressed
        assert alert_sent_2 is False, "Second alert should be suppressed (cooldown)"
        assert mock_slack_service.send_alert.call_count == 1, "Slack should not be called again"
        
        # Verify statistics
        stats = alert_manager.get_statistics()
        assert stats['alerts_sent'] == 1, "Should have sent 1 alert"
        assert stats['alerts_suppressed'] == 1, "Should have suppressed 1 alert"
    
    @pytest.mark.asyncio
    async def test_multiple_companies_independent_cooldowns(self, mock_redis, mock_slack_service, alert_config):
        """Test that different companies have independent cooldowns."""
        threshold_checker = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        alert_manager = AlertManager(threshold_checker, mock_slack_service)
        
        # Create features for two companies
        aapl_features = CompanyFeatures(
            ticker="AAPL",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=5,
            pos_news_count_5m=1,
            sentiment_score_5m=-0.3,
            risk_score_5m=0.85
        )
        
        tsla_features = CompanyFeatures(
            ticker="TSLA",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=4,
            pos_news_count_5m=1,
            sentiment_score_5m=-0.2,
            risk_score_5m=0.80
        )
        
        # Send alert for AAPL
        alert_sent_aapl = await alert_manager.process_features(aapl_features)
        assert alert_sent_aapl is True, "AAPL alert should be sent"
        
        # Send alert for TSLA (should still work - independent cooldown)
        alert_sent_tsla = await alert_manager.process_features(tsla_features)
        assert alert_sent_tsla is True, "TSLA alert should be sent (independent cooldown)"
        
        # Try AAPL again (should be suppressed)
        alert_sent_aapl_2 = await alert_manager.process_features(aapl_features)
        assert alert_sent_aapl_2 is False, "AAPL second alert should be suppressed"
        
        # Try TSLA again (should also be suppressed)
        alert_sent_tsla_2 = await alert_manager.process_features(tsla_features)
        assert alert_sent_tsla_2 is False, "TSLA second alert should be suppressed"
        
        # Verify both Redis keys exist
        assert mock_redis.exists("alert:last_sent:AAPL"), "AAPL Redis key should exist"
        assert mock_redis.exists("alert:last_sent:TSLA"), "TSLA Redis key should exist"
    
    @pytest.mark.asyncio
    async def test_below_threshold_never_triggers(self, mock_redis, mock_slack_service, alert_config):
        """Test that low-risk features never trigger alerts."""
        threshold_checker = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        alert_manager = AlertManager(threshold_checker, mock_slack_service)
        
        low_risk_features = CompanyFeatures(
            ticker="GOOGL",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=1,
            pos_news_count_5m=5,
            sentiment_score_5m=0.5,
            risk_score_5m=0.5  # Below 0.7 threshold
        )
        
        # Act
        alert_sent = await alert_manager.process_features(low_risk_features)
        
        # Assert
        assert alert_sent is False, "Low-risk features should not trigger alert"
        mock_slack_service.send_alert.assert_not_called()
        assert not mock_redis.setex.called, "Redis should not be updated for non-alerts"
    
    @pytest.mark.asyncio
    async def test_slack_failure_does_not_record_cooldown(self, mock_redis, alert_config, high_risk_features):
        """Test that if Slack fails, cooldown is not recorded."""
        # Setup Slack service that fails
        failing_slack = Mock(spec=SlackNotificationService)
        failing_slack.send_alert = AsyncMock(return_value=False)
        
        threshold_checker = AlertThresholdChecker(alert_config, redis_client=mock_redis)
        alert_manager = AlertManager(threshold_checker, failing_slack)
        
        # Act
        alert_sent = await alert_manager.process_features(high_risk_features)
        
        # Assert
        assert alert_sent is False, "Alert should fail"
        failing_slack.send_alert.assert_called_once()
        
        # Verify cooldown was NOT recorded (since Slack failed)
        assert not mock_redis.setex.called, "Cooldown should not be recorded on Slack failure"
    
    @pytest.mark.asyncio
    async def test_create_alert_manager_factory(self, mock_redis):
        """Test the create_alert_manager factory function."""
        # Act
        alert_manager = create_alert_manager(
            webhook_url="https://hooks.slack.com/test",
            channel="#test-alerts",
            default_threshold=0.7,
            cooldown_minutes=30,
            redis_client=mock_redis
        )
        
        # Assert
        assert alert_manager is not None
        assert isinstance(alert_manager, AlertManager)
        assert alert_manager.threshold_checker.redis_client == mock_redis
        assert alert_manager.threshold_checker.config.default_threshold == 0.7
        assert alert_manager.threshold_checker.config.cooldown_minutes == 30


class TestAlertingSinkIntegration:
    """Test AlertingSink with Flink-like behavior."""
    
    @pytest.fixture
    def mock_redis_url(self):
        """Mock Redis URL."""
        return "redis://localhost:6379/0"
    
    @pytest.fixture
    def alert_config_dict(self):
        """Alert configuration as dict."""
        return {
            "channel": "#supply-chain-alerts",
            "company_thresholds": {},
            "default_threshold": 0.7,
            "cooldown_minutes": 30
        }
    
    def test_alerting_sink_initialization(self, mock_redis_url, alert_config_dict):
        """Test AlertingSink can be initialized with required parameters."""
        # Import here to avoid Flink dependencies in test discovery
        from news_processing_job import AlertingSink
        
        # Act
        sink = AlertingSink(
            slack_webhook_url="disabled",
            alert_config=alert_config_dict,
            redis_url=mock_redis_url
        )
        
        # Assert
        assert sink.slack_webhook_url == "disabled"
        assert sink.alert_config == alert_config_dict
        assert sink.redis_url == mock_redis_url
        assert sink.alert_manager is None  # Not initialized until open()
    
    @patch('news_processing_job.redis')
    @patch('news_processing_job.create_alert_manager')
    def test_alerting_sink_open_initializes_redis(self, mock_create_manager, mock_redis_module, 
                                                   mock_redis_url, alert_config_dict):
        """Test AlertingSink.open() initializes Redis and AlertManager."""
        from news_processing_job import AlertingSink
        
        # Setup mocks
        mock_redis_client = Mock()
        mock_redis_client.ping = Mock()
        mock_redis_module.from_url.return_value = mock_redis_client
        
        mock_alert_manager = Mock()
        mock_create_manager.return_value = mock_alert_manager
        
        # Create sink
        sink = AlertingSink(
            slack_webhook_url="https://hooks.slack.com/test",
            alert_config=alert_config_dict,
            redis_url=mock_redis_url
        )
        
        # Act
        sink.open(None)
        
        # Assert
        mock_redis_module.from_url.assert_called_once_with(mock_redis_url, decode_responses=True)
        mock_redis_client.ping.assert_called_once()
        mock_create_manager.assert_called_once()
        
        # Verify redis_client was passed to create_alert_manager
        call_kwargs = mock_create_manager.call_args[1]
        assert call_kwargs['redis_client'] == mock_redis_client
        assert call_kwargs['default_threshold'] == 0.7
        assert call_kwargs['cooldown_minutes'] == 30
    
    @patch('news_processing_job.redis')
    def test_alerting_sink_processes_features_json(self, mock_redis_module, mock_redis_url, alert_config_dict):
        """Test AlertingSink.invoke() processes CompanyFeatures JSON."""
        from news_processing_job import AlertingSink
        
        # Setup
        mock_redis_client = Mock()
        mock_redis_client.ping = Mock()
        mock_redis_module.from_url.return_value = mock_redis_client
        
        sink = AlertingSink(
            slack_webhook_url="disabled",  # Disabled to avoid actual Slack calls
            alert_config=alert_config_dict,
            redis_url=mock_redis_url
        )
        
        # Initialize (with disabled webhook, alert_manager will be None)
        sink.open(None)
        
        # Create features JSON
        features_json = json.dumps({
            "ticker": "AAPL",
            "window_end": "2025-11-15T23:45:00Z",
            "neg_news_count_5m": 5,
            "pos_news_count_5m": 1,
            "sentiment_score_5m": -0.3,
            "risk_score_5m": 0.85
        })
        
        # Act - should not raise exception even with alert_manager=None
        sink.invoke(features_json, None)
        
        # Assert - no exception means success (graceful handling of disabled alerts)
        assert True, "Should handle disabled alerts gracefully"

