"""
Test suite for supply chain risk alerting functionality.
Following TDD approach - tests written first, then implementation.
"""
import pytest
import asyncio
import json
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch
from dataclasses import dataclass
from typing import List, Optional

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

# Import our alerting module
from alerting import (
    AlertThresholdChecker,
    SlackNotificationService,
    AlertManager,
    RiskAlert,
    AlertSeverity,
    AlertConfig
)

from models import CompanyFeatures


class TestRiskAlert:
    """Test the RiskAlert data model."""
    
    def test_risk_alert_creation(self):
        """Test creating a risk alert with all required fields."""
        alert = RiskAlert(
            ticker="AAPL",
            risk_score=0.85,
            threshold=0.7,
            severity=AlertSeverity.HIGH,
            window_end=datetime.utcnow(),
            message="High supply chain risk detected for AAPL"
        )
        
        assert alert.ticker == "AAPL"
        assert alert.risk_score == 0.85
        assert alert.threshold == 0.7
        assert alert.severity == AlertSeverity.HIGH
        assert "AAPL" in alert.message
        assert alert.triggered_at is not None
    
    def test_risk_alert_severity_classification(self):
        """Test alert severity is correctly classified based on risk score."""
        # High severity: risk >= 0.8
        high_alert = RiskAlert.from_risk_score("TSLA", 0.9, 0.7)
        assert high_alert.severity == AlertSeverity.HIGH
        
        # Medium severity: 0.6 <= risk < 0.8
        medium_alert = RiskAlert.from_risk_score("MSFT", 0.75, 0.6)
        assert medium_alert.severity == AlertSeverity.MEDIUM
        
        # Low severity: threshold < risk < 0.6
        low_alert = RiskAlert.from_risk_score("GOOGL", 0.55, 0.5)
        assert low_alert.severity == AlertSeverity.LOW
    
    def test_risk_alert_to_slack_format(self):
        """Test converting risk alert to Slack message format."""
        alert = RiskAlert(
            ticker="AAPL",
            risk_score=0.85,
            threshold=0.7,
            severity=AlertSeverity.HIGH,
            window_end=datetime(2025, 11, 15, 23, 45, 0),
            message="Critical supply chain disruption detected"
        )
        
        slack_message = alert.to_slack_message()
        
        # Should be a proper Slack message format
        assert isinstance(slack_message, dict)
        assert "text" in slack_message or "blocks" in slack_message
        assert "AAPL" in str(slack_message)
        assert "0.85" in str(slack_message)
        assert "🔴" in str(slack_message)  # High severity emoji


class TestAlertThresholdChecker:
    """Test the alert threshold checking logic."""
    
    def test_should_alert_above_threshold(self):
        """Test alert is triggered when risk score exceeds threshold."""
        config = AlertConfig(
            company_thresholds={"AAPL": 0.7, "TSLA": 0.6},
            default_threshold=0.8,
            cooldown_minutes=30
        )
        checker = AlertThresholdChecker(config)
        
        features = CompanyFeatures(
            ticker="AAPL",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=4,
            pos_news_count_5m=1,
            sentiment_score_5m=-0.6,
            risk_score_5m=0.85  # Above 0.7 threshold
        )
        
        should_alert = checker.should_trigger_alert(features)
        assert should_alert is True
    
    def test_should_not_alert_below_threshold(self):
        """Test alert is not triggered when risk score is below threshold."""
        config = AlertConfig(
            company_thresholds={"AAPL": 0.7},
            default_threshold=0.8,
            cooldown_minutes=30
        )
        checker = AlertThresholdChecker(config)
        
        features = CompanyFeatures(
            ticker="AAPL",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=2,
            pos_news_count_5m=3,
            sentiment_score_5m=0.2,
            risk_score_5m=0.5  # Below 0.7 threshold
        )
        
        should_alert = checker.should_trigger_alert(features)
        assert should_alert is False
    
    def test_uses_default_threshold_for_unknown_company(self):
        """Test uses default threshold for companies not in config."""
        config = AlertConfig(
            company_thresholds={"AAPL": 0.7},
            default_threshold=0.8,
            cooldown_minutes=30
        )
        checker = AlertThresholdChecker(config)
        
        # Unknown company should use default threshold of 0.8
        features = CompanyFeatures(
            ticker="UNKNOWN",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=3,
            pos_news_count_5m=1,
            sentiment_score_5m=-0.4,
            risk_score_5m=0.75  # Below default 0.8, should not alert
        )
        
        should_alert = checker.should_trigger_alert(features)
        assert should_alert is False
    
    def test_cooldown_prevents_duplicate_alerts(self):
        """Test cooldown mechanism prevents spam alerts."""
        config = AlertConfig(
            company_thresholds={"AAPL": 0.7},
            default_threshold=0.8,
            cooldown_minutes=30
        )
        checker = AlertThresholdChecker(config)
        
        features = CompanyFeatures(
            ticker="AAPL",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=4,
            pos_news_count_5m=1,
            sentiment_score_5m=-0.6,
            risk_score_5m=0.85
        )
        
        # First alert should trigger
        first_alert = checker.should_trigger_alert(features)
        assert first_alert is True
        
        # Mark as alerted
        checker.record_alert("AAPL", datetime.utcnow())
        
        # Second alert within cooldown should not trigger
        second_alert = checker.should_trigger_alert(features)
        assert second_alert is False
    
    def test_create_alert_from_features(self):
        """Test creating proper alert from company features."""
        config = AlertConfig(
            company_thresholds={"TSLA": 0.6},
            default_threshold=0.8,
            cooldown_minutes=30
        )
        checker = AlertThresholdChecker(config)
        
        features = CompanyFeatures(
            ticker="TSLA",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=5,
            pos_news_count_5m=0,
            sentiment_score_5m=-0.8,
            risk_score_5m=0.95
        )
        
        alert = checker.create_alert(features)
        
        assert alert.ticker == "TSLA"
        assert alert.risk_score == 0.95
        assert alert.threshold == 0.6
        assert alert.severity == AlertSeverity.HIGH
        assert "TSLA" in alert.message
        assert "0.95" in alert.message


class TestSlackNotificationService:
    """Test the Slack notification service."""
    
    @pytest.fixture
    def mock_slack_client(self):
        """Mock Slack client for testing."""
        return Mock()
    
    def test_slack_service_initialization(self, mock_slack_client):
        """Test Slack service initializes with correct configuration."""
        service = SlackNotificationService(
            webhook_url="https://hooks.slack.com/test",
            channel="#supply-chain-alerts",
            username="RiskBot"
        )
        
        assert service.webhook_url == "https://hooks.slack.com/test"
        assert service.channel == "#supply-chain-alerts"
        assert service.username == "RiskBot"
    
    @pytest.mark.asyncio
    async def test_send_alert_success(self, mock_slack_client):
        """Test successful Slack alert sending."""
        service = SlackNotificationService(
            webhook_url="https://hooks.slack.com/test",
            channel="#supply-chain-alerts",
            username="RiskBot"
        )
        
        alert = RiskAlert(
            ticker="AAPL",
            risk_score=0.85,
            threshold=0.7,
            severity=AlertSeverity.HIGH,
            window_end=datetime.utcnow(),
            message="High risk detected"
        )
        
        # Mock successful HTTP response
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.text = AsyncMock(return_value="ok")
            mock_post.return_value.__aenter__.return_value = mock_response
            
            result = await service.send_alert(alert)
            
            assert result is True
            mock_post.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_send_alert_failure_retry(self, mock_slack_client):
        """Test Slack alert retry logic on failure."""
        service = SlackNotificationService(
            webhook_url="https://hooks.slack.com/test",
            channel="#supply-chain-alerts",
            username="RiskBot",
            max_retries=2
        )
        
        alert = RiskAlert(
            ticker="AAPL",
            risk_score=0.85,
            threshold=0.7,
            severity=AlertSeverity.HIGH,
            window_end=datetime.utcnow(),
            message="High risk detected"
        )
        
        # Mock failed HTTP response
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_response = AsyncMock()
            mock_response.status = 500
            mock_response.text = AsyncMock(return_value="Internal Server Error")
            mock_post.return_value.__aenter__.return_value = mock_response
            
            result = await service.send_alert(alert)
            
            assert result is False
            assert mock_post.call_count == 2  # Should retry once
    
    def test_format_high_severity_alert(self):
        """Test high severity alert formatting includes proper urgency indicators."""
        service = SlackNotificationService("test", "#test", "bot")
        
        alert = RiskAlert(
            ticker="AAPL",
            risk_score=0.95,
            threshold=0.7,
            severity=AlertSeverity.HIGH,
            window_end=datetime(2025, 11, 15, 23, 45, 0),
            message="Critical supply chain disruption"
        )
        
        message = service._format_alert_message(alert)
        
        # Should include high severity indicators
        message_str = str(message)
        assert "🔴" in message_str
        assert "CRITICAL" in message_str
        assert "AAPL" in message_str
        assert "0.95" in message_str


class TestAlertManager:
    """Test the overall alert management system."""
    
    @pytest.fixture
    def mock_notification_service(self):
        """Mock notification service for testing."""
        service = Mock()
        service.send_alert = AsyncMock(return_value=True)
        return service
    
    @pytest.fixture
    def alert_config(self):
        """Standard alert configuration for testing."""
        return AlertConfig(
            company_thresholds={
                "AAPL": 0.7,
                "TSLA": 0.6,
                "MSFT": 0.75
            },
            default_threshold=0.8,
            cooldown_minutes=30
        )
    
    def test_alert_manager_initialization(self, alert_config, mock_notification_service):
        """Test alert manager initializes correctly."""
        manager = AlertManager(
            threshold_checker=AlertThresholdChecker(alert_config),
            notification_service=mock_notification_service
        )
        
        assert manager.threshold_checker is not None
        assert manager.notification_service is not None
    
    @pytest.mark.asyncio
    async def test_process_features_triggers_alert(self, alert_config, mock_notification_service):
        """Test processing features that should trigger an alert."""
        manager = AlertManager(
            threshold_checker=AlertThresholdChecker(alert_config),
            notification_service=mock_notification_service
        )
        
        high_risk_features = CompanyFeatures(
            ticker="AAPL",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=5,
            pos_news_count_5m=0,
            sentiment_score_5m=-0.8,
            risk_score_5m=0.9  # Above AAPL threshold of 0.7
        )
        
        result = await manager.process_features(high_risk_features)
        
        assert result is True  # Alert was sent
        mock_notification_service.send_alert.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_process_features_no_alert_below_threshold(self, alert_config, mock_notification_service):
        """Test processing features that should not trigger an alert."""
        manager = AlertManager(
            threshold_checker=AlertThresholdChecker(alert_config),
            notification_service=mock_notification_service
        )
        
        low_risk_features = CompanyFeatures(
            ticker="AAPL",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=1,
            pos_news_count_5m=4,
            sentiment_score_5m=0.3,
            risk_score_5m=0.4  # Below AAPL threshold of 0.7
        )
        
        result = await manager.process_features(low_risk_features)
        
        assert result is False  # No alert sent
        mock_notification_service.send_alert.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_multiple_companies_different_thresholds(self, alert_config, mock_notification_service):
        """Test multiple companies with different threshold configurations."""
        manager = AlertManager(
            threshold_checker=AlertThresholdChecker(alert_config),
            notification_service=mock_notification_service
        )
        
        # TSLA has lower threshold (0.6), should alert
        tsla_features = CompanyFeatures(
            ticker="TSLA",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=3,
            pos_news_count_5m=1,
            sentiment_score_5m=-0.4,
            risk_score_5m=0.65  # Above TSLA threshold of 0.6
        )
        
        # MSFT has higher threshold (0.75), should not alert
        msft_features = CompanyFeatures(
            ticker="MSFT",
            window_end="2025-11-15T23:45:00Z",
            neg_news_count_5m=2,
            pos_news_count_5m=2,
            sentiment_score_5m=-0.2,
            risk_score_5m=0.65  # Below MSFT threshold of 0.75
        )
        
        tsla_result = await manager.process_features(tsla_features)
        msft_result = await manager.process_features(msft_features)
        
        assert tsla_result is True   # TSLA should alert
        assert msft_result is False  # MSFT should not alert
        
        # Only one alert should be sent (for TSLA)
        assert mock_notification_service.send_alert.call_count == 1


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])