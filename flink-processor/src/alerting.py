"""
Supply Chain Risk Alerting System

Implements threshold-based alerting for supply chain risk scores with Slack notifications.
Built using Test-Driven Development (TDD) approach.
"""
import asyncio
import aiohttp
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Optional, List
from models import CompanyFeatures

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels based on risk score thresholds."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass
class AlertConfig:
    """Configuration for alert thresholds and behavior."""
    company_thresholds: Dict[str, float] = field(default_factory=dict)
    default_threshold: float = 0.8
    cooldown_minutes: int = 30
    
    def get_threshold(self, ticker: str) -> float:
        """Get threshold for specific company or default."""
        return self.company_thresholds.get(ticker, self.default_threshold)


@dataclass
class RiskAlert:
    """Represents a supply chain risk alert."""
    ticker: str
    risk_score: float
    threshold: float
    severity: AlertSeverity
    window_end: datetime
    message: str
    triggered_at: datetime = field(default_factory=datetime.utcnow)
    
    @classmethod
    def from_risk_score(cls, ticker: str, risk_score: float, threshold: float, 
                       window_end: Optional[datetime] = None) -> 'RiskAlert':
        """Create alert from risk score with automatic severity classification."""
        if window_end is None:
            window_end = datetime.utcnow()
            
        # Classify severity based on risk score
        if risk_score >= 0.8:
            severity = AlertSeverity.HIGH
        elif risk_score >= 0.6:
            severity = AlertSeverity.MEDIUM
        else:
            severity = AlertSeverity.LOW
            
        # Generate appropriate message
        if severity == AlertSeverity.HIGH:
            message = f"CRITICAL: High supply chain risk detected for {ticker} (risk: {risk_score:.3f})"
        elif severity == AlertSeverity.MEDIUM:
            message = f"WARNING: Medium supply chain risk for {ticker} (risk: {risk_score:.3f})"
        else:
            message = f"NOTICE: Supply chain risk alert for {ticker} (risk: {risk_score:.3f})"
            
        return cls(
            ticker=ticker,
            risk_score=risk_score,
            threshold=threshold,
            severity=severity,
            window_end=window_end,
            message=message
        )
    
    def to_slack_message(self) -> Dict:
        """Convert alert to Slack message format."""
        # Emoji and color based on severity
        emoji_map = {
            AlertSeverity.HIGH: "🔴",
            AlertSeverity.MEDIUM: "🟡", 
            AlertSeverity.LOW: "🟢"
        }
        
        color_map = {
            AlertSeverity.HIGH: "#ff0000",
            AlertSeverity.MEDIUM: "#ffaa00",
            AlertSeverity.LOW: "#00aa00"
        }
        
        emoji = emoji_map.get(self.severity, "⚠️")
        color = color_map.get(self.severity, "#999999")
        
        # Map severity to readable names
        severity_names = {
            AlertSeverity.HIGH: "CRITICAL",
            AlertSeverity.MEDIUM: "WARNING",
            AlertSeverity.LOW: "NOTICE"
        }
        severity_name = severity_names.get(self.severity, self.severity.value.upper())
        
        # Format timestamp
        time_str = self.window_end.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        return {
            "text": f"{emoji} Supply Chain Risk Alert",
            "attachments": [
                {
                    "color": color,
                    "title": f"{severity_name} Risk Alert: {self.ticker}",
                    "text": self.message,
                    "fields": [
                        {
                            "title": "Risk Score",
                            "value": f"{self.risk_score:.3f}",
                            "short": True
                        },
                        {
                            "title": "Threshold",
                            "value": f"{self.threshold:.3f}",
                            "short": True
                        },
                        {
                            "title": "Window End",
                            "value": time_str,
                            "short": False
                        }
                    ],
                    "footer": "Supply Chain Risk Monitor",
                    "ts": int(self.triggered_at.timestamp())
                }
            ]
        }


class AlertThresholdChecker:
    """Checks if risk scores exceed configured thresholds and manages cooldowns via Redis."""
    
    def __init__(self, config: AlertConfig, redis_client=None):
        self.config = config
        self.redis_client = redis_client
    
    def should_trigger_alert(self, features: CompanyFeatures) -> bool:
        """Check if an alert should be triggered for the given features."""
        ticker = features.ticker
        risk_score = features.risk_score_5m
        threshold = self.config.get_threshold(ticker)
        
        # Check 1: Risk score exceeds threshold?
        if risk_score <= threshold:
            return False
            
        # Check 2: In cooldown? (check Redis)
        if self._is_in_cooldown_redis(ticker):
            return False
            
        return True
    
    def _is_in_cooldown_redis(self, ticker: str) -> bool:
        """Check Redis for cooldown key."""
        if not self.redis_client:
            logger.warning("No Redis client, skipping cooldown check")
            return False
        
        try:
            key = f"alert:last_sent:{ticker}"
            exists = self.redis_client.exists(key)
            
            if exists:
                # Log when alert was last sent
                last_sent = self.redis_client.get(key)
                logger.debug(f"{ticker} in cooldown, last alert: {last_sent}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Redis cooldown check failed for {ticker}: {e}")
            return False  # Fail safe: allow alert on Redis error
    
    def record_alert(self, ticker: str, alert_time: Optional[datetime] = None) -> None:
        """Record alert in Redis with TTL based on cooldown period."""
        if not self.redis_client:
            logger.warning("No Redis client, cannot record alert")
            return
        
        try:
            key = f"alert:last_sent:{ticker}"
            if alert_time is None:
                alert_time = datetime.utcnow()
            value = alert_time.isoformat() + "Z"
            ttl_seconds = self.config.cooldown_minutes * 60  # Convert minutes to seconds
            
            self.redis_client.setex(key, ttl_seconds, value)
            logger.info(f"Recorded alert for {ticker}, cooldown for {ttl_seconds}s")
            
        except Exception as e:
            logger.error(f"Failed to record alert in Redis for {ticker}: {e}")
    
    def create_alert(self, features: CompanyFeatures) -> RiskAlert:
        """Create a RiskAlert from CompanyFeatures."""
        threshold = self.config.get_threshold(features.ticker)
        window_end = datetime.fromisoformat(features.window_end.replace('Z', '+00:00'))
        
        return RiskAlert.from_risk_score(
            ticker=features.ticker,
            risk_score=features.risk_score_5m,
            threshold=threshold,
            window_end=window_end
        )


class SlackNotificationService:
    """Handles sending alerts to Slack via webhooks."""
    
    def __init__(self, webhook_url: str, channel: str, username: str = "RiskBot", 
                 max_retries: int = 3, timeout_seconds: int = 10):
        self.webhook_url = webhook_url
        self.channel = channel
        self.username = username
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
    
    async def send_alert(self, alert: RiskAlert) -> bool:
        """Send alert to Slack with retry logic."""
        message = self._format_alert_message(alert)
        
        for attempt in range(self.max_retries):
            try:
                success = await self._send_to_slack(message)
                if success:
                    logger.info(f"Slack alert sent successfully for {alert.ticker}")
                    return True
                else:
                    logger.warning(f"Slack alert failed (attempt {attempt + 1}/{self.max_retries}) for {alert.ticker}")
                    
            except Exception as e:
                logger.error(f"Slack alert error (attempt {attempt + 1}/{self.max_retries}) for {alert.ticker}: {e}")
                
            # Wait before retry (exponential backoff)
            if attempt < self.max_retries - 1:
                await asyncio.sleep(2 ** attempt)
        
        logger.error(f"Failed to send Slack alert for {alert.ticker} after {self.max_retries} attempts")
        return False
    
    async def _send_to_slack(self, message: Dict) -> bool:
        """Send message to Slack webhook."""
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout_seconds)) as session:
            async with session.post(self.webhook_url, json=message) as response:
                if response.status == 200:
                    response_text = await response.text()
                    return response_text.strip() == "ok"
                else:
                    logger.error(f"Slack webhook returned status {response.status}")
                    return False
    
    def _format_alert_message(self, alert: RiskAlert) -> Dict:
        """Format alert as Slack message."""
        slack_message = alert.to_slack_message()
        
        # Add channel and username
        slack_message["channel"] = self.channel
        slack_message["username"] = self.username
        
        return slack_message


class AlertManager:
    """Main alert management system that coordinates threshold checking and notifications."""
    
    def __init__(self, threshold_checker: AlertThresholdChecker, 
                 notification_service: SlackNotificationService,
                 postgres_logger=None):
        self.threshold_checker = threshold_checker
        self.notification_service = notification_service
        self.postgres_logger = postgres_logger
        
        # Statistics
        self._alerts_sent = 0
        self._alerts_suppressed = 0
        
    async def process_features(self, features: CompanyFeatures) -> bool:
        """Process company features and send alert if threshold is exceeded."""
        try:
            # Check if alert should be triggered
            should_trigger = self.threshold_checker.should_trigger_alert(features)
            
            # Always create alert object (for logging even if suppressed)
            alert = self.threshold_checker.create_alert(features)
            
            if not should_trigger:
                self._alerts_suppressed += 1
                
                # Log suppressed alert to PostgreSQL
                if self.postgres_logger:
                    # Determine suppression reason
                    in_cooldown = self.threshold_checker._is_in_cooldown_redis(features.ticker)
                    suppression_reason = "cooldown" if in_cooldown else "threshold_not_met"
                    
                    await self.postgres_logger.log_alert(
                        alert=alert,
                        notification_sent=False,
                        suppressed=True,
                        suppression_reason=suppression_reason
                    )
                
                return False
                
            # Create and send alert
            success = await self.notification_service.send_alert(alert)
            
            if success:
                # Record alert for cooldown tracking
                self.threshold_checker.record_alert(features.ticker)
                self._alerts_sent += 1
                
                logger.info(f"Alert sent for {features.ticker}: risk={features.risk_score_5m:.3f}")
            else:
                logger.error(f"Failed to send alert for {features.ticker}")
            
            # Log to PostgreSQL (sent or failed)
            if self.postgres_logger:
                await self.postgres_logger.log_alert(
                    alert=alert,
                    notification_sent=success,
                    suppressed=False,
                    notification_error=None if success else "Slack notification failed"
                )
            
            return success
                
        except Exception as e:
            logger.error(f"Error processing alert for {features.ticker}: {e}")
            
            # Try to log error to PostgreSQL
            if self.postgres_logger and 'alert' in locals():
                try:
                    await self.postgres_logger.log_alert(
                        alert=alert,
                        notification_sent=False,
                        suppressed=False,
                        notification_error=f"Exception: {str(e)}"
                    )
                except Exception as log_error:
                    logger.error(f"Failed to log error to PostgreSQL: {log_error}")
            
            return False
    
    def get_statistics(self) -> Dict:
        """Get alerting system statistics."""
        stats = {
            "alerts_sent": self._alerts_sent,
            "alerts_suppressed": self._alerts_suppressed
        }
        if self.postgres_logger:
            stats["postgres"] = self.postgres_logger.get_stats()
        return stats


# Factory function for easy setup
def create_alert_manager(webhook_url: str, channel: str = "#supply-chain-alerts",
                        company_thresholds: Optional[Dict[str, float]] = None,
                        default_threshold: float = 0.7,
                        cooldown_minutes: int = 30,
                        redis_client=None,
                        postgres_logger=None) -> AlertManager:
    """Create a fully configured alert manager with Redis-backed cooldown and PostgreSQL logging."""
    
    config = AlertConfig(
        company_thresholds=company_thresholds or {},
        default_threshold=default_threshold,
        cooldown_minutes=cooldown_minutes
    )
    
    threshold_checker = AlertThresholdChecker(config, redis_client=redis_client)
    notification_service = SlackNotificationService(webhook_url, channel)
    
    return AlertManager(threshold_checker, notification_service, postgres_logger=postgres_logger)