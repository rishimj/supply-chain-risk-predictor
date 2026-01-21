#!/usr/bin/env python3
"""Debug the alert message format"""
import sys
import os
sys.path.append('flink-processor/src')

from alerting import RiskAlert, AlertSeverity, SlackNotificationService
from datetime import datetime

# Create test alert
alert = RiskAlert(
    ticker="AAPL",
    risk_score=0.95,
    threshold=0.7,
    severity=AlertSeverity.HIGH,
    window_end=datetime(2025, 11, 15, 23, 45, 0),
    message="Critical supply chain disruption"
)

service = SlackNotificationService("test", "#test", "bot")
message = service._format_alert_message(alert)

print("🔍 Debug: Slack message format:")
print(f"Message: {message}")
print(f"Text: {message['text']}")
print(f"Attachments: {message.get('attachments', [])}")

# Check what we're looking for
print(f"\n🔍 Looking for 'CRITICAL':")
print(f"In text: {'CRITICAL' in message['text']}")
print(f"In attachments: {str(message.get('attachments', []))}")
print(f"CRITICAL in attachments: {'CRITICAL' in str(message.get('attachments', []))}")