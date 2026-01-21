"""
PostgreSQL Alert Logger - Minimal async logging for alert history.

Logs all alert events (sent, suppressed, failed) to PostgreSQL for audit trail.
Uses fire-and-forget async pattern to avoid blocking alert processing.
"""
import asyncio
import asyncpg
import logging
from typing import Optional
from datetime import datetime
from alerting import RiskAlert

logger = logging.getLogger(__name__)


class PostgresAlertLogger:
    """Minimal async PostgreSQL logger for alert history."""
    
    def __init__(self, db_url: str, pool_size: int = 5):
        """
        Initialize PostgreSQL alert logger.
        
        Args:
            db_url: PostgreSQL connection URL (postgresql://user:pass@host:port/db)
            pool_size: Connection pool size (default: 5)
        """
        self.db_url = db_url
        self.pool_size = pool_size
        self.pool: Optional[asyncpg.Pool] = None
        self._pending_writes = 0
        self._writes_total = 0
        self._writes_failed = 0
        
    async def initialize(self):
        """Initialize connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                self.db_url,
                min_size=2,
                max_size=self.pool_size,
                command_timeout=2.0,  # 2 second timeout per query
                max_inactive_connection_lifetime=300.0  # 5 min
            )
            logger.info(f"PostgreSQL pool initialized: {self.pool_size} connections")
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL pool: {e}")
            self.pool = None
    
    async def log_alert(
        self,
        alert: RiskAlert,
        notification_sent: bool,
        suppressed: bool = False,
        suppression_reason: Optional[str] = None,
        notification_error: Optional[str] = None
    ):
        """
        Log alert to PostgreSQL (fire-and-forget, non-blocking).
        
        Args:
            alert: RiskAlert object containing alert details
            notification_sent: Whether Slack notification was sent successfully
            suppressed: Whether alert was suppressed (not sent)
            suppression_reason: Reason for suppression (cooldown, threshold_not_met)
            notification_error: Error message if notification failed
        """
        if not self.pool:
            logger.warning("PostgreSQL pool not initialized, skipping alert log")
            return
        
        # Fire-and-forget: create task but don't await it
        # This returns immediately without waiting for DB write
        asyncio.create_task(
            self._write_alert_async(
                alert, notification_sent, suppressed, 
                suppression_reason, notification_error
            )
        )
        
    async def _write_alert_async(
        self,
        alert: RiskAlert,
        notification_sent: bool,
        suppressed: bool,
        suppression_reason: Optional[str],
        notification_error: Optional[str]
    ):
        """Internal async write method (runs in background)."""
        self._pending_writes += 1
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO risk_alerts (
                        ticker, risk_score, threshold, severity,
                        window_end, triggered_at,
                        notification_sent, notification_sent_at, notification_error,
                        suppressed, suppression_reason,
                        alert_message
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    """,
                    alert.ticker,
                    float(alert.risk_score),
                    float(alert.threshold),
                    alert.severity.value,
                    alert.window_end,
                    alert.triggered_at,
                    notification_sent,
                    alert.triggered_at if notification_sent else None,
                    notification_error,
                    suppressed,
                    suppression_reason,
                    alert.message
                )
            
            self._writes_total += 1
            logger.debug(f"Alert logged to PostgreSQL: {alert.ticker} @ {alert.triggered_at}")
            
        except asyncio.TimeoutError:
            self._writes_failed += 1
            logger.warning(f"PostgreSQL write timeout for alert {alert.ticker}")
        except Exception as e:
            self._writes_failed += 1
            logger.error(f"Failed to log alert to PostgreSQL: {e}")
        finally:
            self._pending_writes -= 1
    
    async def close(self):
        """Close connection pool gracefully, waiting for pending writes."""
        if self.pool:
            # Wait for pending writes (with timeout)
            try:
                await asyncio.wait_for(
                    self._wait_for_pending_writes(),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"Timeout waiting for {self._pending_writes} pending PostgreSQL writes"
                )
            
            await self.pool.close()
            logger.info(
                f"PostgreSQL pool closed. Stats: {self._writes_total} total, "
                f"{self._writes_failed} failed"
            )
    
    async def _wait_for_pending_writes(self):
        """Wait for all pending writes to complete."""
        while self._pending_writes > 0:
            await asyncio.sleep(0.1)
    
    def get_stats(self) -> dict:
        """Get logger statistics."""
        if not self.pool:
            return {"status": "not_initialized"}
        return {
            "pool_size": self.pool.get_size(),
            "pool_free": self.pool.get_idle_size(),
            "pending_writes": self._pending_writes,
            "writes_total": self._writes_total,
            "writes_failed": self._writes_failed
        }
