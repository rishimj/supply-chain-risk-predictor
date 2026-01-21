-- Supply Chain Risk Predictor - PostgreSQL Schema
-- Minimal alert history table

-- Create risk_alerts table
CREATE TABLE IF NOT EXISTS risk_alerts (
    id BIGSERIAL PRIMARY KEY,
    
    -- Alert identification
    ticker VARCHAR(20) NOT NULL,
    risk_score DECIMAL(6,3) NOT NULL,
    threshold DECIMAL(6,3) NOT NULL,
    severity VARCHAR(20) NOT NULL, -- low, medium, high
    
    -- Timestamps
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    triggered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Notification tracking
    notification_sent BOOLEAN DEFAULT FALSE,
    notification_sent_at TIMESTAMP WITH TIME ZONE,
    notification_error TEXT,
    
    -- Suppression tracking
    suppressed BOOLEAN DEFAULT FALSE,
    suppression_reason VARCHAR(100), -- cooldown, threshold_not_met
    
    -- Alert message
    alert_message TEXT,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX idx_ticker_triggered ON risk_alerts(ticker, triggered_at DESC);
CREATE INDEX idx_triggered_at ON risk_alerts(triggered_at DESC);
CREATE INDEX idx_notification_sent ON risk_alerts(notification_sent, triggered_at DESC);
CREATE INDEX idx_suppressed ON risk_alerts(suppressed, triggered_at DESC);

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE risk_alerts TO supply_chain_user;
GRANT USAGE, SELECT ON SEQUENCE risk_alerts_id_seq TO supply_chain_user;

-- Add comment
COMMENT ON TABLE risk_alerts IS 'Historical log of all supply chain risk alerts (sent and suppressed)';
