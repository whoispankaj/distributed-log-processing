CREATE TABLE log_events (
    id VARCHAR(255) PRIMARY KEY,
    organization_id VARCHAR(255) NOT NULL,
    level VARCHAR(255) NOT NULL,
    message TEXT,
    source VARCHAR(255),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_log_events_organization_timestamp
    ON log_events (organization_id, timestamp);
