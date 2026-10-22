CREATE TABLE log_events (
    id VARCHAR(255) NOT NULL,
    organization_id VARCHAR(255) NOT NULL,
    level VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    source VARCHAR(255),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (organization_id, id)
);

CREATE INDEX idx_log_events_organization_timestamp
    ON log_events (organization_id, timestamp);
