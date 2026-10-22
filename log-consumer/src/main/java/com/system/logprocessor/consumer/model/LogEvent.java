package com.system.logprocessor.consumer.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "log_events", indexes = {
        @Index(name = "idx_log_events_organization_timestamp", columnList = "organization_id, timestamp")
})
public class LogEvent {
    
    @Id
    private String id;
    
    @Column(name = "organization_id", nullable = false)
    private String organizationId;
    
    @Column(name = "level", nullable = false)
    private String level;
    
    @Column(name = "message", columnDefinition = "TEXT")
    private String message;
    
    @Column(name = "source")
    private String source;
    
    @Column(name = "timestamp", nullable = false, columnDefinition = "timestamptz")
    private Instant timestamp;
    
    @Column(name = "processed_at", columnDefinition = "timestamptz")
    private Instant processedAt;

    public LogEvent() {}

    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getOrganizationId() { return organizationId; }
    public void setOrganizationId(String organizationId) { this.organizationId = organizationId; }

    public String getLevel() { return level; }
    public void setLevel(String level) { this.level = level; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public Instant getProcessedAt() { return processedAt; }
    public void setProcessedAt(Instant processedAt) { this.processedAt = processedAt; }
}
