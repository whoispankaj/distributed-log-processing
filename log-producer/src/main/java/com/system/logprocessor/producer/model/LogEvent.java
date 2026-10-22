package com.system.logprocessor.producer.model;

import java.time.Instant;
import java.util.UUID;

public class LogEvent {
    private String id;
    private String organizationId;
    private String level;
    private String message;
    private String source;
    private Instant timestamp;

    public LogEvent() {
        this.id = UUID.randomUUID().toString();
    }
    
    public String getId() {
        return id;
    }

    public String getOrganizationId() {
        return organizationId;
    }

    public String getLevel() {
        return level;
    }

    public String getMessage() {
        return message;
    }

    public String getSource() {
        return source;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }

    public void setId(String id) {
        this.id = id;
    }
    
    public void setOrganizationId(String organizationId) {
        this.organizationId = organizationId;
    }

    public void setLevel(String level) {
        this.level = level;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public void assignTimestampIfMissing() {
        if (this.timestamp == null) {
            this.timestamp = Instant.now();
        }
    }

    @Override
    public String toString() {
        return "LogEvent{" +
                "id='" + id + '\'' +
                ", organizationId='" + organizationId + '\'' +
                ", level='" + level + '\'' +
                ", message='" + message + '\'' +
                ", source='" + source + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}