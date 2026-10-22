package com.system.logprocessor.producer.model;

import java.time.LocalDateTime;
import java.util.UUID;

public class LogEvent {
    private String id;
    private String organizationId;
    private String level;
    private String message;
    private String source;
    private LocalDateTime timestamp;

    public LogEvent() {
        this.timestamp = LocalDateTime.now();
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
    
    public LocalDateTime getTimestamp() {
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

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
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