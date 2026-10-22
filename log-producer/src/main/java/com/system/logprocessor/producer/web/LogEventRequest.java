package com.system.logprocessor.producer.web;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;

public record LogEventRequest(
        @JsonProperty("id") String id,
        @JsonProperty("organizationId") String organizationId,
        @JsonProperty("level") String level,
        @JsonProperty("message") String message,
        @JsonProperty("source") String source,
        @JsonProperty("timestamp") Instant timestamp) {
}
