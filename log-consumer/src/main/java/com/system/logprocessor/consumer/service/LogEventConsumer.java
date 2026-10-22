package com.system.logprocessor.consumer.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.system.logprocessor.consumer.model.LogEvent;
import com.system.logprocessor.consumer.repository.LogEventRepository;

import io.micrometer.core.instrument.MeterRegistry;

@Service
public class LogEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(LogEventConsumer.class);

    @Autowired
    private LogEventRepository logEventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MeterRegistry meterRegistry;

    @KafkaListener(topics = "log-events", groupId = "log-consumer-group")
    public void consumeLogEvent(String message, Acknowledgment acknowledgment) {
        LogEvent logEvent = readLogEvent(message);
        validate(logEvent);
        logEvent.setProcessedAt(Instant.now());
        int inserted = logEventRepository.insertIfAbsent(
                logEvent.getId(),
                logEvent.getOrganizationId(),
                logEvent.getLevel(),
                logEvent.getMessage(),
                logEvent.getSource(),
                logEvent.getTimestamp(),
                logEvent.getProcessedAt());
        if (inserted == 0) {
            log.info("Skipped already stored log event id={}", logEvent.getId());
        }
        acknowledgment.acknowledge();
    }

    private LogEvent readLogEvent(String message) {
        try {
            return objectMapper.readValue(message, LogEvent.class);
        } catch (JsonProcessingException exception) {
            throw rejected("malformed", "Log event payload is not valid JSON", exception);
        }
    }

    private void validate(LogEvent logEvent) {
        List<String> missing = new ArrayList<>();
        if (isBlank(logEvent.getId())) {
            missing.add("id");
        }
        if (isBlank(logEvent.getOrganizationId())) {
            missing.add("organizationId");
        }
        if (isBlank(logEvent.getLevel())) {
            missing.add("level");
        }
        if (logEvent.getTimestamp() == null) {
            missing.add("timestamp");
        }
        if (!missing.isEmpty()) {
            throw rejected("invalid", "Log event is missing required fields: " + missing, null);
        }
    }

    private InvalidLogEventException rejected(String reason, String detail, Exception cause) {
        meterRegistry.counter("log.events.rejected", "reason", reason).increment();
        if (cause == null) {
            log.warn("Rejected log event ({}): {}", reason, detail);
            return new InvalidLogEventException(detail);
        }
        log.warn("Rejected log event ({}): {}", reason, detail, cause);
        return new InvalidLogEventException(detail, cause);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
