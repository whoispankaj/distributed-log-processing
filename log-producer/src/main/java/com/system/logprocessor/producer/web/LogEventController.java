package com.system.logprocessor.producer.web;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.system.logprocessor.producer.model.LogEvent;

@RestController
@RequestMapping("/logs")
public class LogEventController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public LogEventController(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${log.events.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    @PostMapping
    public ResponseEntity<LogEvent> publish(@RequestBody LogEventRequest request) {
        validate(request);
        LogEvent event = toEvent(request);
        publish(event);
        return ResponseEntity.accepted().body(event);
    }

    private void validate(LogEventRequest request) {
        if (isBlank(request.organizationId()) || isBlank(request.level()) || isBlank(request.message())) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "organizationId, level, and message are required");
        }
    }

    private LogEvent toEvent(LogEventRequest request) {
        LogEvent event = new LogEvent();
        event.setOrganizationId(request.organizationId());
        event.setLevel(request.level());
        event.setMessage(request.message());
        event.setSource(request.source());
        event.setTimestamp(request.timestamp());
        event.assignTimestampIfMissing();
        return event;
    }

    private void publish(LogEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(topic, event.getOrganizationId(), payload).get(10, TimeUnit.SECONDS);
        } catch (JsonProcessingException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Log event could not be serialized", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Interrupted while publishing log event", exception);
        } catch (TimeoutException | ExecutionException exception) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Could not publish log event", exception);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
