package com.system.logprocessor.producer.web;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.errors.RecordTooLargeException;
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

    static final int MAX_LENGTH = 255;
    static final Set<String> LEVELS = Set.of("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL");

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
        List<String> problems = new ArrayList<>();
        checkRequired(problems, "organizationId", request.organizationId());
        checkLevel(problems, request.level());
        checkRequired(problems, "message", request.message());
        checkLength(problems, "source", request.source());
        if (!problems.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, String.join("; ", problems));
        }
    }

    private static void checkRequired(List<String> problems, String name, String value) {
        if (isBlank(value)) {
            problems.add(name + " is required");
        } else {
            checkLength(problems, name, value);
        }
    }

    private static void checkLevel(List<String> problems, String level) {
        if (isBlank(level)) {
            problems.add("level is required");
        } else if (!LEVELS.contains(level)) {
            problems.add("level must be one of TRACE, DEBUG, INFO, WARN, ERROR, FATAL");
        } else {
            checkLength(problems, "level", level);
        }
    }

    private static void checkLength(List<String> problems, String name, String value) {
        if (value != null && value.length() > MAX_LENGTH) {
            problems.add(name + " must be at most " + MAX_LENGTH + " characters");
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
        } catch (TimeoutException exception) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Could not publish log event", exception);
        } catch (ExecutionException exception) {
            throw publishFailure(exception);
        } catch (RuntimeException exception) {
            if (hasCause(exception, RecordTooLargeException.class)) {
                throw tooLarge(exception);
            }
            throw exception;
        }
    }

    private ResponseStatusException publishFailure(ExecutionException exception) {
        if (hasCause(exception, RecordTooLargeException.class)) {
            return tooLarge(exception);
        }
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Could not publish log event", exception);
    }

    private static ResponseStatusException tooLarge(Exception exception) {
        return new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, "Log event is too large to publish", exception);
    }

    private static boolean hasCause(Throwable exception, Class<? extends Throwable> type) {
        Throwable current = exception;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
