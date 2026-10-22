package com.system.logprocessor.consumer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.system.logprocessor.consumer.model.LogEvent;
import com.system.logprocessor.consumer.repository.LogEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;

@Service
public class LogEventConsumer {
    @Autowired
    private LogEventRepository logEventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "log-events", groupId = "log-consumer-group")
    public void consumeLogEvent(String message, Acknowledgment acknowledgment) {
        try {
            LogEvent logEvent = objectMapper.readValue(message, LogEvent.class);
            logEvent.setProcessedAt(LocalDateTime.now());
            logEventRepository.save(logEvent);
            acknowledgment.acknowledge();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}