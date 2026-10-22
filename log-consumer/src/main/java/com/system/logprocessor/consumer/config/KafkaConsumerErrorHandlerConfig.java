package com.system.logprocessor.consumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.system.logprocessor.consumer.service.InvalidLogEventException;

@Configuration
public class KafkaConsumerErrorHandlerConfig {

    static final String DEAD_LETTER_SUFFIX = ".DLT";

    @Bean
    public NewTopic logEventsDeadLetterTopic(
            @Value("${log.events.topic}") String topic,
            @Value("${log.events.partitions}") int partitions,
            @Value("${log.events.replicas}") short replicas) {
        return TopicBuilder.name(topic + DEAD_LETTER_SUFFIX)
                .partitions(partitions)
                .replicas(replicas)
                .build();
    }

    @Bean
    public DefaultErrorHandler kafkaConsumerErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + DEAD_LETTER_SUFFIX, record.partition()));

        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(10_000L);
        backOff.setMaxElapsedTime(30_000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(
                JsonProcessingException.class,
                InvalidLogEventException.class);
        return errorHandler;
    }
}
