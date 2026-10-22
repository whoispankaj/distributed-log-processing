package com.system.logprocessor.consumer.config;

import java.sql.SQLException;

import org.hibernate.exception.JDBCConnectionException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

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
    public DefaultErrorHandler kafkaConsumerErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate,
            @Value("${log.events.partitions}") int deadLetterPartitions) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(
                        record.topic() + DEAD_LETTER_SUFFIX,
                        deadLetterPartition(record.partition(), deadLetterPartitions)));

        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(10_000L);
        backOff.setMaxElapsedTime(30_000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.setBackOffFunction((record, exception) -> isTransient(exception)
                ? new FixedBackOff(5_000L, FixedBackOff.UNLIMITED_ATTEMPTS)
                : null);
        errorHandler.addNotRetryableExceptions(
                JsonProcessingException.class,
                InvalidLogEventException.class,
                DataIntegrityViolationException.class);
        return errorHandler;
    }

    static int deadLetterPartition(int sourcePartition, int deadLetterPartitions) {
        return sourcePartition < deadLetterPartitions ? sourcePartition : -1;
    }

    private static boolean isTransient(Throwable exception) {
        Throwable current = exception;
        while (current != null) {
            if (current instanceof TransientDataAccessException
                    || current instanceof DataAccessResourceFailureException
                    || current instanceof CannotCreateTransactionException
                    || current instanceof JDBCConnectionException
                    || isConnectionFailure(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean isConnectionFailure(Throwable exception) {
        if (!(exception instanceof SQLException sql)) {
            return false;
        }
        String state = sql.getSQLState();
        return state != null && (state.startsWith("08") || "57P01".equals(state));
    }
}
