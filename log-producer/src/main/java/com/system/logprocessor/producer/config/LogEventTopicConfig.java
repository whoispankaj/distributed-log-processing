package com.system.logprocessor.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class LogEventTopicConfig {

    @Bean
    public NewTopic logEventsTopic(
            @Value("${log.events.topic}") String name,
            @Value("${log.events.partitions}") int partitions,
            @Value("${log.events.replicas}") short replicas) {
        return TopicBuilder.name(name).partitions(partitions).replicas(replicas).build();
    }
}
