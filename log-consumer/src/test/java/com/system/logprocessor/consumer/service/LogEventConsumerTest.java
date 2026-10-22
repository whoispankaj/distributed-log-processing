package com.system.logprocessor.consumer.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.system.logprocessor.consumer.model.LogEvent;
import com.system.logprocessor.consumer.repository.LogEventRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"log-events", "log-events.DLT"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@Testcontainers
class LogEventConsumerTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:15-alpine");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("POSTGRES_USER", POSTGRES::getUsername);
        registry.add("POSTGRES_PASSWORD", POSTGRES::getPassword);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private LogEventRepository repository;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Value("${log.events.topic}")
    private String topic;

    @BeforeEach
    void cleanDatabase() {
        repository.deleteAll();
    }

    @Test
    void savesAValidLogEvent() throws Exception {
        String id = "evt-" + UUID.randomUUID();
        String payload = """
                {"id":"%s","organizationId":"org-1","level":"INFO","message":"hello","source":"test","timestamp":"2026-10-22T04:00:00Z"}
                """.formatted(id);

        kafkaTemplate.send(topic, "org-1", payload).get(10, TimeUnit.SECONDS);

        LogEvent saved = await().atMost(Duration.ofSeconds(20))
                .until(() -> repository.findById(id).orElse(null), event -> event != null);
        assertThat(saved.getOrganizationId()).isEqualTo("org-1");
        assertThat(saved.getLevel()).isEqualTo("INFO");
        assertThat(saved.getMessage()).isEqualTo("hello");
        assertThat(saved.getProcessedAt()).isNotNull();
    }

    @Test
    void sendsANullLogEventToTheDeadLetterTopic() throws Exception {
        kafkaTemplate.send(topic, "null-event", "null").get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(deadLetterValues()).contains("null");
            assertThat(repository.count()).isZero();
        });
    }

    @Test
    void sendsAMalformedLogEventToTheDeadLetterTopic() throws Exception {
        String payload = "not-json-" + UUID.randomUUID();

        kafkaTemplate.send(topic, "bad", payload).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(deadLetterValues()).contains(payload);
            assertThat(repository.count()).isZero();
        });
    }

    private List<String> deadLetterValues() {
        Map<String, Object> properties = KafkaTestUtils.consumerProps("dlt-reader-" + UUID.randomUUID(), "true", broker);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic + ".DLT"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            List<String> values = new ArrayList<>();
            records.records(topic + ".DLT").forEach(record -> values.add(record.value()));
            return values;
        }
    }
}
