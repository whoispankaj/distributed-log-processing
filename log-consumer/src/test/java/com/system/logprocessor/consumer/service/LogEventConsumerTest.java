package com.system.logprocessor.consumer.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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
import org.testcontainers.DockerClientFactory;
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
        partitions = 3,
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
        registry.add("spring.datasource.hikari.connection-timeout", () -> "1000");
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
    void storesAValidEventAfterPostgresRecovers() throws Exception {
        String id = "evt-" + UUID.randomUUID();
        String payload = """
                {"id":"%s","organizationId":"org-1","level":"INFO","message":"hello","source":"test","timestamp":"2026-10-22T04:00:00Z"}
                """.formatted(id).strip();

        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(POSTGRES.getContainerId()).exec();
        try {
            kafkaTemplate.send(topic, "org-1", payload).get(10, TimeUnit.SECONDS);
            Thread.sleep(Duration.ofSeconds(40));
            assertThat(deadLetterValues()).doesNotContain(payload);

            docker.unpauseContainerCmd(POSTGRES.getContainerId()).exec();
            await().atMost(Duration.ofSeconds(40)).ignoreExceptions().until(
                    () -> repository.findById(id).isPresent());
            assertThat(repository.findById(id).orElseThrow().getMessage()).isEqualTo("hello");
            assertThat(deadLetterValues()).doesNotContain(payload);
        } finally {
            try {
                docker.unpauseContainerCmd(POSTGRES.getContainerId()).exec();
            } catch (RuntimeException ignored) {
                // The container is already running.
            }
        }
    }

    @Test
    void sendsALogEventWithoutAMessageToTheDeadLetterTopic() throws Exception {
        String id = "evt-" + UUID.randomUUID();
        String payload = """
                {"id":"%s","organizationId":"org-1","level":"INFO","timestamp":"2026-10-22T04:00:00Z"}
                """.formatted(id).strip();

        kafkaTemplate.send(topic, "org-1", payload).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(deadLetterValues()).contains(payload);
            assertThat(repository.findById(id)).isEmpty();
        });
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
    void sendsAnOverlongOrganizationIdToTheDeadLetterTopic() throws Exception {
        String id = "evt-" + UUID.randomUUID();
        String payload = """
                {"id":"%s","organizationId":"%s","level":"INFO","message":"hello","timestamp":"2026-10-22T04:00:00Z"}
                """.formatted(id, "x".repeat(256)).strip();

        kafkaTemplate.send(topic, "too-long", payload).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(deadLetterValues()).contains(payload);
            assertThat(repository.findById(id)).isEmpty();
        });
    }

    @Test
    void sendsAMalformedLogEventToTheDeadLetterTopic() throws Exception {
        String payload = "not-json-" + UUID.randomUUID();
        int partition = 2;

        kafkaTemplate.send(new ProducerRecord<>(topic, partition, "bad", payload)).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(deadLetterRecords())
                    .filteredOn(record -> payload.equals(record.value()))
                    .anySatisfy(record -> assertThat(record.partition()).isEqualTo(partition));
            assertThat(repository.count()).isZero();
        });
    }

    private List<String> deadLetterValues() {
        return deadLetterRecords().stream().map(ConsumerRecord::value).toList();
    }

    private List<ConsumerRecord<String, String>> deadLetterRecords() {
        String deadLetterTopic = topic + ".DLT";
        Map<String, Object> properties = KafkaTestUtils.consumerProps("dlt-reader-" + UUID.randomUUID(), "false", broker);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(deadLetterTopic);
            if (partitionInfos == null) {
                return List.of();
            }
            List<TopicPartition> partitions = partitionInfos.stream()
                    .map(info -> new TopicPartition(deadLetterTopic, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            List<ConsumerRecord<String, String>> values = new ArrayList<>();
            records.records(deadLetterTopic).forEach(values::add);
            return values;
        }
    }
}
