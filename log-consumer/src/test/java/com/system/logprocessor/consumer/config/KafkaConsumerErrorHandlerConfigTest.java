package com.system.logprocessor.consumer.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConsumerErrorHandlerConfigTest {

    @Test
    void keepsTheSourcePartitionWhenTheDeadLetterTopicHasIt() {
        assertThat(KafkaConsumerErrorHandlerConfig.deadLetterPartition(2, 3)).isEqualTo(2);
    }

    @Test
    void letsKafkaChooseWhenTheSourcePartitionIsMissingFromTheDeadLetterTopic() {
        assertThat(KafkaConsumerErrorHandlerConfig.deadLetterPartition(3, 3)).isEqualTo(-1);
    }
}
