package com.system.logprocessor.producer.web;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration",
        "spring.kafka.producer.properties.max.request.size=100"
})
@AutoConfigureMockMvc
@EmbeddedKafka(
        partitions = 1,
        topics = "log-events",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class LogEventTooLargeTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void rejectsALogEventLargerThanKafkaAllows() throws Exception {
        mockMvc.perform(post("/logs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"organizationId":"org-1","level":"INFO","message":"hello"}
                                """))
                .andExpect(status().isPayloadTooLarge());
    }
}
