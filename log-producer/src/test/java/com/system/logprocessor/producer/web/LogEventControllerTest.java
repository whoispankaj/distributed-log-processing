package com.system.logprocessor.producer.web;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
        partitions = 1,
        topics = "log-events",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class LogEventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void rejectsAnOrganizationIdLongerThanTheColumn() throws Exception {
        String body = """
                {"organizationId":"%s","level":"INFO","message":"hello"}
                """.formatted("x".repeat(LogEventController.MAX_LENGTH + 1));

        mockMvc.perform(post("/logs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsAnUnknownLevel() throws Exception {
        mockMvc.perform(post("/logs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"organizationId":"org-1","level":"NOPE","message":"hello"}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsAMissingMessage() throws Exception {
        mockMvc.perform(post("/logs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"organizationId":"org-1","level":"INFO"}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void acceptsAValidLogEvent() throws Exception {
        mockMvc.perform(post("/logs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"organizationId":"org-1","level":"INFO","message":"hello","source":"test"}
                                """))
                .andExpect(status().isAccepted());
    }

    @Test
    void acceptsALowercaseLevel() throws Exception {
        mockMvc.perform(post("/logs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"organizationId":"org-1","level":"info","message":"hello"}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.level").value("INFO"));
    }

    @Test
    void usesAClientSuppliedId() throws Exception {
        mockMvc.perform(post("/logs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id":"11111111-1111-4111-8111-111111111111","organizationId":"org-1","level":"INFO","message":"hello"}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").value("11111111-1111-4111-8111-111111111111"));
    }

    @Test
    void rejectsAClientIdThatIsNotAUuid() throws Exception {
        mockMvc.perform(post("/logs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id":"evt-1","organizationId":"org-1","level":"INFO","message":"hello"}
                                """))
                .andExpect(status().isBadRequest());
    }
}
