package cl.duoc.gymflow.kafkaadmin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Kafka caído (nadie escucha en localhost:1): el servicio arranca igual, el health sigue UP y cada llamada responde
 * 503 con el formato común en el tiempo configurado (no queda colgada).
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=localhost:1",
        "gymflow.kafka-admin.timeout=2s",
        "logging.level.org.apache.kafka=ERROR"
})
@AutoConfigureMockMvc
class KafkaCaidoTest {

    @Autowired
    private MockMvc mvc;

    @Test
    void conKafkaCaido_responde503Rapido() throws Exception {
        long inicio = System.currentTimeMillis();
        mvc.perform(get("/api/kafka/topics"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503))
                .andExpect(jsonPath("$.message").value(containsString("Kafka no responde")))
                .andExpect(jsonPath("$.path").value("/api/kafka/topics"));
        assertThat(System.currentTimeMillis() - inicio).isLessThan(5000);
    }

    @Test
    void conKafkaCaido_elHealthSigueUp() throws Exception {
        mvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }
}
