package cl.duoc.gymflow.kafkaadmin.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

/**
 * Base de las pruebas: 3 brokers embebidos (sin Docker) configurados como el clúster real
 * ({@code auto.create.topics.enable=false}) y con los dos tópicos protegidos ya creados. Las clases que extienden esta
 * base comparten el mismo contexto de Spring y los mismos brokers; cada prueba usa nombres propios (UUID).
 */
@EmbeddedKafka(count = 3, partitions = 3, topics = {"reservations.events", "audit.timeline"},
        brokerProperties = {"auto.create.topics.enable=false"})
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "logging.level.kafka=WARN", "logging.level.org.apache.kafka=WARN", "logging.level.org.apache.zookeeper=WARN",
        "logging.level.state.change.logger=WARN"
})
@AutoConfigureMockMvc
public abstract class PruebaKafkaBase {

    @Autowired
    protected MockMvc mvc;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    protected EmbeddedKafkaBroker broker;

    protected static String nombre(String prefijo) {
        return prefijo + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    protected ResultActions get(String ruta) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.get(ruta));
    }

    protected ResultActions delete(String ruta) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.delete(ruta));
    }

    protected ResultActions post(String ruta, Object cuerpo) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.post(ruta).contentType(MediaType.APPLICATION_JSON)
                .content(cuerpo instanceof String s ? s : objectMapper.writeValueAsString(cuerpo)));
    }

    protected ResultActions put(String ruta, Object cuerpo) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.put(ruta).contentType(MediaType.APPLICATION_JSON)
                .content(cuerpo instanceof String s ? s : objectMapper.writeValueAsString(cuerpo)));
    }

    protected JsonNode json(ResultActions resultado) throws Exception {
        return objectMapper.readTree(resultado.andReturn().getResponse().getContentAsString());
    }

    protected static Map<String, Object> topico(String nombre, int particiones, int replicas, Map<String, String> configs) {
        return Map.of("name", nombre, "partitions", particiones, "replicationFactor", replicas, "configs", configs);
    }
}
