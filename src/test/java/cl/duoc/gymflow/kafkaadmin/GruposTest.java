package cl.duoc.gymflow.kafkaadmin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import cl.duoc.gymflow.kafkaadmin.support.PruebaKafkaBase;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/**
 * Consumer groups con un consumidor real: 3 mensajes en cada una de las 3 particiones; el grupo confirma 3 en la
 * partición 0, 1 en la 1 y nada en la 2 → lag 0 + 2 (la 2 no cuenta: nunca confirmó).
 */
class GruposTest extends PruebaKafkaBase {

    @Test
    void grupoActivo_muestraMiembrosYLag_yNoSePuedeResetearNiBorrar() throws Exception {
        String topico = crearTopicoConMensajes();
        String grupo = nombre("grupo");
        try (KafkaConsumer<String, String> consumidor = consumidorQueConfirma(grupo, topico)) {
            JsonNode detalle = json(get("/api/kafka/consumer-groups/" + grupo).andExpect(status().isOk()));
            assertThat(detalle.get("state").asText()).isEqualTo("STABLE");
            assertThat(detalle.get("members")).hasSize(1);
            assertThat(detalle.get("members").get(0).get("assignments")).hasSize(3);
            assertThat(detalle.get("lag").asLong()).isEqualTo(2);
            JsonNode p0 = offset(detalle, 0);
            JsonNode p1 = offset(detalle, 1);
            JsonNode p2 = offset(detalle, 2);
            assertThat(p0.get("committedOffset").asLong()).isEqualTo(3);
            assertThat(p0.get("lag").asLong()).isZero();
            assertThat(p1.get("committedOffset").asLong()).isEqualTo(1);
            assertThat(p1.get("endOffset").asLong()).isEqualTo(3);
            assertThat(p1.get("lag").asLong()).isEqualTo(2);
            assertThat(p2.get("committedOffset").isNull()).isTrue();
            assertThat(p2.get("memberId").asText()).isNotBlank();

            JsonNode fila = buscar(json(get("/api/kafka/consumer-groups").andExpect(status().isOk())), grupo);
            assertThat(fila.get("members").asInt()).isEqualTo(1);
            assertThat(fila.get("lag").asLong()).isEqualTo(2);
            assertThat(fila.get("topics").get(0).asText()).isEqualTo(topico);

            post("/api/kafka/consumer-groups/" + grupo + "/reset-offsets", Map.of("strategy", "earliest"))
                    .andExpect(status().isConflict())
                    .andExpect(jsonPath("$.message").value(containsString("consumidor(es) activo(s)")));
            delete("/api/kafka/consumer-groups/" + grupo)
                    .andExpect(status().isConflict())
                    .andExpect(jsonPath("$.message").value(containsString("consumidores activos")));
        }
    }

    @Test
    void grupoSinConsumidores_seReseteaYSeBorra() throws Exception {
        String topico = crearTopicoConMensajes();
        String grupo = nombre("grupo");
        consumidorQueConfirma(grupo, topico).close(); // al cerrar, deja el grupo: queda EMPTY con sus offsets

        // earliest sin 'topic': solo las particiones con offsets (0 y 1) → reprocesa 3 + 3.
        JsonNode reset = json(post("/api/kafka/consumer-groups/" + grupo + "/reset-offsets", Map.of("strategy", "earliest"))
                .andExpect(status().isOk()));
        assertThat(reset.get("offsets")).hasSize(2);
        assertThat(reset.get("offsets").get(0).get("anterior").asLong()).isEqualTo(3);
        assertThat(reset.get("offsets").get(0).get("nuevo").asLong()).isZero();
        JsonNode detalle = json(get("/api/kafka/consumer-groups/" + grupo).andExpect(status().isOk()));
        assertThat(detalle.get("state").asText()).isEqualTo("EMPTY");
        assertThat(detalle.get("lag").asLong()).isEqualTo(6);

        // latest con 'topic': también la partición 2, que nunca había confirmado → nada pendiente.
        JsonNode alFinal = json(post("/api/kafka/consumer-groups/" + grupo + "/reset-offsets",
                Map.of("strategy", "latest", "topic", topico)).andExpect(status().isOk()));
        assertThat(alFinal.get("offsets")).hasSize(3);
        assertThat(alFinal.get("offsets").get(2).get("anterior").isNull()).isTrue();
        assertThat(json(get("/api/kafka/consumer-groups/" + grupo)).get("lag").asLong()).isZero();

        // timestamp: desde el pasado → vuelve al principio; desde el futuro → queda al final.
        JsonNode desdeAyer = json(post("/api/kafka/consumer-groups/" + grupo + "/reset-offsets",
                Map.of("strategy", "timestamp", "timestamp", Instant.now().minus(Duration.ofDays(1)).toString()))
                .andExpect(status().isOk()));
        desdeAyer.get("offsets").forEach(o -> assertThat(o.get("nuevo").asLong()).isZero());
        JsonNode desdeManana = json(post("/api/kafka/consumer-groups/" + grupo + "/reset-offsets",
                Map.of("strategy", "timestamp", "timestamp", Instant.now().plus(Duration.ofDays(1)).toString()))
                .andExpect(status().isOk()));
        desdeManana.get("offsets").forEach(o -> assertThat(o.get("nuevo").asLong()).isEqualTo(3));

        delete("/api/kafka/consumer-groups/" + grupo).andExpect(status().isNoContent());
        get("/api/kafka/consumer-groups/" + grupo).andExpect(status().isNotFound());
    }

    @Test
    void resetOffsets_validaLaPeticion() throws Exception {
        String grupo = nombre("grupo");
        post("/api/kafka/consumer-groups/" + grupo + "/reset-offsets", Map.of("strategy", "ayer"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("La estrategia debe ser earliest, latest o timestamp"));
        post("/api/kafka/consumer-groups/" + grupo + "/reset-offsets", Map.of("strategy", "timestamp"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("debes indicar 'timestamp'")));
        // Grupo sin offsets y sin 'topic': no hay dónde dejarlo.
        post("/api/kafka/consumer-groups/" + grupo + "/reset-offsets", Map.of("strategy", "earliest"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("indica 'topic'")));
        post("/api/kafka/consumer-groups/" + grupo + "/reset-offsets",
                Map.of("strategy", "earliest", "topic", nombre("no-existe")))
                .andExpect(status().isNotFound());
    }

    @Test
    void grupoInexistenteONombreInvalido() throws Exception {
        get("/api/kafka/consumer-groups/" + nombre("fantasma"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value(containsString("no existe")));
        delete("/api/kafka/consumer-groups/" + nombre("fantasma")).andExpect(status().isNotFound());
        get("/api/kafka/consumer-groups/grupo$raro").andExpect(status().isBadRequest());
    }

    // ---- Utilidades ----

    /** Tópico de 3 particiones × 3 réplicas con 3 mensajes en cada partición. */
    private String crearTopicoConMensajes() throws Exception {
        String topico = nombre("eventos");
        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                broker.getBrokersAsString()))) {
            admin.createTopics(List.of(new NewTopic(topico, 3, (short) 3))).all().get();
        }
        try (KafkaProducer<String, String> productor = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ProducerConfig.ACKS_CONFIG, "all"), new StringSerializer(), new StringSerializer())) {
            for (int particion = 0; particion < 3; particion++) {
                for (int i = 0; i < 3; i++) {
                    productor.send(new ProducerRecord<>(topico, particion, "clave-" + i, "{\"n\":" + i + "}")).get();
                }
            }
        }
        return topico;
    }

    /** Consumidor del grupo que lee los 9 mensajes y confirma 3 en la partición 0, 1 en la 1 y nada en la 2. */
    private KafkaConsumer<String, String> consumidorQueConfirma(String grupo, String topico) {
        KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, grupo,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new StringDeserializer(), new StringDeserializer());
        consumidor.subscribe(List.of(topico));
        int leidos = 0;
        long limite = System.currentTimeMillis() + 20_000;
        while (leidos < 9 && System.currentTimeMillis() < limite) {
            leidos += consumidor.poll(Duration.ofMillis(300)).count();
        }
        assertThat(leidos).isEqualTo(9);
        consumidor.commitSync(Map.of(
                new TopicPartition(topico, 0), new OffsetAndMetadata(3),
                new TopicPartition(topico, 1), new OffsetAndMetadata(1)));
        return consumidor;
    }

    private static JsonNode offset(JsonNode detalle, int particion) {
        for (JsonNode o : detalle.get("offsets")) {
            if (o.get("partition").asInt() == particion) {
                return o;
            }
        }
        throw new AssertionError("No está la partición " + particion);
    }

    private static JsonNode buscar(JsonNode lista, String grupo) {
        for (JsonNode g : lista) {
            if (g.get("groupId").asText().equals(grupo)) {
                return g;
            }
        }
        throw new AssertionError("No está el grupo " + grupo);
    }
}
