package cl.duoc.gymflow.kafkaadmin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import cl.duoc.gymflow.kafkaadmin.support.PruebaKafkaBase;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

/** Clúster y tópicos contra 3 brokers embebidos: lo que hace la API y cómo responde a cada error. */
class TopicosTest extends PruebaKafkaBase {

    @Test
    void cluster_muestraLosTresBrokersYElControlador() throws Exception {
        JsonNode cluster = json(get("/api/kafka/cluster").andExpect(status().isOk()));
        assertThat(cluster.get("clusterId").asText()).isNotBlank();
        assertThat(cluster.get("brokers")).hasSize(3);
        assertThat(cluster.get("controller").get("id").asInt()).isIn(0, 1, 2);
    }

    @Test
    void crearTopico_quedaConSusParticionesReplicasYConfigs() throws Exception {
        String nombre = nombre("prueba");
        JsonNode creado = json(post("/api/kafka/topics", topico(nombre, 3, 3,
                Map.of("retention.ms", "3600000", "cleanup.policy", "delete", "min.insync.replicas", "2")))
                .andExpect(status().isCreated()));

        assertThat(creado.get("name").asText()).isEqualTo(nombre);
        assertThat(creado.get("replicationFactor").asInt()).isEqualTo(3);
        assertThat(creado.get("partitions")).hasSize(3);
        creado.get("partitions").forEach(p -> {
            assertThat(p.get("replicas")).hasSize(3);
            assertThat(p.get("isr")).hasSize(3);
            assertThat(p.get("underReplicated").asBoolean()).isFalse();
        });
        JsonNode retencion = config(creado, "retention.ms");
        assertThat(retencion.get("value").asText()).isEqualTo("3600000");
        assertThat(retencion.get("source").asText()).isEqualTo("DYNAMIC_TOPIC_CONFIG");
        assertThat(retencion.get("isDefault").asBoolean()).isFalse();

        // Aparece en la lista con su resumen.
        JsonNode lista = json(get("/api/kafka/topics").andExpect(status().isOk()));
        JsonNode fila = buscar(lista, nombre);
        assertThat(fila.get("partitions").asInt()).isEqualTo(3);
        assertThat(fila.get("replicationFactor").asInt()).isEqualTo(3);
        assertThat(fila.get("retentionMs").asLong()).isEqualTo(3600000L);
        assertThat(fila.get("cleanupPolicy").asText()).isEqualTo("delete");
        assertThat(fila.get("minInsyncReplicas").asInt()).isEqualTo(2);
        assertThat(fila.get("protegido").asBoolean()).isFalse();

        // Crearlo de nuevo → 409.
        post("/api/kafka/topics", topico(nombre, 3, 3, Map.of()))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value(containsString("ya existe")))
                .andExpect(jsonPath("$.path").value("/api/kafka/topics"));
    }

    @Test
    void crearTopico_rechazaDatosInvalidosCon400() throws Exception {
        post("/api/kafka/topics", topico("mal nombre!", 1, 1, Map.of()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("solo admite letras")));
        post("/api/kafka/topics", topico(nombre("muchas-replicas"), 1, 5, Map.of()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("brokers disponibles (3)")));
        post("/api/kafka/topics", topico("__interno", 1, 1, Map.of()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("reservados para Kafka")));
        post("/api/kafka/topics", topico(nombre("sin-particiones"), 0, 1, Map.of()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("El tópico debe tener al menos 1 partición"));
        // Una configuración que Kafka no conoce: la rechaza el broker y se traduce a 400.
        post("/api/kafka/topics", topico(nombre("config-rara"), 1, 1, Map.of("retencion.dias", "7")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("Kafka rechazó crear el tópico")));
        post("/api/kafka/topics", "{\"name\": ")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("El cuerpo de la petición no es un JSON válido"));
    }

    @Test
    void actualizarConfigs_fijaYQuitaPropiedades() throws Exception {
        String nombre = nombre("retencion");
        post("/api/kafka/topics", topico(nombre, 1, 3, Map.of())).andExpect(status().isCreated());

        JsonNode cambiado = json(put("/api/kafka/topics/" + nombre + "/configs",
                Map.of("configs", Map.of("retention.ms", "86400000"))).andExpect(status().isOk()));
        assertThat(config(cambiado, "retention.ms").get("value").asText()).isEqualTo("86400000");
        assertThat(config(cambiado, "retention.ms").get("isDefault").asBoolean()).isFalse();

        // null → se quita y vuelve al valor por defecto del broker.
        Map<String, String> quitar = new HashMap<>();
        quitar.put("retention.ms", null);
        JsonNode restaurado = json(put("/api/kafka/topics/" + nombre + "/configs", Map.of("configs", quitar))
                .andExpect(status().isOk()));
        assertThat(config(restaurado, "retention.ms").get("isDefault").asBoolean()).isTrue();

        put("/api/kafka/topics/" + nombre + "/configs", Map.of("configs", Map.of("retention.ms", "mucho")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("Kafka rechazó actualizar la configuración")));
        put("/api/kafka/topics/" + nombre + "/configs", Map.of("configs", Map.of()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Indica al menos una configuración en 'configs'"));
        put("/api/kafka/topics/" + nombre("no-existe") + "/configs", Map.of("configs", Map.of("retention.ms", "1000")))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value(containsString("el tópico no existe")));
    }

    @Test
    void aumentarParticiones_soloHaciaArriba() throws Exception {
        String nombre = nombre("particiones");
        post("/api/kafka/topics", topico(nombre, 3, 3, Map.of())).andExpect(status().isCreated());

        JsonNode ampliado = json(post("/api/kafka/topics/" + nombre + "/partitions", Map.of("partitions", 5))
                .andExpect(status().isOk()));
        assertThat(ampliado.get("partitions")).hasSize(5);

        post("/api/kafka/topics/" + nombre + "/partitions", Map.of("partitions", 2))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("ya tiene 5 y Kafka no permite reducirlas")));
        post("/api/kafka/topics/" + nombre("no-existe") + "/partitions", Map.of("partitions", 4))
                .andExpect(status().isNotFound());
    }

    /**
     * Borrado real. En Windows el broker embebido no puede renombrar los archivos abiertos del tópico borrado y se
     * apaga (bug conocido de Kafka en Windows, KAFKA-1194), lo que tumbaría las demás pruebas. En Linux (los brokers
     * reales corren en contenedores Linux) y en {@code docker run maven} sí corre.
     */
    @Test
    @DisabledOnOs(OS.WINDOWS)
    void borrarTopico_loElimina() throws Exception {
        String nombre = nombre("borrable");
        post("/api/kafka/topics", topico(nombre, 1, 1, Map.of())).andExpect(status().isCreated());

        delete("/api/kafka/topics/" + nombre).andExpect(status().isNoContent());
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                get("/api/kafka/topics/" + nombre).andExpect(status().isNotFound()));
    }

    @Test
    void borrarTopico_inexistenteDa404YLosProtegidos409() throws Exception {
        delete("/api/kafka/topics/" + nombre("no-existe")).andExpect(status().isNotFound());

        delete("/api/kafka/topics/reservations.events")
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value(containsString("está protegido")));
        delete("/api/kafka/topics/audit.timeline").andExpect(status().isConflict());
        delete("/api/kafka/topics/__consumer_offsets").andExpect(status().isConflict());
        // Siguen ahí.
        get("/api/kafka/topics/reservations.events")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.protegido").value(true));
    }

    @Test
    void describirTopico_inexistenteONombreInvalido() throws Exception {
        get("/api/kafka/topics/" + nombre("fantasma"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.error").value("Not Found"));
        get("/api/kafka/topics/topico$raro")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("no es válido")));
    }

    private static JsonNode config(JsonNode detalle, String clave) {
        for (JsonNode c : detalle.get("configs")) {
            if (c.get("name").asText().equals(clave)) {
                return c;
            }
        }
        throw new AssertionError("No está la configuración " + clave);
    }

    private static JsonNode buscar(JsonNode lista, String nombre) {
        for (JsonNode t : lista) {
            if (t.get("name").asText().equals(nombre)) {
                return t;
            }
        }
        throw new AssertionError("No está el tópico " + nombre);
    }
}
