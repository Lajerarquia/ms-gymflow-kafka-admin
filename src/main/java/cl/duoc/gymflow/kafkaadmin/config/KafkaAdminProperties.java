package cl.duoc.gymflow.kafkaadmin.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Propiedades {@code gymflow.kafka-admin.*}.
 *
 * @param timeout    cuánto espera cada operación a Kafka antes de responder 503
 * @param protegidos tópicos de negocio que no se pueden borrar (409)
 */
@ConfigurationProperties("gymflow.kafka-admin")
public record KafkaAdminProperties(
        @DefaultValue("10s") Duration timeout,
        @DefaultValue({"reservations.events", "audit.timeline"}) List<String> protegidos) {
}
