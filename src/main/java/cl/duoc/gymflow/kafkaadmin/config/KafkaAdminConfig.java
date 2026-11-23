package cl.duoc.gymflow.kafkaadmin.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Un solo {@link AdminClient} para todo el servicio (es thread-safe y mantiene sus conexiones a los brokers).
 * <p>
 * Se arma con las mismas propiedades {@code spring.kafka.*} que Spring Boot ya junta en {@link KafkaAdmin}
 * (bootstrap servers, client-id), más los timeouts propios: así una llamada nunca queda colgada si Kafka no responde.
 * Crear el cliente no conecta a nada: si Kafka está caído el servicio arranca igual y las llamadas responden 503.
 */
@Configuration
public class KafkaAdminConfig {

    @Bean(destroyMethod = "close")
    public AdminClient adminClient(KafkaAdmin kafkaAdmin, KafkaAdminProperties propiedades) {
        Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        int timeoutMs = (int) propiedades.timeout().toMillis();
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);
        return AdminClient.create(config);
    }
}
