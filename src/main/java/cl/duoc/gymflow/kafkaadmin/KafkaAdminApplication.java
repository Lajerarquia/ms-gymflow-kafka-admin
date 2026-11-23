package cl.duoc.gymflow.kafkaadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * ms-gymflow-kafka-admin (EP5): administra el clúster Kafka de GymFlow con el {@code AdminClient} de Kafka.
 * Crea, lista, describe y mantiene tópicos y sus configuraciones, e inspecciona y administra los consumer groups
 * (lag, reset de offsets). Sin BD y sin puertos publicados: lo usa el BFF en {@code /api/admin/kafka/**} (rol Admin).
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminApplication.class, args);
    }
}
