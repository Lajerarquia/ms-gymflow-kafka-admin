package cl.duoc.gymflow.kafkaadmin.kafka;

import cl.duoc.gymflow.kafkaadmin.config.KafkaAdminProperties;
import cl.duoc.gymflow.kafkaadmin.error.ErrorApi;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Único punto donde se espera una respuesta del {@code AdminClient} y se traducen sus errores a HTTP con mensajes en
 * español (formato de error común):
 * <ul>
 *   <li>404: tópico o grupo inexistente.</li>
 *   <li>409: el tópico ya existe, el grupo tiene consumidores activos.</li>
 *   <li>400: nombre, particiones, factor de replicación o configuración inválidos (Kafka los rechaza).</li>
 *   <li>503: Kafka no respondió dentro de {@code gymflow.kafka-admin.timeout}.</li>
 * </ul>
 */
@Component
public class ErroresKafka {

    private final long timeoutMs;

    public ErroresKafka(KafkaAdminProperties propiedades) {
        this.timeoutMs = propiedades.timeout().toMillis();
    }

    /**
     * Espera el resultado de una operación del AdminClient.
     *
     * @param operacion qué se intentaba, para el mensaje de error (ej. "crear el tópico 'x'")
     */
    public <T> T esperar(KafkaFuture<T> futuro, String operacion) {
        try {
            return futuro.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ErrorApi(HttpStatus.SERVICE_UNAVAILABLE, "Se interrumpió la operación al " + operacion, e);
        } catch (TimeoutException e) {
            throw noResponde(operacion, e);
        } catch (ExecutionException e) {
            throw traducir(e.getCause() != null ? e.getCause() : e, operacion);
        }
    }

    static ErrorApi traducir(Throwable causa, String operacion) {
        String detalle = causa.getMessage() != null ? ": " + causa.getMessage() : "";
        if (causa instanceof UnknownTopicOrPartitionException) {
            return new ErrorApi(HttpStatus.NOT_FOUND, "No se pudo " + operacion + ": el tópico no existe", causa);
        }
        if (causa instanceof GroupIdNotFoundException) {
            return new ErrorApi(HttpStatus.NOT_FOUND, "No se pudo " + operacion + ": el grupo no existe", causa);
        }
        if (causa instanceof TopicExistsException) {
            return new ErrorApi(HttpStatus.CONFLICT, "No se pudo " + operacion + ": el tópico ya existe", causa);
        }
        if (causa instanceof GroupNotEmptyException || causa instanceof UnknownMemberIdException) {
            return new ErrorApi(HttpStatus.CONFLICT,
                    "No se pudo " + operacion + ": el grupo tiene consumidores activos (deténlos primero)", causa);
        }
        if (causa instanceof InvalidTopicException || causa instanceof InvalidPartitionsException
                || causa instanceof InvalidReplicationFactorException || causa instanceof InvalidConfigurationException
                || causa instanceof InvalidRequestException || causa instanceof PolicyViolationException) {
            return new ErrorApi(HttpStatus.BAD_REQUEST, "Kafka rechazó " + operacion + detalle, causa);
        }
        if (causa instanceof org.apache.kafka.common.errors.TimeoutException) {
            return noResponde(operacion, causa);
        }
        return new ErrorApi(HttpStatus.SERVICE_UNAVAILABLE, "Error de Kafka al " + operacion + detalle, causa);
    }

    private static ErrorApi noResponde(String operacion, Throwable causa) {
        return new ErrorApi(HttpStatus.SERVICE_UNAVAILABLE,
                "Kafka no responde: no se pudo " + operacion + " (revisa que los brokers estén arriba)", causa);
    }
}
