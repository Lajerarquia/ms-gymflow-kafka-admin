package cl.duoc.gymflow.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.Map;

/** Peticiones y respuestas de {@code /api/kafka/topics/**}. */
public final class Topicos {

    /** Caracteres que Kafka acepta en un nombre de tópico (y que validamos antes de llamarlo). */
    public static final String PATRON_NOMBRE = "[a-zA-Z0-9._-]+";

    private Topicos() {
    }

    /** Una fila de la lista: lo necesario para ver de un vistazo cómo está configurado cada flujo. */
    public record TopicoResumen(String name, int partitions, int replicationFactor, String cleanupPolicy,
                                Long retentionMs, Integer minInsyncReplicas, boolean internal, boolean protegido) {
    }

    /** Detalle: cada partición con su líder, réplicas e ISR, y las configuraciones del tópico. */
    public record TopicoDetalle(String name, boolean internal, boolean protegido, int replicationFactor,
                                List<ParticionResponse> partitions, List<ConfigResponse> configs) {
    }

    /**
     * @param underReplicated hay réplicas fuera de sincronía (ISR más chico que las réplicas): un broker cayó o va atrasado
     */
    public record ParticionResponse(int partition, Integer leader, List<Integer> replicas, List<Integer> isr,
                                    boolean underReplicated) {
    }

    /**
     * @param source    de dónde sale el valor: DYNAMIC_TOPIC_CONFIG (fijado en el tópico), DEFAULT_CONFIG, STATIC_BROKER_CONFIG...
     * @param isDefault el valor es el del broker por defecto (no se fijó para este tópico)
     */
    public record ConfigResponse(String name, String value, String source, boolean isDefault, boolean readOnly) {
    }

    public record CrearTopicoRequest(
            @NotBlank(message = "El nombre del tópico es obligatorio")
            @Size(max = 249, message = "El nombre del tópico no puede superar 249 caracteres")
            @Pattern(regexp = PATRON_NOMBRE, message = "El nombre del tópico solo admite letras, números, '.', '_' y '-'")
            String name,
            @NotNull(message = "La cantidad de particiones es obligatoria")
            @Min(value = 1, message = "El tópico debe tener al menos 1 partición")
            @Max(value = 100, message = "No se permiten más de 100 particiones por tópico")
            Integer partitions,
            @NotNull(message = "El factor de replicación es obligatorio")
            @Min(value = 1, message = "El factor de replicación debe ser al menos 1")
            @Max(value = 10, message = "El factor de replicación no puede superar 10")
            Integer replicationFactor,
            /** Configuraciones del tópico (ej. {@code retention.ms}, {@code cleanup.policy}). Opcional. */
            Map<String, String> configs) {
    }

    /**
     * Cambios de configuración: un valor fija la propiedad en el tópico; {@code null} la quita (vuelve al valor por
     * defecto del broker). Ej.: {@code {"configs": {"retention.ms": "86400000", "segment.ms": null}}}.
     */
    public record ActualizarConfigsRequest(
            @NotEmpty(message = "Indica al menos una configuración en 'configs'")
            Map<String, String> configs) {
    }

    public record AumentarParticionesRequest(
            @NotNull(message = "La cantidad total de particiones es obligatoria")
            @Min(value = 1, message = "El tópico debe tener al menos 1 partición")
            @Max(value = 100, message = "No se permiten más de 100 particiones por tópico")
            Integer partitions) {
    }
}
