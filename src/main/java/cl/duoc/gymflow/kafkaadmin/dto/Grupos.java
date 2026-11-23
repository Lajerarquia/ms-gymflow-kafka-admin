package cl.duoc.gymflow.kafkaadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;
import java.util.List;

/** Peticiones y respuestas de {@code /api/kafka/consumer-groups/**}. */
public final class Grupos {

    private Grupos() {
    }

    /**
     * Una fila de la lista.
     *
     * @param state   STABLE (consumiendo), EMPTY (sin consumidores), PREPARING_REBALANCE / COMPLETING_REBALANCE, DEAD
     * @param members consumidores conectados ahora (hilos o instancias)
     * @param lag     mensajes que el grupo aún no procesa, sumando todas sus particiones
     */
    public record GrupoResumen(String groupId, String state, int members, List<String> topics, long lag) {
    }

    /** Detalle: quién consume qué partición y el lag de cada una. */
    public record GrupoDetalle(String groupId, String state, Integer coordinator, String partitionAssignor,
                               List<MiembroResponse> members, List<OffsetResponse> offsets, long lag) {
    }

    /** @param assignments particiones asignadas, como {@code "reservations.events-0"} */
    public record MiembroResponse(String memberId, String clientId, String host, List<String> assignments) {
    }

    /**
     * @param committedOffset siguiente offset que leerá el grupo ({@code null}: aún no confirma nada en esa partición)
     * @param endOffset       offset siguiente al último mensaje de la partición
     * @param lag             {@code endOffset - committedOffset}
     * @param memberId        consumidor que tiene asignada la partición ahora ({@code null} si nadie)
     */
    public record OffsetResponse(String topic, int partition, Long committedOffset, long endOffset, Long lag,
                                 String memberId) {
    }

    /**
     * Reset de offsets.
     *
     * @param strategy  {@code earliest} (reprocesar todo lo retenido), {@code latest} (saltar lo pendiente) o
     *                  {@code timestamp} (desde una fecha)
     * @param timestamp obligatorio con {@code timestamp}; ISO-8601 UTC
     * @param topic     opcional: solo ese tópico (incluye particiones donde el grupo nunca confirmó). Sin él, todos los
     *                  tópicos donde el grupo tiene offsets
     */
    public record ResetOffsetsRequest(
            @NotBlank(message = "La estrategia es obligatoria (earliest, latest o timestamp)")
            @Pattern(regexp = "earliest|latest|timestamp", message = "La estrategia debe ser earliest, latest o timestamp")
            String strategy,
            Instant timestamp,
            @Pattern(regexp = Topicos.PATRON_NOMBRE, message = "El nombre del tópico solo admite letras, números, '.', '_' y '-'")
            String topic) {
    }

    public record ResetOffsetsResponse(String groupId, String strategy, List<OffsetMovido> offsets) {
    }

    /** @param anterior offset confirmado antes del reset ({@code null} si no había) */
    public record OffsetMovido(String topic, int partition, Long anterior, long nuevo) {
    }
}
