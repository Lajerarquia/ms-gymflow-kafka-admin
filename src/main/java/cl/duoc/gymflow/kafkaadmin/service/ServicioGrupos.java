package cl.duoc.gymflow.kafkaadmin.service;

import cl.duoc.gymflow.kafkaadmin.dto.Grupos.GrupoDetalle;
import cl.duoc.gymflow.kafkaadmin.dto.Grupos.GrupoResumen;
import cl.duoc.gymflow.kafkaadmin.dto.Grupos.MiembroResponse;
import cl.duoc.gymflow.kafkaadmin.dto.Grupos.OffsetMovido;
import cl.duoc.gymflow.kafkaadmin.dto.Grupos.OffsetResponse;
import cl.duoc.gymflow.kafkaadmin.dto.Grupos.ResetOffsetsRequest;
import cl.duoc.gymflow.kafkaadmin.dto.Grupos.ResetOffsetsResponse;
import cl.duoc.gymflow.kafkaadmin.error.ErrorApi;
import cl.duoc.gymflow.kafkaadmin.kafka.ErroresKafka;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * Consumer groups: listar con lag, detalle (quién consume qué partición), reset de offsets y borrar.
 * <p>
 * El <b>lag</b> de una partición es {@code endOffset - committedOffset}: cuántos mensajes llegaron y el grupo aún no
 * procesa. Si crece sin parar, el consumidor está caído o no da abasto (se escala con más instancias, hasta una por
 * partición).
 * <p>
 * El reset solo se permite con el grupo <b>sin consumidores activos</b> (409): Kafka no deja mover offsets de un grupo
 * que está consumiendo, porque el consumidor los volvería a pisar con su próximo commit.
 */
@Service
public class ServicioGrupos {

    private static final Logger log = LoggerFactory.getLogger(ServicioGrupos.class);

    private final AdminClient admin;
    private final ErroresKafka kafka;

    public ServicioGrupos(AdminClient admin, ErroresKafka kafka) {
        this.admin = admin;
        this.kafka = kafka;
    }

    public List<GrupoResumen> listar() {
        List<String> ids = kafka.esperar(admin.listConsumerGroups().all(), "listar los consumer groups").stream()
                .map(ConsumerGroupListing::groupId)
                .sorted()
                .toList();
        if (ids.isEmpty()) {
            return List.of();
        }
        Map<String, ConsumerGroupDescription> descripciones =
                kafka.esperar(admin.describeConsumerGroups(ids).all(), "describir los consumer groups");
        Map<String, ListConsumerGroupOffsetsSpec> specs = ids.stream()
                .collect(Collectors.toMap(id -> id, id -> new ListConsumerGroupOffsetsSpec()));
        Map<String, Map<TopicPartition, OffsetAndMetadata>> confirmados =
                kafka.esperar(admin.listConsumerGroupOffsets(specs).all(), "leer los offsets de los consumer groups");
        Set<TopicPartition> todas = new HashSet<>();
        confirmados.values().forEach(m -> todas.addAll(sinNulos(m).keySet()));
        Map<TopicPartition, Long> finales = offsetsFinales(todas);

        return ids.stream().map(id -> {
            ConsumerGroupDescription d = descripciones.get(id);
            Map<TopicPartition, OffsetAndMetadata> offsets = sinNulos(confirmados.getOrDefault(id, Map.of()));
            long lag = offsets.entrySet().stream()
                    .mapToLong(e -> Math.max(0, finales.getOrDefault(e.getKey(), 0L) - e.getValue().offset()))
                    .sum();
            List<String> topicos = new TreeSet<>(offsets.keySet().stream().map(TopicPartition::topic).toList())
                    .stream().toList();
            return new GrupoResumen(id, d.state().name(), d.members().size(), topicos, lag);
        }).toList();
    }

    public GrupoDetalle detalle(String id) {
        Nombres.grupo(id);
        ConsumerGroupDescription d = describir(id);
        Map<TopicPartition, OffsetAndMetadata> confirmados = confirmados(id);
        if (noExiste(d, confirmados)) {
            throw new ErrorApi(HttpStatus.NOT_FOUND, "El consumer group '" + id + "' no existe");
        }
        Map<TopicPartition, String> asignadas = new HashMap<>();
        List<MiembroResponse> miembros = d.members().stream()
                .sorted(Comparator.comparing(MemberDescription::clientId).thenComparing(MemberDescription::consumerId))
                .map(m -> {
                    m.assignment().topicPartitions().forEach(tp -> asignadas.put(tp, m.consumerId()));
                    List<String> particiones = m.assignment().topicPartitions().stream()
                            .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                            .map(TopicPartition::toString)
                            .toList();
                    return new MiembroResponse(m.consumerId(), m.clientId(), m.host(), particiones);
                })
                .toList();

        Set<TopicPartition> particiones = new HashSet<>(confirmados.keySet());
        particiones.addAll(asignadas.keySet());
        Map<TopicPartition, Long> finales = offsetsFinales(particiones);
        List<OffsetResponse> offsets = particiones.stream()
                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                .map(tp -> {
                    OffsetAndMetadata confirmado = confirmados.get(tp);
                    long fin = finales.getOrDefault(tp, 0L);
                    Long comprometido = confirmado == null ? null : confirmado.offset();
                    Long lag = comprometido == null ? null : Math.max(0, fin - comprometido);
                    return new OffsetResponse(tp.topic(), tp.partition(), comprometido, fin, lag, asignadas.get(tp));
                })
                .toList();
        long lagTotal = offsets.stream().map(OffsetResponse::lag).filter(Objects::nonNull).mapToLong(Long::longValue).sum();
        Integer coordinador = d.coordinator() == null ? null : d.coordinator().id();
        return new GrupoDetalle(id, d.state().name(), coordinador, d.partitionAssignor(), miembros, offsets, lagTotal);
    }

    /**
     * Mueve los offsets del grupo: {@code earliest} reprocesa todo lo que el tópico aún retiene (los consumidores son
     * idempotentes por {@code eventId}, así que no duplica datos), {@code latest} salta lo pendiente y {@code timestamp}
     * vuelve a una fecha.
     */
    public ResetOffsetsResponse resetearOffsets(String id, ResetOffsetsRequest datos) {
        Nombres.grupo(id);
        if ("timestamp".equals(datos.strategy()) && datos.timestamp() == null) {
            throw new ErrorApi(HttpStatus.BAD_REQUEST, "Con la estrategia 'timestamp' debes indicar 'timestamp' (ISO-8601)");
        }
        ConsumerGroupDescription d = describir(id);
        if (!d.members().isEmpty()) {
            throw new ErrorApi(HttpStatus.CONFLICT, "El consumer group '" + id + "' tiene " + d.members().size()
                    + " consumidor(es) activo(s): detén el servicio antes de mover sus offsets");
        }
        Map<TopicPartition, OffsetAndMetadata> confirmados = confirmados(id);
        Set<TopicPartition> particiones = particionesAResetear(id, datos.topic(), confirmados);

        Map<TopicPartition, Long> nuevos = switch (datos.strategy()) {
            case "earliest" -> offsets(particiones, OffsetSpec.earliest());
            case "latest" -> offsetsFinales(particiones);
            default -> {
                Map<TopicPartition, Long> porFecha = offsets(particiones, OffsetSpec.forTimestamp(datos.timestamp().toEpochMilli()));
                Map<TopicPartition, Long> finales = offsetsFinales(particiones);
                // -1: no hay mensajes desde esa fecha → se deja al final (nada que reprocesar en esa partición).
                porFecha.replaceAll((tp, offset) -> offset < 0 ? finales.get(tp) : offset);
                yield porFecha;
            }
        };
        Map<TopicPartition, OffsetAndMetadata> cambio = nuevos.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
        kafka.esperar(admin.alterConsumerGroupOffsets(id, cambio).all(), "mover los offsets del grupo '" + id + "'");

        List<OffsetMovido> movidos = nuevos.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(
                        Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition)))
                .map(e -> new OffsetMovido(e.getKey().topic(), e.getKey().partition(),
                        confirmados.containsKey(e.getKey()) ? confirmados.get(e.getKey()).offset() : null, e.getValue()))
                .toList();
        log.warn("Consumer group '{}': offsets reseteados ({}) en {} particiones", id, datos.strategy(), movidos.size());
        return new ResetOffsetsResponse(id, datos.strategy(), movidos);
    }

    public void borrar(String id) {
        Nombres.grupo(id);
        kafka.esperar(admin.deleteConsumerGroups(List.of(id)).all(), "borrar el consumer group '" + id + "'");
        log.warn("Consumer group '{}' borrado", id);
    }

    // ---- Apoyo ----

    private ConsumerGroupDescription describir(String id) {
        return kafka.esperar(admin.describeConsumerGroups(List.of(id)).describedGroups().get(id),
                "describir el consumer group '" + id + "'");
    }

    private Map<TopicPartition, OffsetAndMetadata> confirmados(String id) {
        return sinNulos(kafka.esperar(admin.listConsumerGroupOffsets(id).partitionsToOffsetAndMetadata(),
                "leer los offsets del grupo '" + id + "'"));
    }

    /** Un grupo que Kafka no conoce se describe como DEAD, sin miembros ni offsets. */
    private static boolean noExiste(ConsumerGroupDescription d, Map<TopicPartition, OffsetAndMetadata> confirmados) {
        return d.state() == ConsumerGroupState.DEAD && d.members().isEmpty() && confirmados.isEmpty();
    }

    private Set<TopicPartition> particionesAResetear(String id, String topico,
                                                     Map<TopicPartition, OffsetAndMetadata> confirmados) {
        if (topico != null && !topico.isBlank()) {
            TopicDescription d = kafka.esperar(admin.describeTopics(List.of(topico)).allTopicNames(),
                    "describir el tópico '" + topico + "'").get(topico);
            return d.partitions().stream().map(p -> new TopicPartition(topico, p.partition())).collect(Collectors.toSet());
        }
        if (confirmados.isEmpty()) {
            throw new ErrorApi(HttpStatus.BAD_REQUEST, "El consumer group '" + id
                    + "' no tiene offsets confirmados: indica 'topic' para elegir dónde dejarlo");
        }
        return confirmados.keySet();
    }

    private Map<TopicPartition, Long> offsetsFinales(Collection<TopicPartition> particiones) {
        return offsets(particiones, OffsetSpec.latest());
    }

    private Map<TopicPartition, Long> offsets(Collection<TopicPartition> particiones, OffsetSpec spec) {
        if (particiones.isEmpty()) {
            return new HashMap<>();
        }
        Map<TopicPartition, OffsetSpec> pedido = particiones.stream().collect(Collectors.toMap(tp -> tp, tp -> spec));
        Map<TopicPartition, ListOffsetsResultInfo> resultado =
                kafka.esperar(admin.listOffsets(pedido).all(), "leer los offsets de las particiones");
        Map<TopicPartition, Long> offsets = new HashMap<>();
        resultado.forEach((tp, info) -> offsets.put(tp, info.offset()));
        return offsets;
    }

    /** listConsumerGroupOffsets puede devolver null para una partición sin offset: se descartan. */
    private static Map<TopicPartition, OffsetAndMetadata> sinNulos(Map<TopicPartition, OffsetAndMetadata> mapa) {
        Map<TopicPartition, OffsetAndMetadata> limpio = new HashMap<>();
        mapa.forEach((tp, o) -> {
            if (o != null) {
                limpio.put(tp, o);
            }
        });
        return limpio;
    }
}
