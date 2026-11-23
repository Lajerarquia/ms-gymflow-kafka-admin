package cl.duoc.gymflow.kafkaadmin.service;

import cl.duoc.gymflow.kafkaadmin.config.KafkaAdminProperties;
import cl.duoc.gymflow.kafkaadmin.dto.Topicos.ActualizarConfigsRequest;
import cl.duoc.gymflow.kafkaadmin.dto.Topicos.AumentarParticionesRequest;
import cl.duoc.gymflow.kafkaadmin.dto.Topicos.ConfigResponse;
import cl.duoc.gymflow.kafkaadmin.dto.Topicos.CrearTopicoRequest;
import cl.duoc.gymflow.kafkaadmin.dto.Topicos.ParticionResponse;
import cl.duoc.gymflow.kafkaadmin.dto.Topicos.TopicoDetalle;
import cl.duoc.gymflow.kafkaadmin.dto.Topicos.TopicoResumen;
import cl.duoc.gymflow.kafkaadmin.error.ErrorApi;
import cl.duoc.gymflow.kafkaadmin.kafka.ErroresKafka;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * Tópicos: listar, describir, crear, alterar configuraciones, aumentar particiones y borrar.
 * <p>
 * Reglas propias (además de las que valida Kafka):
 * <ul>
 *   <li>Los tópicos de negocio ({@code gymflow.kafka-admin.protegidos}) y los internos ({@code __*}) no se borran: 409.</li>
 *   <li>Los nombres que empiezan con {@code __} están reservados para Kafka: no se crean (400).</li>
 *   <li>El factor de replicación no puede superar la cantidad de brokers vivos (400 con mensaje claro).</li>
 *   <li>Las particiones solo se aumentan; Kafka no permite reducirlas (400).</li>
 * </ul>
 */
@Service
public class ServicioTopicos {

    private static final Logger log = LoggerFactory.getLogger(ServicioTopicos.class);

    /** Tras crear o ampliar un tópico, los metadatos tardan un momento en llegar a todos los brokers. */
    private static final int INTENTOS_METADATOS = 20;
    private static final long PAUSA_METADATOS_MS = 150;

    private final AdminClient admin;
    private final ErroresKafka kafka;
    private final ServicioCluster cluster;
    private final Set<String> protegidos;

    public ServicioTopicos(AdminClient admin, ErroresKafka kafka, ServicioCluster cluster, KafkaAdminProperties propiedades) {
        this.admin = admin;
        this.kafka = kafka;
        this.cluster = cluster;
        this.protegidos = Set.copyOf(propiedades.protegidos());
    }

    public List<TopicoResumen> listar(boolean incluirInternos) {
        Set<String> nombres = kafka.esperar(
                admin.listTopics(new ListTopicsOptions().listInternal(incluirInternos)).names(), "listar los tópicos");
        if (nombres.isEmpty()) {
            return List.of();
        }
        Map<String, TopicDescription> descripciones =
                kafka.esperar(admin.describeTopics(nombres).allTopicNames(), "describir los tópicos");
        Map<ConfigResource, Config> configs =
                kafka.esperar(admin.describeConfigs(recursos(nombres)).all(), "leer las configuraciones de los tópicos");
        return descripciones.values().stream()
                .sorted(Comparator.comparing(TopicDescription::name))
                .map(d -> resumen(d, configs.get(recurso(d.name()))))
                .toList();
    }

    public TopicoDetalle describir(String nombre) {
        Nombres.topico(nombre);
        String operacion = "describir el tópico '" + nombre + "'";
        TopicDescription descripcion =
                kafka.esperar(admin.describeTopics(List.of(nombre)).allTopicNames(), operacion).get(nombre);
        Config config = kafka.esperar(admin.describeConfigs(List.of(recurso(nombre))).all(), operacion)
                .get(recurso(nombre));
        return detalle(descripcion, config);
    }

    public TopicoDetalle crear(CrearTopicoRequest datos) {
        String nombre = Nombres.topico(datos.name());
        if (nombre.startsWith("__")) {
            throw new ErrorApi(HttpStatus.BAD_REQUEST, "Los nombres que empiezan con '__' están reservados para Kafka");
        }
        Map<String, String> configs = datos.configs() == null ? Map.of() : datos.configs();
        configs.forEach((clave, valor) -> {
            if (clave == null || clave.isBlank() || valor == null) {
                throw new ErrorApi(HttpStatus.BAD_REQUEST, "Cada configuración necesita nombre y valor (revisa '" + clave + "')");
            }
        });
        int brokers = cluster.cantidadBrokers();
        if (datos.replicationFactor() > brokers) {
            throw new ErrorApi(HttpStatus.BAD_REQUEST, "El factor de replicación (" + datos.replicationFactor()
                    + ") no puede superar la cantidad de brokers disponibles (" + brokers + ")");
        }
        NewTopic topico = new NewTopic(nombre, datos.partitions(), datos.replicationFactor().shortValue()).configs(configs);
        kafka.esperar(admin.createTopics(List.of(topico)).all(), "crear el tópico '" + nombre + "'");
        log.info("Tópico '{}' creado: {} particiones, factor de replicación {}, configs {}",
                nombre, datos.partitions(), datos.replicationFactor(), configs);
        return describirCuando(nombre, d -> true);
    }

    /** {@code incrementalAlterConfigs}: solo cambia las propiedades indicadas (SET) o las quita (DELETE si es null). */
    public TopicoDetalle actualizarConfigs(String nombre, ActualizarConfigsRequest datos) {
        Nombres.topico(nombre);
        List<AlterConfigOp> operaciones = new ArrayList<>();
        datos.configs().forEach((clave, valor) -> {
            if (clave == null || clave.isBlank()) {
                throw new ErrorApi(HttpStatus.BAD_REQUEST, "Hay una configuración sin nombre");
            }
            operaciones.add(valor == null
                    ? new AlterConfigOp(new ConfigEntry(clave, null), AlterConfigOp.OpType.DELETE)
                    : new AlterConfigOp(new ConfigEntry(clave, valor), AlterConfigOp.OpType.SET));
        });
        kafka.esperar(admin.incrementalAlterConfigs(Map.of(recurso(nombre), operaciones)).all(),
                "actualizar la configuración del tópico '" + nombre + "'");
        log.info("Tópico '{}': configuración actualizada {}", nombre, datos.configs());
        return describir(nombre);
    }

    public TopicoDetalle aumentarParticiones(String nombre, AumentarParticionesRequest datos) {
        int actuales = describir(nombre).partitions().size();
        int nuevas = datos.partitions();
        if (nuevas <= actuales) {
            throw new ErrorApi(HttpStatus.BAD_REQUEST, "Solo se pueden aumentar las particiones: '" + nombre + "' ya tiene "
                    + actuales + " y Kafka no permite reducirlas");
        }
        kafka.esperar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(nuevas))).all(),
                "aumentar las particiones del tópico '" + nombre + "'");
        // Ojo: con más particiones cambia a qué partición va cada clave (hash % particiones). Los eventos nuevos de una
        // reserva pueden caer en otra partición que los antiguos: el orden por reserva solo se garantiza hacia adelante.
        log.warn("Tópico '{}': particiones {} → {} (las claves existentes pueden cambiar de partición)", nombre, actuales, nuevas);
        return describirCuando(nombre, d -> d.partitions().size() == nuevas);
    }

    public void borrar(String nombre) {
        Nombres.topico(nombre);
        if (esProtegido(nombre)) {
            throw new ErrorApi(HttpStatus.CONFLICT, "El tópico '" + nombre
                    + "' está protegido: es parte del flujo de GymFlow o interno de Kafka y no se puede borrar");
        }
        kafka.esperar(admin.deleteTopics(List.of(nombre)).all(), "borrar el tópico '" + nombre + "'");
        log.warn("Tópico '{}' borrado", nombre);
    }

    boolean esProtegido(String nombre) {
        return protegidos.contains(nombre) || nombre.startsWith("__");
    }

    // ---- Conversión ----

    private TopicoResumen resumen(TopicDescription d, Config config) {
        return new TopicoResumen(d.name(), d.partitions().size(), factorReplicacion(d),
                valor(config, TopicConfig.CLEANUP_POLICY_CONFIG),
                numero(valor(config, TopicConfig.RETENTION_MS_CONFIG), Long::valueOf),
                numero(valor(config, TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), Integer::valueOf),
                d.isInternal(), esProtegido(d.name()));
    }

    private TopicoDetalle detalle(TopicDescription d, Config config) {
        List<ParticionResponse> particiones = d.partitions().stream()
                .sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
                .map(p -> new ParticionResponse(p.partition(), p.leader() == null ? null : p.leader().id(),
                        ids(p.replicas()), ids(p.isr()), p.isr().size() < p.replicas().size()))
                .toList();
        List<ConfigResponse> configs = config == null ? List.of() : config.entries().stream()
                .sorted(Comparator.comparing(ConfigEntry::name))
                .map(e -> new ConfigResponse(e.name(), e.isSensitive() ? null : e.value(), e.source().name(),
                        e.isDefault(), e.isReadOnly()))
                .toList();
        return new TopicoDetalle(d.name(), d.isInternal(), esProtegido(d.name()), factorReplicacion(d), particiones, configs);
    }

    /** Describe el tópico reintentando hasta que los metadatos muestren el cambio recién hecho (o se agoten los intentos). */
    private TopicoDetalle describirCuando(String nombre, Predicate<TopicoDetalle> listo) {
        ErrorApi ultimoError = null;
        TopicoDetalle ultimo = null;
        for (int intento = 0; intento < INTENTOS_METADATOS; intento++) {
            try {
                ultimo = describir(nombre);
                if (listo.test(ultimo)) {
                    return ultimo;
                }
            } catch (ErrorApi e) {
                if (e.getStatus() != HttpStatus.NOT_FOUND) {
                    throw e;
                }
                ultimoError = e;
            }
            pausa();
        }
        if (ultimo != null) {
            return ultimo;
        }
        throw ultimoError;
    }

    private static void pausa() {
        try {
            Thread.sleep(PAUSA_METADATOS_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ErrorApi(HttpStatus.SERVICE_UNAVAILABLE, "Se interrumpió la espera de los metadatos del tópico", e);
        }
    }

    private static int factorReplicacion(TopicDescription d) {
        return d.partitions().isEmpty() ? 0 : d.partitions().get(0).replicas().size();
    }

    private static List<Integer> ids(List<Node> nodos) {
        return nodos.stream().map(Node::id).toList();
    }

    private static String valor(Config config, String clave) {
        if (config == null) {
            return null;
        }
        ConfigEntry entrada = config.get(clave);
        return entrada == null ? null : entrada.value();
    }

    private static <T> T numero(String texto, java.util.function.Function<String, T> conversion) {
        try {
            return texto == null ? null : conversion.apply(texto);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static ConfigResource recurso(String nombre) {
        return new ConfigResource(ConfigResource.Type.TOPIC, nombre);
    }

    private static List<ConfigResource> recursos(Collection<String> nombres) {
        return nombres.stream().map(ServicioTopicos::recurso).toList();
    }
}
