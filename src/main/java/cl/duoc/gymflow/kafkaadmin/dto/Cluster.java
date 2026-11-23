package cl.duoc.gymflow.kafkaadmin.dto;

import java.util.List;
import org.apache.kafka.common.Node;

/** Respuestas de {@code GET /api/kafka/cluster}. */
public final class Cluster {

    private Cluster() {
    }

    /** El clúster: su id, el broker controlador (el que asigna líderes de partición) y los brokers vivos. */
    public record ClusterResponse(String clusterId, BrokerResponse controller, List<BrokerResponse> brokers) {
    }

    public record BrokerResponse(int id, String host, int port, String rack) {

        public static BrokerResponse de(Node nodo) {
            return nodo == null ? null : new BrokerResponse(nodo.id(), nodo.host(), nodo.port(), nodo.rack());
        }
    }
}
