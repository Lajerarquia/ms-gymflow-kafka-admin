package cl.duoc.gymflow.kafkaadmin.controller;

import cl.duoc.gymflow.kafkaadmin.dto.Cluster.ClusterResponse;
import cl.duoc.gymflow.kafkaadmin.dto.Grupos.GrupoDetalle;
import cl.duoc.gymflow.kafkaadmin.dto.Grupos.GrupoResumen;
import cl.duoc.gymflow.kafkaadmin.dto.Grupos.ResetOffsetsRequest;
import cl.duoc.gymflow.kafkaadmin.dto.Grupos.ResetOffsetsResponse;
import cl.duoc.gymflow.kafkaadmin.dto.Topicos.ActualizarConfigsRequest;
import cl.duoc.gymflow.kafkaadmin.dto.Topicos.AumentarParticionesRequest;
import cl.duoc.gymflow.kafkaadmin.dto.Topicos.CrearTopicoRequest;
import cl.duoc.gymflow.kafkaadmin.dto.Topicos.TopicoDetalle;
import cl.duoc.gymflow.kafkaadmin.dto.Topicos.TopicoResumen;
import cl.duoc.gymflow.kafkaadmin.service.ServicioCluster;
import cl.duoc.gymflow.kafkaadmin.service.ServicioGrupos;
import cl.duoc.gymflow.kafkaadmin.service.ServicioTopicos;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Administración de Kafka para GymFlow. <b>No se expone a internet</b>: no publica puertos y solo lo llama
 * ms-gymflow-bff por la red interna de Docker ({@code /api/admin/kafka/**}, que ya validó el JWT y exige el rol Admin).
 * Por eso aquí no se valida JWT. El controlador solo recibe y delega: la lógica está en los servicios.
 */
@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final ServicioCluster cluster;
    private final ServicioTopicos topicos;
    private final ServicioGrupos grupos;

    public KafkaAdminController(ServicioCluster cluster, ServicioTopicos topicos, ServicioGrupos grupos) {
        this.cluster = cluster;
        this.topicos = topicos;
        this.grupos = grupos;
    }

    @GetMapping("/cluster")
    public ClusterResponse cluster() {
        return cluster.describir();
    }

    // ---- Tópicos ----

    /** {@code ?internal=true} incluye los internos de Kafka ({@code __consumer_offsets}). */
    @GetMapping("/topics")
    public List<TopicoResumen> topicos(@RequestParam(name = "internal", defaultValue = "false") boolean internos) {
        return topicos.listar(internos);
    }

    @GetMapping("/topics/{name}")
    public TopicoDetalle topico(@PathVariable String name) {
        return topicos.describir(name);
    }

    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public TopicoDetalle crearTopico(@Valid @RequestBody CrearTopicoRequest datos) {
        return topicos.crear(datos);
    }

    @PutMapping("/topics/{name}/configs")
    public TopicoDetalle actualizarConfigs(@PathVariable String name, @Valid @RequestBody ActualizarConfigsRequest datos) {
        return topicos.actualizarConfigs(name, datos);
    }

    @PostMapping("/topics/{name}/partitions")
    public TopicoDetalle aumentarParticiones(@PathVariable String name,
                                             @Valid @RequestBody AumentarParticionesRequest datos) {
        return topicos.aumentarParticiones(name, datos);
    }

    @DeleteMapping("/topics/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void borrarTopico(@PathVariable String name) {
        topicos.borrar(name);
    }

    // ---- Consumer groups ----

    @GetMapping("/consumer-groups")
    public List<GrupoResumen> grupos() {
        return grupos.listar();
    }

    @GetMapping("/consumer-groups/{id}")
    public GrupoDetalle grupo(@PathVariable String id) {
        return grupos.detalle(id);
    }

    @PostMapping("/consumer-groups/{id}/reset-offsets")
    public ResetOffsetsResponse resetearOffsets(@PathVariable String id, @Valid @RequestBody ResetOffsetsRequest datos) {
        return grupos.resetearOffsets(id, datos);
    }

    @DeleteMapping("/consumer-groups/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void borrarGrupo(@PathVariable String id) {
        grupos.borrar(id);
    }
}
