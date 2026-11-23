package cl.duoc.gymflow.kafkaadmin.service;

import cl.duoc.gymflow.kafkaadmin.dto.Topicos;
import cl.duoc.gymflow.kafkaadmin.error.ErrorApi;
import org.springframework.http.HttpStatus;

/**
 * Validación de los nombres que llegan en la ruta ({@code /topics/{name}}, {@code /consumer-groups/{id}}): solo
 * {@code [a-zA-Z0-9._-]}, como exige Kafka para los tópicos. Se valida antes de llamar a Kafka para responder un 400
 * claro (y no mandar basura al clúster).
 */
final class Nombres {

    private static final int LARGO_MAXIMO = 249;

    private Nombres() {
    }

    static String topico(String nombre) {
        return validar(nombre, "tópico");
    }

    static String grupo(String id) {
        return validar(id, "grupo");
    }

    private static String validar(String valor, String que) {
        if (valor == null || valor.isBlank()) {
            throw new ErrorApi(HttpStatus.BAD_REQUEST, "El nombre del " + que + " es obligatorio");
        }
        if (valor.length() > LARGO_MAXIMO || !valor.matches(Topicos.PATRON_NOMBRE)) {
            throw new ErrorApi(HttpStatus.BAD_REQUEST, "El nombre del " + que + " '" + valor
                    + "' no es válido: solo letras, números, '.', '_' y '-' (máximo " + LARGO_MAXIMO + ")");
        }
        return valor;
    }
}
