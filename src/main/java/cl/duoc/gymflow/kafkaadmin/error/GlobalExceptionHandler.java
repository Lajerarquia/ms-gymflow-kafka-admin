package cl.duoc.gymflow.kafkaadmin.error;

import jakarta.servlet.http.HttpServletRequest;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/** Todas las respuestas de error en el JSON común {@code {"timestamp","status","error","message","path"}}. */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ErrorApi.class)
    public ResponseEntity<ErrorResponse> errorApi(ErrorApi ex, HttpServletRequest req) {
        if (ex.getStatus().is5xxServerError()) {
            log.error("{} {}: {}", req.getMethod(), req.getRequestURI(), ex.getMessage(), ex.getCause());
        } else {
            log.info("{} {} → {}: {}", req.getMethod(), req.getRequestURI(), ex.getStatus().value(), ex.getMessage());
        }
        return responder(ex.getStatus(), ex.getMessage(), req);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> validacion(MethodArgumentNotValidException ex, HttpServletRequest req) {
        String mensaje = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getDefaultMessage())
                .sorted()
                .collect(Collectors.joining("; "));
        return responder(HttpStatus.BAD_REQUEST, mensaje, req);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> cuerpoInvalido(HttpMessageNotReadableException ex, HttpServletRequest req) {
        return responder(HttpStatus.BAD_REQUEST, "El cuerpo de la petición no es un JSON válido", req);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> parametroInvalido(MethodArgumentTypeMismatchException ex, HttpServletRequest req) {
        return responder(HttpStatus.BAD_REQUEST, "El parámetro '" + ex.getName() + "' no es válido", req);
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> metodoNoSoportado(HttpRequestMethodNotSupportedException ex, HttpServletRequest req) {
        return responder(HttpStatus.METHOD_NOT_ALLOWED, "Método " + ex.getMethod() + " no permitido en esta ruta", req);
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ErrorResponse> rutaInexistente(NoResourceFoundException ex, HttpServletRequest req) {
        return responder(HttpStatus.NOT_FOUND, "La ruta no existe", req);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> inesperado(Exception ex, HttpServletRequest req) {
        log.error("Error no controlado en {} {}", req.getMethod(), req.getRequestURI(), ex);
        return responder(HttpStatus.INTERNAL_SERVER_ERROR, "Error interno del administrador de Kafka", req);
    }

    private static ResponseEntity<ErrorResponse> responder(HttpStatus status, String mensaje, HttpServletRequest req) {
        return ResponseEntity.status(status).body(ErrorResponse.of(status, mensaje, req.getRequestURI()));
    }
}
