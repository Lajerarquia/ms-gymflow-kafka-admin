# ms-gymflow-kafka-admin

Administración del clúster Kafka de GymFlow (EP5) con el `AdminClient` de Kafka: tópicos, sus configuraciones y los
consumer groups (lag y reset de offsets). Sin base de datos y **sin puertos publicados**: en `ec2-apps` solo lo alcanza
ms-gymflow-bff por la red interna de Docker, que lo expone como `/api/admin/kafka/**` (solo rol Admin, mismo filtro
JWT). Por eso este servicio no valida JWT.

Complementa al script `infra/kafka/scripts/crear-topicos.sh`: el script deja el clúster listo de forma replicable; esta
API permite inspeccionarlo y mantenerlo desde la pantalla `/admin/kafka` del frontend.

## API

| Método y ruta | Descripción |
|---|---|
| `GET /api/kafka/cluster` | Id del clúster, broker controlador y brokers vivos |
| `GET /api/kafka/topics?internal=false` | Tópicos con particiones, réplicas, `cleanup.policy`, `retention.ms`, `min.insync.replicas` y si están protegidos |
| `GET /api/kafka/topics/{name}` | Cada partición con líder, réplicas, ISR y `underReplicated`; todas las configuraciones con su origen |
| `POST /api/kafka/topics` | Crea un tópico (201) |
| `PUT /api/kafka/topics/{name}/configs` | Cambia configuraciones con `incrementalAlterConfigs` (solo las indicadas) |
| `POST /api/kafka/topics/{name}/partitions` | Aumenta las particiones (solo hacia arriba) |
| `DELETE /api/kafka/topics/{name}` | Borra un tópico (204). `reservations.events`, `audit.timeline` y los internos `__*` → 409 |
| `GET /api/kafka/consumer-groups` | Grupos con estado, cantidad de miembros, tópicos y lag total |
| `GET /api/kafka/consumer-groups/{id}` | Miembros con sus particiones asignadas y, por partición, offset confirmado, offset final y lag |
| `POST /api/kafka/consumer-groups/{id}/reset-offsets` | Mueve los offsets (`earliest`, `latest` o `timestamp`). 409 si el grupo tiene consumidores activos |
| `DELETE /api/kafka/consumer-groups/{id}` | Borra el grupo (204). 409 si tiene consumidores activos |

Ejemplos:
```json
POST /api/kafka/topics
{"name": "pruebas.demo", "partitions": 3, "replicationFactor": 3,
 "configs": {"retention.ms": "86400000", "cleanup.policy": "delete", "min.insync.replicas": "2"}}

PUT /api/kafka/topics/reservations.events/configs
{"configs": {"retention.ms": "259200000"}}          // null en un valor = quitarlo (vuelve al default del broker)

POST /api/kafka/topics/pruebas.demo/partitions
{"partitions": 6}

POST /api/kafka/consumer-groups/gymflow-report/reset-offsets
{"strategy": "timestamp", "timestamp": "2026-11-20T00:00:00Z", "topic": "reservations.events"}
```

Errores en el JSON común `{"timestamp","status","error","message","path"}`, con mensajes en español:

| Código | Cuándo |
|---|---|
| 400 | Nombre fuera de `[a-zA-Z0-9._-]+`; particiones o réplicas fuera de rango; réplicas > brokers vivos; nombre `__*`; reducir particiones; configuración que Kafka rechaza (nombre desconocido o valor inválido) |
| 404 | El tópico o el grupo no existe |
| 409 | El tópico ya existe; borrar un tópico protegido; reset o borrado de un grupo con consumidores activos |
| 503 | Kafka no respondió en `gymflow.kafka-admin.timeout` (10 s) |

## Decisiones

- **Un solo punto de traducción de errores** (`kafka/ErroresKafka`): cada llamada al `AdminClient` devuelve un
  `KafkaFuture`; se espera con timeout y la excepción de Kafka (`UnknownTopicOrPartitionException`,
  `TopicExistsException`, `GroupNotEmptyException`, `InvalidConfigurationException`...) se convierte en el código HTTP
  y el mensaje. Así ninguna llamada queda colgada y el controlador no conoce Kafka.
- **Tópicos protegidos.** `reservations.events` y `audit.timeline` son el flujo de negocio: borrarlos perdería eventos
  aún no consumidos. Sí se puede cambiar su retención (la pantalla de Kafka lo usa para la demo).
- **Reset de offsets solo con el grupo detenido.** Si hay consumidores activos, el próximo commit de ellos pisaría el
  reset; Kafka además lo rechaza. Para reprocesar: detener report o audit, resetear a `earliest` y volver a levantarlos.
  Como report y audit son idempotentes por `eventId`, reprocesar no duplica datos.
- **Aumentar particiones** cambia a qué partición va cada clave (`hash(clave) % particiones`): el orden por reserva
  solo se garantiza para los eventos nuevos. Por eso el servicio lo registra como advertencia.
- **Validación propia antes que la de Kafka** (nombres, réplicas vs. brokers vivos, reducir particiones): da mensajes
  claros en español en vez del texto del broker. Lo demás (configuraciones) lo valida Kafka y se traduce a 400.
- **Sin health de Kafka**: si Kafka está caído el servicio arranca y responde 503 en cada llamada, pero no queda unhealthy.

## Ejecutar

```bash
# Con infra/kafka levantado en local (brokers en localhost:9092-9094):
./mvnw spring-boot:run                       # puerto 8087
curl -s localhost:8087/api/kafka/topics
curl -s localhost:8087/api/kafka/consumer-groups/gymflow-report
```

En AWS: `KAFKA_BOOTSTRAP_SERVERS=<ip-privada-ec2-kafka>:9092,...:9093,...:9094` (ver `.env.example`) y se usa a través
del BFF: `GET https://<api-gateway>/api/admin/kafka/topics` con el token de un Admin.

## Pruebas

`./mvnw clean test`: 14 pruebas con `@EmbeddedKafka` (3 brokers en memoria, `auto.create.topics.enable=false`, sin
Docker) y MockMvc:

- `TopicosTest`: clúster, crear/listar/describir con sus configs, 409 al repetir, validaciones 400 (nombre, réplicas,
  `__*`, config desconocida, JSON inválido), alterar y quitar configs, aumentar particiones, protegidos 409, 404.
- `GruposTest`: con un consumidor real, miembros, lag por partición y total; reset y borrado con el grupo activo → 409;
  con el grupo vacío, reset `earliest`, `latest` (con `topic`) y `timestamp`, y borrado; validaciones.
- `KafkaCaidoTest`: sin brokers, el health sigue UP y la API responde 503 en el tiempo configurado.

`TopicosTest.borrarTopico_loElimina` se omite en Windows: el broker embebido no puede renombrar los archivos abiertos
del tópico borrado y se apaga (bug conocido de Kafka en Windows, KAFKA-1194). Para correr las 14 en Linux:

```bash
docker run --rm -v "$PWD:/src" -w /src maven:3.9-eclipse-temurin-17 mvn -q test
```
