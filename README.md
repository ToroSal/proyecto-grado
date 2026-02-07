# Proyecto Maestría — SCADA (MQTT → Kafka → Flink → Postgres → Grafana)

Pipeline local para ingestión, procesamiento y monitoreo de variables SCADA de una turbina–generador.

## Arquitectura

**Flujo de datos**

1) Teléfono/cliente MQTT publica JSON a HiveMQ (`1883`)  
2) `mqtt_kafka_bridge` suscribe topics MQTT y publica a Kafka (topic `scada.turbina1.all`)  
3) PyFlink consume Kafka, enriquece/calcula features y escribe a Postgres  
4) Grafana consulta Postgres y muestra dashboard en tiempo real

**Servicios (docker-compose)**

- `hivemq`: broker MQTT (puerto `1883`)
- `mqtt_kafka_bridge`: puente MQTT → Kafka (`bridge/bridge.py`)
- `kafka`: broker Kafka en modo KRaft
- `jobmanager`, `taskmanager`: cluster Flink para ejecutar el job PyFlink
- `postgres`: base de datos para series temporales/tablas
- `grafana`: dashboard (datasource + dashboard provisionados)

## Topics y payload MQTT

**Topics soportados**

- `planta1/turbina/all` (recomendado)
- `planta1/turbina1/all`

**Payload ejemplo (JSON)**

```json
{
  "ts": "2026-01-20T13:45:00.000Z",
  "asset": "turbina1",
  "vib_lo": 0.55,
  "vib_la": 0.77,
  "pres_turbina": 11.8,
  "pres_tuberia": 7.28,
  "coj_radial_lo": 35.5,
  "coj_radial_la": 41.2,
  "rod_empuje": 56.5,
  "rod_guia": 46.7,
  "dev_u1": 68.6,
  "dev_u2": 66.7,
  "dev_v1": 67.3,
  "dev_v2": 70.2,
  "dev_w1": 64.9,
  "dev_w2": 68.3,
  "aire_frio": 30.7,
  "aire_caliente": 41.7
}
```

Notas:
- `ts` puede venir en el payload, pero por defecto el job usa **tiempo de ingesta** para `ts` en la base de datos (ver `TIME_MODE`).
- `asset` permite filtrar en Grafana (ej: `turbina1`).

## Cómo correr (local)

Levantar servicios:

`docker compose up -d hivemq kafka postgres grafana mqtt_kafka_bridge jobmanager taskmanager`

Crear/actualizar tabla en Postgres (incluye columnas derivadas):

`cat flink/sql/scada_turbina1_all.sql | docker compose exec -T postgres psql -U postgres -d postgres`

Tip: este SQL también crea índices para acelerar los paneles de Grafana (filtro por `asset` + rango de tiempo).

Ejecutar el job PyFlink (consume Kafka y escribe a Postgres):

`docker compose exec -T jobmanager env SINK_MODE=jdbc TIME_MODE=ingest KAFKA_STARTUP_MODE=latest-offset POSTGRES_TABLE=scada_turbina1_all KAFKA_GROUP_ID=pyflink-$(date +%s) flink run -d -py /opt/src/job/scada_turbina_all.py`

Verificar que está corriendo:

`docker compose exec -T jobmanager flink list -r`

Verificar que llegan filas a Postgres:

`docker compose exec -T postgres psql -U postgres -d postgres -c "select now(), max(ts), count(*) from scada_turbina1_all;"`

## Grafana

- UI: `http://localhost:3000` (o `http://<IP_LAN>:3000`)
- Datasource Postgres provisionado en `grafana/provisioning/datasources/postgres.yml`
- Dashboard SCADA provisionado en `grafana/dashboards/scada_turbina1_all.json`

El filtro de asset usa `LIKE`:
- All ⇒ `%`
- Asset específico ⇒ `turbina1`

## Features derivadas y anomalías

El job PyFlink calcula y guarda estas columnas en Postgres:

- `deltaT_aire` = `aire_caliente - aire_frio` (°C)
- `dev_avg` = promedio de `dev_u1..dev_w2` (°C) ignorando nulos
- `anomaly_score` (0–1): score agregado (promedio) de 5 sub-scores normalizados a [0,1]:
  - vibración: `max(vib_lo, vib_la)`
  - cojinetes/rodamientos: `max(coj_radial_lo, coj_radial_la, rod_empuje, rod_guia)`
  - devanados: `max(dev_u1..dev_w2)`
  - enfriamiento: `deltaT_aire`
  - presión diferencial: `abs(pres_turbina - pres_tuberia)`

Cada sub-score usa umbrales `WARN`/`ALARM` y se normaliza como:
`clamp01((valor - WARN) / (ALARM - WARN))`

**Umbrales configurables (env vars del job)**

- `VIB_WARN`, `VIB_ALARM` (mm/s RMS)
- `BEARING_WARN`, `BEARING_ALARM` (°C)
- `WINDING_WARN`, `WINDING_ALARM` (°C)
- `DELTA_T_WARN`, `DELTA_T_ALARM` (°C)
- `PRES_DIFF_WARN`, `PRES_DIFF_ALARM` (bar)

## Modos importantes (job)

- `TIME_MODE=ingest|payload`  
  - `ingest` (default): `ts` en Postgres = tiempo de llegada (mejor para “últimos 5 min” en Grafana)
  - `payload`: usa `ts` del JSON (si viene correcto)
- `KAFKA_STARTUP_MODE=latest-offset|earliest-offset|group-offsets`
  - `latest-offset` (default): solo datos nuevos

## Troubleshooting rápido

- No se ven datos en Postgres/Grafana → el job no está corriendo: `docker compose exec -T jobmanager flink list -r`
- Ver si llega a Kafka: `docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic scada.turbina1.all --from-beginning --timeout-ms 5000`
- Ver logs del puente: `docker compose logs -f mqtt_kafka_bridge`

## Correr en GCP (Ubuntu, e2-standard-2)

Para un POC en una VM con **8 GB RAM**, este repo ya trae límites de memoria y una configuración más conservadora de Flink en `docker-compose.yml`.

1) Crea la VM en `us-central1` con **100 GB** de disco (ideal: `pd-balanced`).

2) En Ubuntu, agrega swap (recomendado, 4–8 GB):

```bash
sudo fallocate -l 8G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
free -h
```

3) Levanta el stack respetando límites (`deploy.resources`) con modo compatibilidad:

```bash
docker compose --compatibility up -d
docker stats
```

Notas:
- Si quieres acceder a Kafka desde fuera de la VM, ajusta `KAFKA_ADVERTISED_LISTENERS` (ahora está en `localhost`).
- No expongas `5432`, `9092`, `8081` públicamente; limita los puertos con firewall/VPC (ideal: solo `1883`, `3000`, `8080`).
