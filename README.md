# Proyecto Maestría — SCADA (MQTT → Kafka → Flink → Postgres → Grafana)

Pipeline local para ingestión, procesamiento y monitoreo de variables SCADA de una turbina–generador hidroeléctrica con detección de anomalías en dos capas.

## Arquitectura

**Flujo de datos**

1) Teléfono/cliente MQTT publica JSON a HiveMQ (`1883`)
2) `mqtt_kafka_bridge` suscribe topics MQTT y publica a Kafka (topic `scada.turbina1.all`)
3) PyFlink consume Kafka, ejecuta **Capa 1** (umbrales) + **Capa 2** (Isolation Forest) y escribe a Postgres
4) Grafana consulta Postgres y muestra dashboard en tiempo real

**Servicios (docker-compose)**

- `hivemq`: broker MQTT (puerto `1883`)
- `mqtt_kafka_bridge`: puente MQTT → Kafka (`bridge/bridge.py`)
- `kafka`: broker Kafka en modo KRaft
- `jobmanager`, `taskmanager`: cluster Flink para ejecutar el job PyFlink
- `postgres`: base de datos para series temporales/tablas
- `grafana`: dashboard (datasource + dashboard provisionados)

**Estructura del código Flink**

```
flink/
├── Dockerfile
├── requirements.txt
├── artifacts/
│   ├── scaler.pkl          ← RobustScaler (entrenado offline)
│   ├── if_model.pkl        ← IsolationForest 400 árboles (entrenado offline)
│   └── model_meta.json     ← threshold, feature_names (49), metadata
├── sql/
│   ├── scada_turbina1_all.sql          ← CREATE TABLE completo
│   └── alter_add_ml_columns.sql        ← ALTER TABLE para tablas existentes
└── src/
    ├── job/
    │   └── scada_turbina_all.py        ← job principal (DataStream API)
    └── ml/
        ├── anomaly_detector.py         ← AnomalyDetector (singleton)
        ├── feature_builder.py          ← DimensionScorer + RollingFeatureBuffer
        └── scada_enricher.py           ← SCADAEnricher (MapFunction)
```

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
- `ts` puede venir en el payload, pero por defecto el job usa **tiempo de ingesta** (ver `TIME_MODE`).
- `asset` permite filtrar en Grafana (ej: `turbina1`).
- `rod_empuje` se usa como proxy de temperatura de aceite (lubricación).

## Cómo correr (local)

### 1. Colocar artefactos del modelo

```
flink/artifacts/scaler.pkl
flink/artifacts/if_model.pkl
flink/artifacts/model_meta.json   ← ya incluido (actualizar si cambian los features)
```

### 2. Build de la imagen Flink (solo la primera vez o si cambia requirements.txt)

```bash
docker compose build jobmanager
```

### 3. Levantar servicios

```bash
docker compose up -d hivemq kafka postgres grafana mqtt_kafka_bridge jobmanager taskmanager
```

### 4. Crear/actualizar tabla en Postgres

**Instalación nueva:**
```bash
cat flink/sql/scada_turbina1_all.sql | docker compose exec -T postgres psql -U postgres -d postgres
```

**Si ya tienes datos (agregar columnas nuevas):**
```bash
cat flink/sql/alter_add_ml_columns.sql | docker compose exec -T postgres psql -U postgres -d postgres
```

### 5. Ejecutar el job PyFlink

```bash
docker compose exec -T jobmanager env \
  SINK_MODE=jdbc TIME_MODE=ingest \
  KAFKA_STARTUP_MODE=latest-offset \
  POSTGRES_TABLE=scada_turbina1_all \
  KAFKA_GROUP_ID=pyflink-$(date +%s) \
  flink run -d -py /opt/src/job/scada_turbina_all.py
```

### 6. Verificar

```bash
# Job corriendo
docker compose exec -T jobmanager flink list -r

# Filas en Postgres (con IF después de 60 min)
docker compose exec -T postgres psql -U postgres -d postgres -c \
  "SELECT ts, anomaly_score, score_vib, if_score, if_anomaly FROM scada_turbina1_all ORDER BY ts DESC LIMIT 5;"
```

## Grafana

- UI: `http://localhost:3000` (o `http://<IP_LAN>:3000`)
- Datasource Postgres provisionado en `grafana/provisioning/datasources/postgres.yml`
- Dashboard SCADA provisionado en `grafana/dashboards/scada_turbina1_all.json`

El filtro de asset usa `LIKE`:
- All ⇒ `%`
- Asset específico ⇒ `turbina1`

## Detección de anomalías

### Capa 1 — Sub-scores por dimensión (umbrales, tiempo real)

El job calcula estas columnas en cada registro:

| Columna | Descripción | Señales |
|---|---|---|
| `score_vib` | Vibración | `max(vib_lo, vib_la)` |
| `score_bearing` | Cojinetes/Rodamientos | `max(coj_radial_lo, coj_radial_la, rod_empuje, rod_guia)` |
| `score_winding` | Devanados | `max(dev_u1..dev_w2)` |
| `score_cooling` | Enfriamiento | `aire_caliente - aire_frio` |
| `score_lubrication` | Lubricación (proxy aceite) | desviación de `rod_empuje` respecto a baseline |
| `anomaly_score` | Promedio de los 5 sub-scores | — |

Cada sub-score se normaliza a [0,1]: `clamp01((valor - WARN) / (ALARM - WARN))`

**Umbrales aplicados:**

| Dimensión | WARN | ALARM | Unidad |
|---|---|---|---|
| Vibración | 2.5 | 5.0 | mm/s RMS |
| Cojinetes | 70.0 | 85.0 | °C |
| Devanados | 80.0 | 95.0 | °C |
| Enfriamiento (ΔT) | 8.0 | 15.0 | °C |
| Lubricación (Δaceite) | 2.0 | 5.0 | °C |

### Capa 2 — Isolation Forest (modelo offline, rolling features)

| Columna | Descripción |
|---|---|
| `if_score` | Puntuación de anomalía del modelo (`score_samples()`). Rango típico: −1.0 a 0.0. **Más bajo = más anómalo.** `NULL` durante cold start. |
| `if_anomaly` | `1` si `if_score ≤ −0.7073` (umbral calibrado), `0` si no |

#### Cómo interpretar `if_score`

El Isolation Forest asigna una puntuación a cada muestra basada en cuán fácil es "aislarla" del resto del conjunto de datos. La lógica es la siguiente:

- **Puntuación cerca de 0** (ej: −0.45): la muestra es similar al comportamiento normal. El árbol necesita muchos cortes para aislarla.
- **Puntuación muy negativa** (ej: −0.85): la muestra es inusual. El árbol la aísla con pocos cortes, lo que indica comportamiento anómalo.
- **Umbral = −0.7073**: valor calibrado con los datos de entrenamiento (contamination=0.005, ~0.5% de anomalías esperadas).

A diferencia de la Capa 1 que evalúa cada señal por separado con umbrales fijos, el IF detecta **combinaciones inusuales** entre variables: por ejemplo, temperatura de devanados normal + vibración normal + ΔT de enfriamiento anormal simultáneamente.

#### Cold start

Las primeras **360 muestras** (~30 min a 5 s/muestra) retornan `if_score=NULL, if_anomaly=0`. El modelo necesita que el buffer rolling esté lleno para calcular estadísticas estables (medias y desviaciones sobre ventanas de 5, 15 y 60 minutos).

#### Features del modelo (49 total)

El modelo no recibe las 16 señales MQTT directamente. Primero se calculan **7 variables derivadas** que sintetizan el estado de la turbina, y luego se computan **7 estadísticos** sobre cada una usando un buffer rolling de 360 muestras.

**7 × 7 = 49 features**

**Variables base (señales derivadas):**

| Variable | Fórmula | Qué representa |
|---|---|---|
| `dev_promedio` | `mean(dev_u1..dev_w2)` | Temperatura media de devanados |
| `dev_desbalance` | `max(dev_*) - min(dev_*)` | Desbalance térmico entre fases |
| `delta_t_aire` | `aire_caliente - aire_frio` | Diferencial de temperatura del sistema de enfriamiento |
| `delta_coj_la_lo` | `coj_radial_la - coj_radial_lo` | Diferencial entre cojinetes lado abscissa y lado opuesto |
| `vib_promedio` | `mean(vib_lo, vib_la)` | Vibración media de la máquina |
| `delta_vib_la_lo` | `abs(vib_la - vib_lo)` | Desbalance vibracional entre extremos |
| `delta_aceite_empuje` | `rod_empuje - baseline` | Desviación de temperatura del rodamiento de empuje (proxy aceite) |

**7 estadísticos por variable:**

| Estadístico | Ventana | Descripción |
|---|---|---|
| `raw` | instantáneo | Valor actual sin suavizar |
| `mean_5m` | últimos 5 min (≈30 muestras) | Media a corto plazo |
| `mean_15m` | últimos 15 min (≈90 muestras) | Media a mediano plazo |
| `mean_60m` | últimos 60 min (≈360 muestras) | Media a largo plazo (baseline) |
| `std_5m` | últimos 5 min | Variabilidad a corto plazo |
| `std_15m` | últimos 15 min | Variabilidad a mediano plazo |
| `std_60m` | últimos 60 min | Variabilidad a largo plazo |

Ejemplo de los 7 features que genera `vib_promedio`:
```
vib_promedio_raw, vib_promedio_mean_5m, vib_promedio_mean_15m, vib_promedio_mean_60m,
vib_promedio_std_5m, vib_promedio_std_15m, vib_promedio_std_60m
```

**Artefactos** (entrenados con datos Jan 12–27, 2026):
- `scaler.pkl`: RobustScaler ajustado en 96,516 muestras
- `if_model.pkl`: IsolationForest, 400 árboles, contamination=0.005
- `model_meta.json`: threshold=−0.7073, 49 feature names

### Columnas legacy (compatibilidad Grafana)

- `deltaT_aire` = `aire_caliente - aire_frio` (°C)
- `dev_avg` = promedio de `dev_u1..dev_w2` (°C)
- `anomaly_score` = promedio de los 5 sub-scores (presente desde la primera muestra)

## Variables de entorno del job

| Variable | Default | Descripción |
|---|---|---|
| `SINK_MODE` | `print` | `print` \| `jdbc` \| `both` |
| `TIME_MODE` | `ingest` | `ingest` (tiempo de llegada) \| `payload` (usa `ts` del JSON) |
| `KAFKA_STARTUP_MODE` | `latest-offset` | `latest-offset` \| `earliest-offset` \| `group-offsets` |
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Bootstrap servers de Kafka |
| `KAFKA_TOPIC` | `scada.turbina1.all` | Topic fuente |
| `KAFKA_GROUP_ID` | `pyflink-scada` | Consumer group |
| `POSTGRES_TABLE` | `scada_turbina1_all` | Tabla destino |
| `ARTIFACTS_DIR` | `/opt/src/artifacts` | Ruta a los artefactos del modelo |

## Troubleshooting rápido

| Problema | Comando |
|---|---|
| No se ven datos en Postgres/Grafana | `docker compose exec -T jobmanager flink list -r` |
| Ver si llega a Kafka | `docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic scada.turbina1.all --from-beginning --timeout-ms 5000` |
| Ver logs del puente | `docker compose logs -f mqtt_kafka_bridge` |
| Ver logs del job Flink | `docker compose logs -f taskmanager` |
| `if_score` siempre NULL | Normal hasta que el buffer acumule 360 muestras (~30 min a 5 s/muestra) |
| Error cargando artefactos | Verificar que `flink/artifacts/scaler.pkl` y `if_model.pkl` existen |

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
