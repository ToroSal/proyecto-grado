"""
PyFlink job — SCADA Turbina (Kafka -> enrich -> PostgreSQL/print).

Usa DataStream API con SCADAEnricher (MapFunction) para:
  - Capa 1: sub-scores por dimension (umbrales)
  - Capa 2: Isolation Forest (modelo offline)
"""
from __future__ import annotations

import json
import logging
import os
import sys

# Asegurar que /opt/src esta en el path para imports de ml.*
sys.path.insert(0, "/opt/src")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.table import StreamTableEnvironment

from ml.scada_enricher import SCADAEnricher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ===================================================================
# Configuracion (env vars)
# ===================================================================
def _env(name, default):
    # type: (str, str) -> str
    value = os.getenv(name)
    return value if value not in (None, "") else default


KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = _env("KAFKA_TOPIC", "scada.turbina1.all")
KAFKA_GROUP_ID = _env("KAFKA_GROUP_ID", "pyflink-scada")
KAFKA_STARTUP_MODE = _env("KAFKA_STARTUP_MODE", "latest-offset")

POSTGRES_URL = _env("POSTGRES_URL", "jdbc:postgresql://postgres:5432/postgres")
POSTGRES_USER = _env("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = _env("POSTGRES_PASSWORD", "postgres")
POSTGRES_TABLE = _env("POSTGRES_TABLE", "scada_turbina1_all")

SINK_MODE = _env("SINK_MODE", "print")   # print | jdbc | both
TIME_MODE = _env("TIME_MODE", "ingest")  # ingest | payload


# ===================================================================
# Helpers
# ===================================================================
class _ExtractString(MapFunction):
    """Extrae el string de un Row (resultado de to_data_stream)."""
    def map(self, value):
        return str(value[0]) if value[0] is not None else "{}"


# ===================================================================
# PostgresSinkFunction — escribe registros JSON a Postgres via psycopg2
# ===================================================================
_PG_COLUMNS = [
    "ts", "asset",
    "vib_lo", "vib_la", "pres_turbina", "pres_tuberia",
    "coj_radial_lo", "coj_radial_la", "rod_empuje", "rod_guia",
    "dev_u1", "dev_u2", "dev_v1", "dev_v2", "dev_w1", "dev_w2",
    "aire_frio", "aire_caliente",
    "deltaT_aire", "dev_avg", "anomaly_score",
    "score_vib", "score_bearing", "score_winding",
    "score_cooling", "score_lubrication",
    "if_score", "if_anomaly",
]


class PostgresWriterMap(MapFunction):
    """
    MapFunction que escribe a PostgreSQL via psycopg2 y retorna el JSON sin
    modificar. En PyFlink 1.16, SinkFunction es un wrapper Java — los sinks
    Python custom se implementan como MapFunction con efecto colateral.
    """

    def __init__(self, jdbc_url, user, password, table):
        # type: (str, str, str, str) -> None
        self._jdbc_url = jdbc_url
        self._user = user
        self._password = password
        self._table = table
        self._conn = None
        self._insert_sql = None

    def open(self, runtime_context):
        import psycopg2

        # Parsear JDBC URL: jdbc:postgresql://host:port/db
        url = self._jdbc_url.replace("jdbc:postgresql://", "")
        host_port, db = url.split("/", 1)
        if ":" in host_port:
            host, port = host_port.split(":")
        else:
            host, port = host_port, "5432"

        self._conn = psycopg2.connect(
            host=host,
            port=int(port),
            dbname=db,
            user=self._user,
            password=self._password,
        )
        self._conn.autocommit = True

        cols = ", ".join(_PG_COLUMNS)
        placeholders = ", ".join(["%s"] * len(_PG_COLUMNS))
        self._insert_sql = "INSERT INTO %s (%s) VALUES (%s)" % (
            self._table, cols, placeholders
        )
        logger.info("PostgresWriterMap abierto — tabla=%s", self._table)

    def map(self, value):
        # type: (str) -> str
        try:
            row = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            logger.warning("PostgresWriterMap: JSON invalido, descartando")
            return value

        values = [row.get(col) for col in _PG_COLUMNS]

        try:
            cursor = self._conn.cursor()
            cursor.execute(self._insert_sql, values)
            cursor.close()
        except Exception:
            logger.exception("PostgresWriterMap: error insertando fila")

        return value

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("PostgresWriterMap cerrado")


# ===================================================================
# Main
# ===================================================================
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # --- Kafka source (formato raw -> un solo campo STRING) ---
    t_env.execute_sql(
        """
        CREATE TABLE kafka_raw (
            `value` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '%s',
            'properties.bootstrap.servers' = '%s',
            'properties.group.id' = '%s',
            'scan.startup.mode' = '%s',
            'format' = 'raw'
        )
        """
        % (KAFKA_TOPIC, KAFKA_BOOTSTRAP, KAFKA_GROUP_ID, KAFKA_STARTUP_MODE)
    )

    # --- Table -> DataStream<Row> -> DataStream<String> ---
    raw_table = t_env.from_path("kafka_raw")
    raw_ds = t_env.to_data_stream(raw_table)

    json_ds = raw_ds.map(_ExtractString(), output_type=Types.STRING())

    # --- Enrichment (Capa 1 + Capa 2) ---
    enriched_ds = json_ds.map(SCADAEnricher(), output_type=Types.STRING())

    # --- Sinks ---
    # En PyFlink 1.16 Python, los sinks custom son MapFunction con efecto
    # colateral. Cada rama del DAG necesita un terminal (.print()).
    if SINK_MODE == "print":
        enriched_ds.print()

    elif SINK_MODE == "jdbc":
        pg_writer = PostgresWriterMap(
            POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_TABLE
        )
        # .print() actua como terminal del DAG; la escritura real ocurre en map()
        enriched_ds.map(pg_writer, output_type=Types.STRING()).print()
        logger.info("Sink: JDBC habilitado -> %s", POSTGRES_TABLE)

    elif SINK_MODE == "both":
        enriched_ds.print()
        pg_writer = PostgresWriterMap(
            POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_TABLE
        )
        enriched_ds.map(pg_writer, output_type=Types.STRING()).print()
        logger.info("Sink: print + JDBC habilitados")

    logger.info(
        "Iniciando job SCADA — topic=%s sink=%s time=%s",
        KAFKA_TOPIC, SINK_MODE, TIME_MODE,
    )
    env.execute("SCADA Turbine Enrichment Job")


if __name__ == "__main__":
    main()
