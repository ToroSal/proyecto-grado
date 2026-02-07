import os

from pyflink.table import EnvironmentSettings, TableEnvironment


def env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = env("KAFKA_TOPIC", "scada.turbina1.all")
KAFKA_GROUP_ID = env("KAFKA_GROUP_ID", "pyflink-scada")
KAFKA_STARTUP_MODE = env("KAFKA_STARTUP_MODE", "latest-offset")  # earliest-offset|latest-offset|group-offsets

POSTGRES_URL = env("POSTGRES_URL", "jdbc:postgresql://postgres:5432/postgres")
POSTGRES_USER = env("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD", "postgres")
POSTGRES_TABLE = env("POSTGRES_TABLE", "scada_turbina1_all")

SINK_MODE = env("SINK_MODE", "print")  # print|jdbc|both
TIME_MODE = env("TIME_MODE", "ingest")  # ingest|payload

TS_EXPR = "ts_ingest" if TIME_MODE == "ingest" else "ts_payload"

# Thresholds (defaults based on typical ops; tune via env vars)
VIB_WARN = float(env("VIB_WARN", "2.8"))  # mm/s RMS
VIB_ALARM = float(env("VIB_ALARM", "4.5"))
BEARING_WARN = float(env("BEARING_WARN", "70"))  # °C
BEARING_ALARM = float(env("BEARING_ALARM", "85"))
WINDING_WARN = float(env("WINDING_WARN", "105"))  # °C
WINDING_ALARM = float(env("WINDING_ALARM", "120"))
DELTA_T_WARN = float(env("DELTA_T_WARN", "15"))  # °C (aire_caliente - aire_frio)
DELTA_T_ALARM = float(env("DELTA_T_ALARM", "25"))
PRES_DIFF_WARN = float(env("PRES_DIFF_WARN", "3"))  # bar (|pres_turbina - pres_tuberia|)
PRES_DIFF_ALARM = float(env("PRES_DIFF_ALARM", "6"))

SELECT_COLUMNS = (
    f"{TS_EXPR} AS ts, asset, vib_lo, vib_la, pres_turbina, pres_tuberia, "
    "coj_radial_lo, coj_radial_la, rod_empuje, rod_guia, "
    "dev_u1, dev_u2, dev_v1, dev_v2, dev_w1, dev_w2, "
    "aire_frio, aire_caliente, deltaT_aire, dev_avg, anomaly_score"
)


def main():
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(settings)

    table_env.execute_sql(
        f"""
        CREATE TABLE scada_source (
          ts STRING,
          ts_payload AS COALESCE(
            TO_TIMESTAMP(REPLACE(ts, 'Z', ''), 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
            TO_TIMESTAMP(REPLACE(ts, 'Z', ''), 'yyyy-MM-dd''T''HH:mm:ss')
          ),
          ts_ingest AS LOCALTIMESTAMP,
          asset STRING,
          vib_lo DOUBLE,
          vib_la DOUBLE,
          pres_turbina DOUBLE,
          pres_tuberia DOUBLE,
          coj_radial_lo DOUBLE,
          coj_radial_la DOUBLE,
          rod_empuje DOUBLE,
          rod_guia DOUBLE,
          dev_u1 DOUBLE,
          dev_u2 DOUBLE,
          dev_v1 DOUBLE,
          dev_v2 DOUBLE,
          dev_w1 DOUBLE,
          dev_w2 DOUBLE,
          aire_frio DOUBLE,
          aire_caliente DOUBLE,
          deltaT_aire AS (aire_caliente - aire_frio),
          dev_avg AS (
            (
              COALESCE(dev_u1, 0) + COALESCE(dev_u2, 0) +
              COALESCE(dev_v1, 0) + COALESCE(dev_v2, 0) +
              COALESCE(dev_w1, 0) + COALESCE(dev_w2, 0)
            ) / NULLIF(
              (CASE WHEN dev_u1 IS NULL THEN 0 ELSE 1 END) +
              (CASE WHEN dev_u2 IS NULL THEN 0 ELSE 1 END) +
              (CASE WHEN dev_v1 IS NULL THEN 0 ELSE 1 END) +
              (CASE WHEN dev_v2 IS NULL THEN 0 ELSE 1 END) +
              (CASE WHEN dev_w1 IS NULL THEN 0 ELSE 1 END) +
              (CASE WHEN dev_w2 IS NULL THEN 0 ELSE 1 END),
              0
            )
          ),
          anomaly_score AS (
            (
              -- Vib score
              LEAST(
                1.0,
                GREATEST(
                  0.0,
                  (GREATEST(COALESCE(vib_lo, 0), COALESCE(vib_la, 0)) - {VIB_WARN}) / ({VIB_ALARM} - {VIB_WARN})
                )
              )
              +
              -- Bearing score
              LEAST(
                1.0,
                GREATEST(
                  0.0,
                  (GREATEST(COALESCE(coj_radial_lo, 0), COALESCE(coj_radial_la, 0), COALESCE(rod_empuje, 0), COALESCE(rod_guia, 0)) - {BEARING_WARN}) / ({BEARING_ALARM} - {BEARING_WARN})
                )
              )
              +
              -- Winding score
              LEAST(
                1.0,
                GREATEST(
                  0.0,
                  (GREATEST(COALESCE(dev_u1, 0), COALESCE(dev_u2, 0), COALESCE(dev_v1, 0), COALESCE(dev_v2, 0), COALESCE(dev_w1, 0), COALESCE(dev_w2, 0)) - {WINDING_WARN}) / ({WINDING_ALARM} - {WINDING_WARN})
                )
              )
              +
              -- Cooling score (deltaT)
              LEAST(
                1.0,
                GREATEST(
                  0.0,
                  (COALESCE(aire_caliente - aire_frio, 0) - {DELTA_T_WARN}) / ({DELTA_T_ALARM} - {DELTA_T_WARN})
                )
              )
              +
              -- Pressure differential score
              LEAST(
                1.0,
                GREATEST(
                  0.0,
                  (ABS(COALESCE(pres_turbina, 0) - COALESCE(pres_tuberia, 0)) - {PRES_DIFF_WARN}) / ({PRES_DIFF_ALARM} - {PRES_DIFF_WARN})
                )
              )
            ) / 5.0
          )
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{KAFKA_TOPIC}',
          'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
          'properties.group.id' = '{KAFKA_GROUP_ID}',
          'scan.startup.mode' = '{KAFKA_STARTUP_MODE}',
          'format' = 'json',
          'json.ignore-parse-errors' = 'true',
          'json.timestamp-format.standard' = 'ISO-8601'
        )
        """
    )

    statement_set = table_env.create_statement_set()

    if SINK_MODE in ("print", "both"):
        table_env.execute_sql(
            """
            CREATE TABLE scada_print (
              ts TIMESTAMP(3),
              asset STRING,
              vib_lo DOUBLE,
              vib_la DOUBLE,
              pres_turbina DOUBLE,
              pres_tuberia DOUBLE,
              coj_radial_lo DOUBLE,
              coj_radial_la DOUBLE,
              rod_empuje DOUBLE,
              rod_guia DOUBLE,
              dev_u1 DOUBLE,
              dev_u2 DOUBLE,
              dev_v1 DOUBLE,
              dev_v2 DOUBLE,
              dev_w1 DOUBLE,
              dev_w2 DOUBLE,
              aire_frio DOUBLE,
              aire_caliente DOUBLE,
              deltaT_aire DOUBLE,
              dev_avg DOUBLE,
              anomaly_score DOUBLE
            ) WITH (
              'connector' = 'print'
            )
            """
        )
        statement_set.add_insert_sql(
            f"INSERT INTO scada_print SELECT {SELECT_COLUMNS} FROM scada_source"
        )

    if SINK_MODE in ("jdbc", "both"):
        table_env.execute_sql(
            f"""
            CREATE TABLE scada_pg (
              ts TIMESTAMP(3),
              asset STRING,
              vib_lo DOUBLE,
              vib_la DOUBLE,
              pres_turbina DOUBLE,
              pres_tuberia DOUBLE,
              coj_radial_lo DOUBLE,
              coj_radial_la DOUBLE,
              rod_empuje DOUBLE,
              rod_guia DOUBLE,
              dev_u1 DOUBLE,
              dev_u2 DOUBLE,
              dev_v1 DOUBLE,
              dev_v2 DOUBLE,
              dev_w1 DOUBLE,
              dev_w2 DOUBLE,
              aire_frio DOUBLE,
              aire_caliente DOUBLE,
              deltaT_aire DOUBLE,
              dev_avg DOUBLE,
              anomaly_score DOUBLE
            ) WITH (
              'connector' = 'jdbc',
              'url' = '{POSTGRES_URL}',
              'table-name' = '{POSTGRES_TABLE}',
              'username' = '{POSTGRES_USER}',
              'password' = '{POSTGRES_PASSWORD}',
              'sink.buffer-flush.max-rows' = '1',
              'sink.buffer-flush.interval' = '0s'
            )
            """
        )
        statement_set.add_insert_sql(
            f"INSERT INTO scada_pg SELECT {SELECT_COLUMNS} FROM scada_source"
        )

    statement_set.execute()


if __name__ == "__main__":
    main()
