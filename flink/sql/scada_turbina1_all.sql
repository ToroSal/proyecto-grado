CREATE TABLE IF NOT EXISTS scada_turbina1_all (
  ts TIMESTAMPTZ,
  asset TEXT,
  vib_lo DOUBLE PRECISION,
  vib_la DOUBLE PRECISION,
  pres_turbina DOUBLE PRECISION,
  pres_tuberia DOUBLE PRECISION,
  coj_radial_lo DOUBLE PRECISION,
  coj_radial_la DOUBLE PRECISION,
  rod_empuje DOUBLE PRECISION,
  rod_guia DOUBLE PRECISION,
  dev_u1 DOUBLE PRECISION,
  dev_u2 DOUBLE PRECISION,
  dev_v1 DOUBLE PRECISION,
  dev_v2 DOUBLE PRECISION,
  dev_w1 DOUBLE PRECISION,
  dev_w2 DOUBLE PRECISION,
  aire_frio DOUBLE PRECISION,
  aire_caliente DOUBLE PRECISION,
  deltaT_aire DOUBLE PRECISION,
  dev_avg DOUBLE PRECISION,
  anomaly_score DOUBLE PRECISION
);

-- Índices para queries típicas de Grafana (tiempo + filtro por asset)
CREATE INDEX IF NOT EXISTS idx_scada_turbina1_all_ts
  ON scada_turbina1_all (ts);

CREATE INDEX IF NOT EXISTS idx_scada_turbina1_all_asset_ts
  ON scada_turbina1_all (asset, ts DESC);
