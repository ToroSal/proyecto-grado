-- Agrega columnas de Capa 1 (sub-scores) y Capa 2 (Isolation Forest)
-- Ejecutar: cat flink/sql/alter_add_ml_columns.sql | docker compose exec -T postgres psql -U postgres -d postgres

ALTER TABLE scada_turbina1_all
    ADD COLUMN IF NOT EXISTS score_vib         DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS score_bearing     DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS score_winding     DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS score_cooling     DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS score_lubrication DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS if_score          DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS if_anomaly        SMALLINT DEFAULT 0;
