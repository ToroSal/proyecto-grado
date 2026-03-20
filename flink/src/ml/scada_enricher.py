"""SCADAEnricher — MapFunction de PyFlink que enriquece cada registro MQTT."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict

from pyflink.datastream.functions import MapFunction

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Mapeo MQTT -> nombres internos
# ---------------------------------------------------------------------------
MQTT_FIELD_MAP = {
    "vib_lo":        "vib_lo",
    "vib_la":        "vib_la",
    "pres_turbina":  "pres_turbina",
    "pres_tuberia":  "pres_tuberia",
    "coj_radial_la": "coj_la",
    "coj_radial_lo": "coj_lo",
    "rod_guia":      "rod_guia",
    "dev_u1": "dev_u1", "dev_u2": "dev_u2",
    "dev_v1": "dev_v1", "dev_v2": "dev_v2",
    "dev_w1": "dev_w1", "dev_w2": "dev_w2",
    "aire_frio":     "aire_frio",
    "aire_caliente": "aire_caliente",
}


def _float_or_none(v):
    """Convierte a float; retorna None si no es posible."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class SCADAEnricher(MapFunction):
    """
    Enriquece cada mensaje JSON del topic Kafka con las 7 columnas nuevas.
    Mantiene los campos legacy (deltaT_aire, dev_avg, anomaly_score).
    """

    def __init__(self):
        self._detector = None
        self._scorer = None
        self._buffers = {}        # type: Dict[str, object]
        self._time_mode = "ingest"
        self._RollingFeatureBuffer = None

    # ------------------------------------------------------------------
    def open(self, runtime_context):
        """Carga AnomalyDetector singleton y DimensionScorer."""
        import sys
        sys.path.insert(0, "/opt/src")

        from ml.anomaly_detector import AnomalyDetector
        from ml.feature_builder import DimensionScorer, RollingFeatureBuffer

        artifacts_dir = os.environ.get("ARTIFACTS_DIR", "/opt/src/artifacts")
        self._time_mode = os.environ.get("TIME_MODE", "ingest")

        self._detector = AnomalyDetector.get_instance(artifacts_dir)
        self._scorer = DimensionScorer()
        self._buffers = {}
        self._RollingFeatureBuffer = RollingFeatureBuffer

        logger.info("SCADAEnricher open — artifacts=%s time_mode=%s",
                     artifacts_dir, self._time_mode)

    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_fields(raw):
        # type: (Dict) -> Dict
        """Convierte payload MQTT a nombres internos."""
        r = {}  # type: Dict
        for mqtt_key, internal_key in MQTT_FIELD_MAP.items():
            r[internal_key] = _float_or_none(raw.get(mqtt_key))

        # rod_empuje -> rod_empuje (bearing) + aceite_empuje (lubrication proxy)
        rod_val = _float_or_none(raw.get("rod_empuje"))
        r["rod_empuje"] = rod_val if rod_val is not None else r.get("rod_empuje")
        r["aceite_empuje"] = rod_val if rod_val is not None else 0.0

        # aceite_gl no disponible en payload MQTT -> imputar 0.0
        r["aceite_gl"] = 0.0

        # Defaults seguros
        for k in ("rod_empuje", "rod_guia", "aceite_empuje"):
            if r.get(k) is None:
                r[k] = 0.0

        # Metadata
        r["asset"] = raw.get("asset", "unknown")
        r["ts_raw"] = raw.get("ts")
        return r

    # ------------------------------------------------------------------
    def map(self, value):
        # type: (str) -> str
        """
        Flujo por registro:
          1. json.loads(value)
          2. Normalizar campos MQTT -> nombres internos
          3. DimensionScorer.score() -> 5 sub-scores + anomaly_score
          4. Calcular deltaT_aire y dev_avg (legacy)
          5. RollingFeatureBuffer.push() + compute_rolling()
          6. Si cold start -> if_score=None, if_anomaly=0
          7. Si hay vector -> AnomalyDetector.predict()
          8. json.dumps(record enriquecido)
        """
        try:
            raw = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            logger.warning("JSON invalido, pasando sin enriquecer")
            return value

        # 1-2. Normalizar
        r = self._normalize_fields(raw)
        asset = r["asset"]

        # 3. Sub-scores por dimension
        scores = self._scorer.score(r)

        # 4. Legacy: deltaT_aire, dev_avg
        aire_c = r.get("aire_caliente")
        aire_f = r.get("aire_frio")
        deltaT_aire = None
        if aire_c is not None and aire_f is not None:
            deltaT_aire = aire_c - aire_f

        dev_keys = ("dev_u1", "dev_u2", "dev_v1", "dev_v2", "dev_w1", "dev_w2")
        valid_devs = [r[k] for k in dev_keys if r.get(k) is not None]
        dev_avg = (sum(valid_devs) / len(valid_devs)) if valid_devs else None

        # 5. Rolling features + IF prediction
        from ml.feature_builder import RollingFeatureBuffer

        if asset not in self._buffers:
            self._buffers[asset] = self._RollingFeatureBuffer()
        buf = self._buffers[asset]

        base_feats = RollingFeatureBuffer.extract_base_features(r)
        buf.push(base_feats)
        rolling = buf.compute_rolling()

        # 6-7. Cold start vs predict
        if rolling is None:
            if_score = None
            if_anomaly = 0
        else:
            pred = self._detector.predict(rolling)
            if_score = pred["if_score"]
            if_anomaly = pred["if_anomaly"]

        # 8. Timestamp
        if self._time_mode == "ingest":
            ts = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3] + "Z"
        else:
            ts = r.get("ts_raw") or datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3] + "Z"

        # --- Build output ---
        output = {
            "ts": ts,
            "asset": asset,
            "vib_lo": r.get("vib_lo"),
            "vib_la": r.get("vib_la"),
            "pres_turbina": r.get("pres_turbina"),
            "pres_tuberia": r.get("pres_tuberia"),
            "coj_radial_lo": r.get("coj_lo"),
            "coj_radial_la": r.get("coj_la"),
            "rod_empuje": r.get("rod_empuje"),
            "rod_guia": r.get("rod_guia"),
            "dev_u1": r.get("dev_u1"),
            "dev_u2": r.get("dev_u2"),
            "dev_v1": r.get("dev_v1"),
            "dev_v2": r.get("dev_v2"),
            "dev_w1": r.get("dev_w1"),
            "dev_w2": r.get("dev_w2"),
            "aire_frio": r.get("aire_frio"),
            "aire_caliente": r.get("aire_caliente"),
            # Legacy
            "deltaT_aire": deltaT_aire,
            "dev_avg": round(dev_avg, 4) if dev_avg is not None else None,
            # Capa 1: sub-scores
            "anomaly_score": scores["anomaly_score"],
            "score_vib": scores["score_vib"],
            "score_bearing": scores["score_bearing"],
            "score_winding": scores["score_winding"],
            "score_cooling": scores["score_cooling"],
            "score_lubrication": scores["score_lubrication"],
            # Capa 2: Isolation Forest
            "if_score": round(if_score, 6) if if_score is not None else None,
            "if_anomaly": if_anomaly,
        }

        return json.dumps(output)
