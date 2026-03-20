"""Feature engineering sin pandas — DimensionScorer + RollingFeatureBuffer."""
from __future__ import annotations

from collections import deque
from typing import Dict, List, Optional

import numpy as np

# ---------------------------------------------------------------------------
# Umbrales por dimension (Capa 1)
# ---------------------------------------------------------------------------
THRESHOLDS = {
    "vib":       {"warn": 0.45,  "alarm": 0.60},    # mm/s RMS  (sim hi≈0.74)
    "bearing":   {"warn": 57.5,  "alarm": 60.0},    # C max(coj_la/lo/rod); rod_empuje normal≈56
    "winding":   {"warn": 63.0,  "alarm": 69.0},    # C (dev_* normal≈58-62, hi≈71.9)
    "delta_t":   {"warn": 8.0,   "alarm": 15.0},    # C (deltaT aire)
    "rod_temp":  {"warn": 55.0,  "alarm": 58.0},    # C (rod_empuje abs, proxy lubricacion)
}


def _clamp01(v):
    # type: (float) -> float
    return max(0.0, min(1.0, v))


def _safe(val, default=0.0):
    # type: (object, float) -> float
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ===================================================================
# DimensionScorer
# ===================================================================
class DimensionScorer:
    """Calcula los 5 sub-scores (0-1) + anomaly_score legacy desde el payload."""

    @staticmethod
    def _normalize(value, warn, alarm):
        # type: (float, float, float) -> float
        if alarm == warn:
            return 1.0 if value >= alarm else 0.0
        return _clamp01((value - warn) / (alarm - warn))

    def score(self, r):
        # type: (Dict) -> Dict
        """
        Entrada: dict normalizado con campos internos.
        Salida:  dict con score_vib … score_lubrication, anomaly_score.
        """
        # 1. Vibracion
        vib_max = max(_safe(r.get("vib_la")), _safe(r.get("vib_lo")))
        score_vib = self._normalize(vib_max, **THRESHOLDS["vib"])

        # 2. Cojinetes / Rodamientos
        bearing_max = max(
            _safe(r.get("coj_la")),
            _safe(r.get("coj_lo")),
            _safe(r.get("rod_empuje")),
            _safe(r.get("rod_guia")),
        )
        score_bearing = self._normalize(bearing_max, **THRESHOLDS["bearing"])

        # 3. Devanados
        dev_keys = ("dev_u1", "dev_u2", "dev_v1", "dev_v2", "dev_w1", "dev_w2")
        devs = [_safe(r.get(k)) for k in dev_keys]
        winding_max = max(devs) if devs else 0.0
        score_winding = self._normalize(winding_max, **THRESHOLDS["winding"])

        # 4. Enfriamiento (deltaT aire)
        delta_t = _safe(r.get("aire_caliente")) - _safe(r.get("aire_frio"))
        score_cooling = self._normalize(delta_t, **THRESHOLDS["delta_t"])

        # 5. Lubricacion (temperatura absoluta rod_empuje como proxy;
        #    aceite_gl no disponible en MQTT -> diferencial no es fiable)
        rod_temp = _safe(r.get("rod_empuje"))
        score_lubrication = self._normalize(rod_temp, **THRESHOLDS["rod_temp"])

        anomaly_score = (
            score_vib + score_bearing + score_winding
            + score_cooling + score_lubrication
        ) / 5.0

        return {
            "score_vib": round(score_vib, 6),
            "score_bearing": round(score_bearing, 6),
            "score_winding": round(score_winding, 6),
            "score_cooling": round(score_cooling, 6),
            "score_lubrication": round(score_lubrication, 6),
            "anomaly_score": round(anomaly_score, 6),
        }


# ===================================================================
# RollingFeatureBuffer
# ===================================================================
class RollingFeatureBuffer:
    """
    Buffer circular por asset para rolling stats online.
    Sin pandas, solo deque + numpy.

    Ventanas identicas al entrenamiento offline:
      W5  = 30 muestras  (5 min  @ 10 s)
      W15 = 90 muestras  (15 min @ 10 s)
      W60 = 360 muestras (60 min @ 10 s)

    Features que produce (49 total, mismo orden que model_meta.json):
      Por cada una de las 7 variables base:
        raw, mean_5m, mean_15m, mean_60m, std_5m, std_15m, std_60m
      -> 7 x 7 = 49 features
    """

    BASE_FEATURES = [
        "dev_promedio",
        "dev_desbalance",
        "delta_t_aire",
        "delta_coj_la_lo",
        "vib_promedio",
        "delta_vib_la_lo",
        "delta_aceite_empuje",
    ]

    W5 = 30    # 5 min
    W15 = 90   # 15 min
    W60 = 360  # 60 min

    def __init__(self):
        # type: () -> None
        self._buffer = deque(maxlen=self.W60)  # type: deque

    def push(self, base_features):
        # type: (Dict[str, float]) -> None
        """Agrega el registro actual al buffer."""
        row = [base_features.get(f, 0.0) for f in self.BASE_FEATURES]
        self._buffer.append(row)

    def compute_rolling(self):
        # type: () -> Optional[List[float]]
        """
        Retorna vector de 49 floats si buffer >= 360 muestras.
        Retorna None si aun esta en cold start (primeras 60 min).
        """
        if len(self._buffer) < self.W60:
            return None

        arr = np.array(self._buffer, dtype=np.float64)  # (360, 7)
        result = []  # type: List[float]

        for i in range(len(self.BASE_FEATURES)):
            col = arr[:, i]
            raw = float(col[-1])

            w5 = col[-self.W5:]
            w15 = col[-self.W15:]
            w60 = col  # completo (360)

            mean_5m = float(np.mean(w5))
            mean_15m = float(np.mean(w15))
            mean_60m = float(np.mean(w60))
            std_5m = float(np.std(w5, ddof=0))
            std_15m = float(np.std(w15, ddof=0))
            std_60m = float(np.std(w60, ddof=0))

            result.extend([
                raw, mean_5m, mean_15m, mean_60m,
                std_5m, std_15m, std_60m,
            ])

        return result

    @staticmethod
    def extract_base_features(r):
        # type: (Dict) -> Dict[str, float]
        """
        Calcula las 7 variables base desde el payload normalizado.
        """
        dev_keys = ("dev_u1", "dev_u2", "dev_v1", "dev_v2", "dev_w1", "dev_w2")
        devs = [_safe(r.get(k)) for k in dev_keys]

        dev_promedio = sum(devs) / len(devs) if devs else 0.0
        dev_desbalance = (max(devs) - min(devs)) if devs else 0.0

        delta_t_aire = _safe(r.get("aire_caliente")) - _safe(r.get("aire_frio"))
        delta_coj_la_lo = _safe(r.get("coj_la")) - _safe(r.get("coj_lo"))

        vib_la = _safe(r.get("vib_la"))
        vib_lo = _safe(r.get("vib_lo"))
        vib_promedio = (vib_la + vib_lo) / 2.0
        delta_vib_la_lo = vib_la - vib_lo

        delta_aceite_empuje = _safe(r.get("aceite_empuje")) - _safe(r.get("aceite_gl"))

        return {
            "dev_promedio": dev_promedio,
            "dev_desbalance": dev_desbalance,
            "delta_t_aire": delta_t_aire,
            "delta_coj_la_lo": delta_coj_la_lo,
            "vib_promedio": vib_promedio,
            "delta_vib_la_lo": delta_vib_la_lo,
            "delta_aceite_empuje": delta_aceite_empuje,
        }
