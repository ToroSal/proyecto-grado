"""Isolation Forest inference — singleton per TaskManager."""
from __future__ import annotations

import json
import logging
import os
import pickle
from typing import Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """
    Carga artefactos offline, expone predict(). Singleton por TaskManager.
    Si los artefactos no existen, opera en modo 'sin modelo':
      predict() retorna {"if_score": None, "if_anomaly": 0}
    """

    _instance = None  # type: Optional[AnomalyDetector]

    @classmethod
    def get_instance(cls, artifacts_dir="/opt/src/artifacts"):
        # type: (str) -> AnomalyDetector
        if cls._instance is None:
            cls._instance = cls(artifacts_dir)
        return cls._instance

    def __init__(self, artifacts_dir):
        # type: (str) -> None
        self._ready = False
        self._scaler = None
        self._model = None
        self._feature_names = []   # type: List[str]
        self._n_features = 49
        self._threshold = -0.7073

        # --- model_meta.json ---
        meta_path = os.path.join(artifacts_dir, "model_meta.json")
        if not os.path.exists(meta_path):
            logger.warning(
                "model_meta.json no encontrado en %s — IF deshabilitado", artifacts_dir
            )
            return

        with open(meta_path, "r") as f:
            meta = json.load(f)

        self._feature_names = meta["feature_names"]  # type: List[str]
        self._n_features = meta["n_features"]         # type: int
        self._threshold = meta["threshold"]            # type: float

        if len(self._feature_names) != self._n_features:
            logger.warning(
                "feature_names length %d != n_features %d — IF deshabilitado",
                len(self._feature_names), self._n_features,
            )
            return

        # --- scaler.pkl (RobustScaler) ---
        scaler_path = os.path.join(artifacts_dir, "scaler.pkl")
        if not os.path.exists(scaler_path):
            logger.warning("scaler.pkl no encontrado — IF deshabilitado")
            return

        with open(scaler_path, "rb") as f:
            self._scaler = pickle.load(f)

        # --- if_model.pkl (IsolationForest) ---
        model_path = os.path.join(artifacts_dir, "if_model.pkl")
        if not os.path.exists(model_path):
            logger.warning("if_model.pkl no encontrado — IF deshabilitado")
            return

        with open(model_path, "rb") as f:
            self._model = pickle.load(f)

        self._ready = True
        logger.info(
            "Cargado: %d features | threshold=%.4f",
            self._n_features,
            self._threshold,
        )

    # ------------------------------------------------------------------
    @property
    def is_ready(self):
        # type: () -> bool
        return self._ready

    @property
    def feature_names(self):
        # type: () -> List[str]
        return list(self._feature_names)

    @property
    def n_features(self):
        # type: () -> int
        return self._n_features

    # ------------------------------------------------------------------
    def predict(self, feature_vector):
        # type: (List[float]) -> Dict[str, object]
        """
        Entrada: lista de 49 floats en el orden exacto de feature_names.
        Salida:  {"if_score": float, "if_anomaly": int}
                 {"if_score": None,  "if_anomaly": 0} si modelo no disponible.
        """
        if not self._ready:
            return {"if_score": None, "if_anomaly": 0}

        if len(feature_vector) != self._n_features:
            raise ValueError(
                "Expected %d features, got %d"
                % (self._n_features, len(feature_vector))
            )

        X = np.array(feature_vector, dtype=np.float64).reshape(1, -1)
        X_scaled = self._scaler.transform(X)
        score = float(self._model.score_samples(X_scaled)[0])
        anomaly = 1 if score <= self._threshold else 0

        return {"if_score": score, "if_anomaly": anomaly}
