"""
train_artifacts.py — Genera scaler.pkl e if_model.pkl para el Isolation Forest.

Uso dentro del container:
    docker exec taskmanager python3 /opt/src/ml/train_artifacts.py

Los Excel deben estar en /tmp/ (copiarlos con docker cp):
    docker cp "Variables de temperatura y vibraciones de G1 (parte 2).xlsx" taskmanager:/tmp/
    docker cp "Variables temperatura de G1 (parte 1).xlsx" taskmanager:/tmp/
"""
from __future__ import annotations

import json
import os
import pickle

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

# ---------------------------------------------------------------------------
# Rutas — configurables por variables de entorno
# ---------------------------------------------------------------------------
EXCEL_DIR     = os.environ.get("EXCEL_DIR", "/tmp")
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "/opt/src/artifacts")

EXCEL_1 = os.path.join(EXCEL_DIR, "Variables de temperatura y vibraciones de G1 (parte 2).xlsx")
EXCEL_2 = os.path.join(EXCEL_DIR, "Variables temperatura de G1 (parte 1).xlsx")

# ---------------------------------------------------------------------------
# Ventanas (igual que RollingFeatureBuffer en feature_builder.py)
# ---------------------------------------------------------------------------
W5  = 30   # 5 min  @ 10 s
W15 = 90   # 15 min @ 10 s
W60 = 360  # 60 min @ 10 s

# ---------------------------------------------------------------------------
# 1. Cargar y unir datos
# ---------------------------------------------------------------------------
print("Cargando datos desde Excel...")
df1 = pd.read_excel(EXCEL_1)
df2 = pd.read_excel(EXCEL_2)

for df in (df1, df2):
    hora = pd.to_datetime(df["HORA"].astype(str), format="%H:%M:%S", errors="coerce").dt.strftime("%H:%M:%S")
    df["TIMESTAMP"] = pd.to_datetime(
        df["FECHA"].dt.strftime("%Y-%m-%d") + " " + hora, errors="coerce"
    )

data = df1.merge(df2.drop(columns=["FECHA", "HORA"]), on="TIMESTAMP", how="inner")
data = data.set_index("TIMESTAMP").sort_index()
print(f"  Filas tras merge: {len(data)}")

# ---------------------------------------------------------------------------
# 2. Calcular 7 variables base
# ---------------------------------------------------------------------------
dev_cols = [
    "G1-TEMPERATURA DEVANADO U1 ValueY",
    "G1-TEMPERATURA DEVANADO U2 ValueY",
    "G1-TEMPERATURA DEVANADO V1 ValueY",
    "G1-TEMPERATURA DEVANADO V2 ValueY",
    "G1-TEMPERATURA DEVANADO W1 ValueY",
    "G1-TEMPERATURA DEVANADO W2 ValueY",
]

df = pd.DataFrame(index=data.index)
df["dev_promedio"]        = data[dev_cols].mean(axis=1)
df["dev_desbalance"]      = data[dev_cols].max(axis=1) - data[dev_cols].min(axis=1)
df["delta_t_aire"]        = (data["G1-TEMPERATURA AIRE CALIENTE ValueY"]
                             - data["G1-TEMPERATURA AIRE FRIO ValueY"])
df["delta_coj_la_lo"]     = (data["G1-TEMPERATURA COJINETE LA ValueY"]
                             - data["G1-TEMPERATURA COJINETE RADIAL LO ValueY"])
df["vib_promedio"]        = data[["G1-VIBRACION  COJINETE LA ValueY",
                                   "G1-VIBRACION  COJINETE LO ValueY"]].mean(axis=1)
df["delta_vib_la_lo"]     = (data["G1-VIBRACION  COJINETE LA ValueY"]
                             - data["G1-VIBRACION  COJINETE LO ValueY"])
df["delta_aceite_empuje"] = (data["G1-TEMPERATURA ACEITE RODAMIENTO EMPUJE ValueY"]
                             - data["G1-TEMPERATURA ACEITE GL ValueY"])

# ---------------------------------------------------------------------------
# 3. Rolling features — ddof=0 para coincidir con numpy en RollingFeatureBuffer
# ---------------------------------------------------------------------------
base_cols = list(df.columns)
feature_cols = []

for col in base_cols:
    df[f"{col}_mean_5m"]  = df[col].rolling(W5,  min_periods=W5).mean()
    df[f"{col}_mean_15m"] = df[col].rolling(W15, min_periods=W15).mean()
    df[f"{col}_mean_60m"] = df[col].rolling(W60, min_periods=W60).mean()
    df[f"{col}_std_5m"]   = df[col].rolling(W5,  min_periods=W5).std(ddof=0)
    df[f"{col}_std_15m"]  = df[col].rolling(W15, min_periods=W15).std(ddof=0)
    df[f"{col}_std_60m"]  = df[col].rolling(W60, min_periods=W60).std(ddof=0)
    feature_cols.extend([
        col,
        f"{col}_mean_5m", f"{col}_mean_15m", f"{col}_mean_60m",
        f"{col}_std_5m",  f"{col}_std_15m",  f"{col}_std_60m",
    ])

df_features = df[feature_cols].dropna()
print(f"  Features tras dropna: {df_features.shape}  (esperado: N x 49)")
assert df_features.shape[1] == 49, f"ERROR: se esperaban 49 features, se obtuvieron {df_features.shape[1]}"

# ---------------------------------------------------------------------------
# 4. Split temporal: 70% Train / 15% Val / 15% Test
# ---------------------------------------------------------------------------
n = len(df_features)
train_end = int(n * 0.70)
val_end   = int(n * 0.85)

train_df = df_features.iloc[:train_end]
val_df   = df_features.iloc[train_end:val_end]
test_df  = df_features.iloc[val_end:]

print(f"  Train: {len(train_df)}  Val: {len(val_df)}  Test: {len(test_df)}")

# ---------------------------------------------------------------------------
# 5. Escalado sin leakage (RobustScaler fit solo en TRAIN)
# ---------------------------------------------------------------------------
scaler = RobustScaler()
X_train = scaler.fit_transform(train_df)
X_val   = scaler.transform(val_df)

# ---------------------------------------------------------------------------
# 6. Entrenamiento Isolation Forest
# ---------------------------------------------------------------------------
print("Entrenando IsolationForest (n_estimators=400, contamination=0.005)...")
model = IsolationForest(
    n_estimators=400,
    max_samples="auto",
    max_features=1.0,
    contamination=0.005,
    random_state=42,
    n_jobs=-1,
)
model.fit(X_train)
print("  Entrenamiento completado.")

# ---------------------------------------------------------------------------
# 7. Calibrar umbral en VAL (percentil 0.5 — misma logica del notebook)
# ---------------------------------------------------------------------------
scores_val = model.score_samples(X_val)
threshold = float(np.percentile(scores_val, 0.5))
print(f"  Umbral calibrado en VAL (percentil 0.5): {threshold:.4f}")

# ---------------------------------------------------------------------------
# 8. Guardar artefactos
# ---------------------------------------------------------------------------
os.makedirs(ARTIFACTS_DIR, exist_ok=True)

with open(os.path.join(ARTIFACTS_DIR, "scaler.pkl"), "wb") as f:
    pickle.dump(scaler, f)
print(f"  Guardado: {ARTIFACTS_DIR}/scaler.pkl")

with open(os.path.join(ARTIFACTS_DIR, "if_model.pkl"), "wb") as f:
    pickle.dump(model, f)
print(f"  Guardado: {ARTIFACTS_DIR}/if_model.pkl")

# Actualizar threshold en model_meta.json
meta_path = os.path.join(ARTIFACTS_DIR, "model_meta.json")
with open(meta_path, "r") as f:
    meta = json.load(f)

meta["threshold"] = round(threshold, 4)
meta["train_samples"] = int(len(train_df))

with open(meta_path, "w") as f:
    json.dump(meta, f, indent=2)
print(f"  Actualizado: model_meta.json  threshold={meta['threshold']}")

# ---------------------------------------------------------------------------
# 9. Evaluacion rapida
# ---------------------------------------------------------------------------
X_test = scaler.transform(test_df)
scores_test = model.score_samples(X_test)
mask_anom = scores_test <= threshold
pct_anom = 100.0 * mask_anom.sum() / len(mask_anom)
print(f"\n  TEST: {pct_anom:.2f}% tiempo anomalo  ({mask_anom.sum()}/{len(mask_anom)} muestras)")
print("\nListo. Reinicia el taskmanager para que cargue los nuevos artefactos.")
