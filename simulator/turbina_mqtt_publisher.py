"""
turbina_mqtt_publisher.py
────────────────────────────────────────────────────────────────────────────
Simulador MQTT – Turbina G1 | planta1/turbina/all

Broker host / port are read from environment variables so the same image
works both inside Docker (pointing at the HiveMQ container) and externally:

  MQTT_HOST  – default: hivemq          (Docker service name)
  MQTT_PORT  – default: 1883            (HiveMQ internal port)
  TOPIC      – default: planta1/turbina/all
  PUBLISH_INTERVAL – default: 60        (seconds)

Behaviour modelled from real historical data (138 240 samples):
  · Winding temperatures  – correlated random walk (daily std ≈ 8 °C)
  · Bearings              – slow drift with small white noise
  · Vibrations            – AR(1) process with heavy tail
  · Cold/hot air          – slight circadian oscillation
  · Pressures             – nominal values with small Gaussian noise
  · Anomalies             – ~1-2 % probability per sample, short spike

Dependencies:
    pip install paho-mqtt
"""

import json
import math
import os
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  (overridable via environment variables)
# ─────────────────────────────────────────────────────────────────────────────
BROKER_HOST       = os.getenv("MQTT_HOST", "hivemq")
BROKER_PORT       = int(os.getenv("MQTT_PORT", "1883"))
TOPIC             = os.getenv("TOPIC", "planta1/turbina/all")
ASSET             = os.getenv("ASSET", "turbina1")
PUBLISH_INTERVAL  = int(os.getenv("PUBLISH_INTERVAL", "20"))
QOS               = 1
RETAIN            = False


# ─────────────────────────────────────────────────────────────────────────────
# STATISTICAL PARAMETERS  (derived from historical files)
# ─────────────────────────────────────────────────────────────────────────────
PARAMS = {
    # windings – highly correlated with each other
    "dev_u1":        {"mean": 55.4, "drift_std": 0.04, "noise_std": 0.05, "lo": 38.9,  "hi": 68.0,  "anom_mult": 3.5},
    "dev_u2":        {"mean": 58.7, "drift_std": 0.04, "noise_std": 0.05, "lo": 40.4,  "hi": 71.9,  "anom_mult": 3.5},
    "dev_v1":        {"mean": 56.3, "drift_std": 0.04, "noise_std": 0.05, "lo": 39.6,  "hi": 69.3,  "anom_mult": 3.5},
    "dev_v2":        {"mean": 55.4, "drift_std": 0.04, "noise_std": 0.05, "lo": 39.3,  "hi": 67.9,  "anom_mult": 3.5},
    "dev_w1":        {"mean": 56.1, "drift_std": 0.04, "noise_std": 0.05, "lo": 39.9,  "hi": 68.9,  "anom_mult": 3.5},
    "dev_w2":        {"mean": 54.5, "drift_std": 0.04, "noise_std": 0.05, "lo": 38.7,  "hi": 66.9,  "anom_mult": 3.5},
    # bearings
    "coj_radial_la": {"mean": 36.0, "drift_std": 0.02, "noise_std": 0.03, "lo": 29.6,  "hi": 42.7,  "anom_mult": 4.0},
    "coj_radial_lo": {"mean": 29.8, "drift_std": 0.02, "noise_std": 0.03, "lo": 24.0,  "hi": 35.4,  "anom_mult": 4.0},
    "rod_empuje":    {"mean": 56.0, "drift_std": 0.02, "noise_std": 0.02, "lo": 43.1,  "hi": 58.5,  "anom_mult": 4.0},
    "rod_guia":      {"mean": 48.8, "drift_std": 0.02, "noise_std": 0.02, "lo": 40.3,  "hi": 52.5,  "anom_mult": 4.0},
    # vibrations – noisier, heavy tail (~1.6 % anomalies observed)
    "vib_lo":        {"mean": 0.327, "drift_std": 0.006, "noise_std": 0.010, "lo": 0.03, "hi": 0.742, "anom_mult": 5.0},
    "vib_la":        {"mean": 0.273, "drift_std": 0.006, "noise_std": 0.010, "lo": 0.01, "hi": 0.638, "anom_mult": 5.0},
    # air
    "aire_caliente": {"mean": 34.3, "drift_std": 0.03, "noise_std": 0.04, "lo": 24.7,  "hi": 41.9,  "anom_mult": 3.0},
    "aire_frio":     {"mean": 24.5, "drift_std": 0.03, "noise_std": 0.04, "lo": 18.3,  "hi": 38.7,  "anom_mult": 3.5},
    # pressures
    "pres_turbina":  {"mean": 11.8, "drift_std": 0.01, "noise_std": 0.02, "lo":  8.0,  "hi": 15.0,  "anom_mult": 4.0},
    "pres_tuberia":  {"mean":  7.3, "drift_std": 0.01, "noise_std": 0.02, "lo":  4.0,  "hi": 10.0,  "anom_mult": 4.0},
}

ANOMALY_PROB = {
    "vib_lo": 0.017, "vib_la": 0.015,
    "rod_empuje": 0.006, "rod_guia": 0.002,
    "aire_frio": 0.003,
    "default": 0.0015,
}

# ─────────────────────────────────────────────────────────────────────────────
# SIMULATOR STATE
# ─────────────────────────────────────────────────────────────────────────────
state: dict[str, float] = {k: v["mean"] for k, v in PARAMS.items()}
shared_dev_drift = 0.0


def circadian_offset(variable: str, hour: float) -> float:
    if variable.startswith("dev_") or variable.startswith("aire"):
        return 1.5 * math.sin(2 * math.pi * (hour - 14) / 24)
    return 0.0


def next_value(key: str, hour: float) -> float:
    global shared_dev_drift
    p = PARAMS[key]

    if key.startswith("dev_"):
        shared_dev_drift = 0.97 * shared_dev_drift + random.gauss(0, p["drift_std"])
        delta = shared_dev_drift
    else:
        delta = random.gauss(0, p["drift_std"])

    noise = random.gauss(0, p["noise_std"])
    target = p["mean"] + circadian_offset(key, hour)
    new_val = state[key] + delta + noise + 0.05 * (target - state[key])

    anom_prob = ANOMALY_PROB.get(key, ANOMALY_PROB["default"])
    if random.random() < anom_prob:
        sign = random.choice([-1, 1])
        spike = sign * random.uniform(p["anom_mult"] * 0.8, p["anom_mult"] * 1.2)
        if key.startswith("vib"):
            spike = abs(spike)
        new_val += spike
        print(f"  ⚠  ANOMALÍA en [{key}]: {state[key]:.3f} → {new_val:.3f}", flush=True)

    new_val = max(p["lo"], min(p["hi"], new_val))
    state[key] = new_val
    return round(new_val, 3)


def build_payload() -> dict:
    now = datetime.now(timezone.utc)
    hour = now.hour + now.minute / 60.0
    return {
        "ts":            now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "asset":         ASSET,
        "vib_lo":        next_value("vib_lo",        hour),
        "vib_la":        next_value("vib_la",        hour),
        "pres_turbina":  next_value("pres_turbina",  hour),
        "pres_tuberia":  next_value("pres_tuberia",  hour),
        "coj_radial_lo": next_value("coj_radial_lo", hour),
        "coj_radial_la": next_value("coj_radial_la", hour),
        "rod_empuje":    next_value("rod_empuje",    hour),
        "rod_guia":      next_value("rod_guia",      hour),
        "dev_u1":        next_value("dev_u1",        hour),
        "dev_u2":        next_value("dev_u2",        hour),
        "dev_v1":        next_value("dev_v1",        hour),
        "dev_v2":        next_value("dev_v2",        hour),
        "dev_w1":        next_value("dev_w1",        hour),
        "dev_w2":        next_value("dev_w2",        hour),
        "aire_frio":     next_value("aire_frio",     hour),
        "aire_caliente": next_value("aire_caliente", hour),
    }


# ─────────────────────────────────────────────────────────────────────────────
# MQTT CALLBACKS
# ─────────────────────────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    codes = {
        0: "Conexión exitosa ✓",
        1: "Versión de protocolo rechazada",
        2: "Client ID inválido",
        3: "Servidor no disponible",
        4: "Usuario/contraseña incorrectos",
        5: "No autorizado",
    }
    print(f"[MQTT] {codes.get(rc, f'Código desconocido: {rc}')}", flush=True)


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print(f"[MQTT] Desconexión inesperada (rc={rc}). Reconectando en 10 s…", flush=True)


def on_publish(client, userdata, mid):
    print(f"[MQTT] Mensaje publicado (mid={mid})", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60, flush=True)
    print(" Simulador MQTT – Turbina G1", flush=True)
    print(f" Broker : {BROKER_HOST}:{BROKER_PORT}", flush=True)
    print(f" Tópico : {TOPIC}", flush=True)
    print(f" Ciclo  : {PUBLISH_INTERVAL} s", flush=True)
    print("=" * 60, flush=True)

    client = mqtt.Client(
        client_id=f"sim_turbina_{int(time.time())}",
        protocol=mqtt.MQTTv311,
        clean_session=True,
    )
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish    = on_publish

    connected = False
    while not connected:
        try:
            client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
            connected = True
        except Exception as e:
            print(f"[ERROR] No se pudo conectar: {e}. Reintentando en 15 s…", flush=True)
            time.sleep(15)

    client.loop_start()

    print("\nPublicando cada 60 segundos. Presiona Ctrl+C para detener.\n", flush=True)
    try:
        while True:
            payload = build_payload()
            json_payload = json.dumps(payload, separators=(",", ":"))
            result = client.publish(TOPIC, json_payload, qos=QOS, retain=RETAIN)
            result.wait_for_publish()
            print(f"[{payload['ts']}] Publicado → {json_payload}", flush=True)
            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print("\n[INFO] Detenido por el usuario.", flush=True)
    finally:
        client.loop_stop()
        client.disconnect()
        print("[MQTT] Desconectado.", flush=True)


if __name__ == "__main__":
    main()
