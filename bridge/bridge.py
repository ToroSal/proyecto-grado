import os, json, time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_RETRY_SECONDS = float(os.getenv("KAFKA_RETRY_SECONDS", "2"))

def parse_map(env_key: str):
    raw = os.getenv(env_key, "")
    if not raw:
        return None, None
    if "=" not in raw:
        return None, None
    left, right = raw.split("=", 1)
    return left.strip(), right.strip()

maps = []
for env_key in sorted(os.environ.keys()):
    if not env_key.startswith("MAP_"):
        continue
    m = parse_map(env_key)
    if m[0]:
        maps.append(m)

mqtt_to_kafka = dict(maps)

def create_kafka_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: (k or "").encode("utf-8"),
                linger_ms=20,
                request_timeout_ms=30_000,
                api_version_auto_timeout_ms=30_000,
                retries=5,
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready at {KAFKA_BOOTSTRAP}; retrying in {KAFKA_RETRY_SECONDS}s")
            time.sleep(KAFKA_RETRY_SECONDS)

producer = create_kafka_producer()

def on_connect(client, userdata, flags, rc, properties=None):
    print("MQTT connected:", rc)
    for mqtt_topic in mqtt_to_kafka:
        client.subscribe(mqtt_topic, qos=1)
        print("Subscribed:", mqtt_topic)

def on_message(client, userdata, msg):
    kafka_topic = mqtt_to_kafka.get(msg.topic)
    if not kafka_topic:
        return
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        payload = {"raw": msg.payload.decode("utf-8", errors="ignore")}
    key = payload.get("asset") or msg.topic
    producer.send(kafka_topic, key=key, value=payload)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
client.loop_forever()
