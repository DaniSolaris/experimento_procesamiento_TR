import os
import json
import time
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "iot_sensor_data")
GROUP_ID = os.getenv("GROUP_ID", "iot_anomaly_detector_group")

consumer = None
print(f"Intentando conectar al broker en {BOOTSTRAP_SERVERS}...")

while not consumer:
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        print("¡Consumidor conectado exitosamente al broker!")
    except Exception as e:
        print(f"Broker no disponible ({e}). Reintentando en 3 segundos...")
        time.sleep(3)

LIMITE_TEMP_CRITICA = 85.0   # °C
LIMITE_VIB_CRITICA = 5.0     # mm/s

print(f"Escuchando eventos en tiempo real en el topic '{TOPIC_NAME}'...\n")

try:
    for mensaje in consumer:
        evento = mensaje.value
        motor = evento.get("motor_id", "DESCONOCIDO")
        temp = evento.get("temperatura_c", 0.0)
        vib = evento.get("vibracion_mms", 0.0)
        ts = evento.get("timestamp")

        es_critico = temp > LIMITE_TEMP_CRITICA or vib > LIMITE_VIB_CRITICA

        if es_critico:
            print(
                f"[⚠️ ALERTA CRÍTICA] {motor} | "
                f"Temp: {temp}°C (Límite: {LIMITE_TEMP_CRITICA}) | "
                f"Vib: {vib} mm/s (Límite: {LIMITE_VIB_CRITICA}) | "
                f"TS: {ts}"
            )
        else:
            print(f"[✅ NORMAL] {motor} | Temp: {temp}°C | Vib: {vib} mm/s")

except KeyboardInterrupt:
    print("Deteniendo consumidor...")
finally:
    if consumer:
        consumer.close()