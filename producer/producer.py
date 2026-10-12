import os
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuración obtenida mediante variables de entorno
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "iot_sensor_data")

# Bucle de reintentos para asegurar conexión con el broker
producer = None
print(f"Intentando conectar al broker en {BOOTSTRAP_SERVERS}...")

while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5
        )
        print("¡Conectado exitosamente al broker!")
    except Exception as e:
        print(f"Broker no disponible ({e}). Reintentando en 3 segundos...")
        time.sleep(3)

# Identificadores de los motores simulados
MOTORES = ["MOTOR_A1", "MOTOR_A2", "MOTOR_B1", "MOTOR_B2"]

print(f"Iniciando emisión hacia el topic '{TOPIC_NAME}'...")

try:
    while True:
        motor_id = random.choice(MOTORES)
        
        # 10% de probabilidad de generar una anomalía crítica
        es_anomalia = random.random() < 0.10

        if es_anomalia:
            temperatura = round(random.uniform(86.0, 110.0), 2)
            vibracion = round(random.uniform(5.5, 9.8), 2)
        else:
            temperatura = round(random.uniform(55.0, 82.0), 2)
            vibracion = round(random.uniform(1.2, 4.0), 2)

        presion = round(random.uniform(2.0, 4.5), 2)

        evento = {
            "motor_id": motor_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperatura_c": temperatura,
            "vibracion_mms": vibracion,
            "presion_bar": presion,
            "alerta_esperada": es_anomalia
        }

        producer.send(TOPIC_NAME, evento)
        print(f"[EMITIDO] {motor_id} | Temp: {temperatura}°C | Vib: {vibracion} mm/s | Anomalía: {es_anomalia}")

        time.sleep(random.uniform(0.3, 0.7))

except KeyboardInterrupt:
    print("Deteniendo emisor...")
finally:
    if producer:
        producer.flush()
        producer.close()