import json
import os
import time
import random
import logging
from datetime import datetime, timedelta
import numpy as np
from kafka import KafkaProducer
#  Simulate at least 3 machines
#  Each machine should have: id, type, location and other fields
#  Example types: CNC Mill, Lathe, Press, Grinder, Welder,...

#  Each machine generates at least 3 sensor types


# 1. Generate sensor readings with realistic values using Gaussian distribution
#  2. Publish messages to Redpanda topic 
# machine-sensors
#  3. Send messages every x seconds (configurable via environment variable)
#  4. At start of the script, generate a bunch of messages from the past week to spread sensor readings
#  5. Include proper error handling and reconnection logic
#  6. Log message delivery status

logging.basicConfig(level=logging.INFO)

SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", 2))
BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC = os.getenv("TOPIC", "machine-sensors")

MACHINES = [
    {"id": "M1", "type": "CNC Mill", "location": "Plant A"},
    {"id": "M2", "type": "Lathe", "location": "Plant B"},
    {"id": "M3", "type": "Press", "location": "Plant A"},
]

SENSOR = ["temperature", "pressure", "vibration"]


# ---------- Functies ----------

def connect_producer():
    """Try to connect with retry logic."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            logging.info("Connected to Redpanda!")
            return producer

        except Exception as e:
            logging.error(f"Kafka connection failed: {e}, retrying...")
            time.sleep(5)


def generate_sensor_value(sensor_type):
    """Gaussian sensor noise."""
    if sensor_type == "temperature":
        return np.random.normal(70, 5)
    elif sensor_type == "pressure":
        return np.random.normal(30, 2)
    elif sensor_type == "vibration":
        return np.random.normal(5, 0.5)


def generate_message(timestamp=None):
    """Build a machine sensor JSON event."""
    machine = random.choice(MACHINES)
    sensor = random.choice(SENSOR_TYPES)

    return {
        "machine_id": machine["id"],
        "machine_type": machine["type"],
        "location": machine["location"],
        "sensor_type": sensor,
        "value": round(generate_sensor_value(sensor), 2),
        "timestamp": timestamp or datetime.utcnow().isoformat()
    }


def send_bootstrap_data(producer):
    """Send 1 week of historical events at startup."""
    logging.info("Sending bootstrap week of historical messages...")

    now = datetime.utcnow()
    start = now - timedelta(days=7)

    ts = start
    while ts < now:
        msg = generate_message(timestamp=ts.isoformat())
        producer.send(TOPIC, msg)
        ts += timedelta(minutes=5)

    producer.flush()
    logging.info("Bootstrap messages sent!")


# ----------- MAIN LOOP -----------

def main():
    producer = connect_producer()
    send_bootstrap_data(producer)

    logging.info("Starting live ingestion loop...")

    while True:
        msg = generate_message()
        producer.send(TOPIC, msg)
        logging.info(f"Sent: {msg}")
        time.sleep(SEND_INTERVAL)


if __name__ == "__main__":
    main()
