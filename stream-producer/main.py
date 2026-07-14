"""
Faker → Kafka producer.
Generates synthetic hospital care events and publishes to notificacoes.raw.
"""

import hashlib
import json
import os
import random
import time
from datetime import UTC, datetime
from uuid import uuid4

from confluent_kafka import Producer
from faker import Faker

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "notificacoes.raw")
EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "2"))

fake = Faker("pt_BR")

TIPOS_ATENDIMENTO = ["urgencia", "consulta", "exame", "internacao"]
TRIAGENS = ["vermelho", "laranja", "amarelo", "verde", "azul"]
CNES_SAMPLE = [
    "2077396",
    "2078023",
    "2079350",
    "6648298",
    "2079717",
    "2080176",
    "2081938",
    "5601930",
    "2082136",
    "2083043",
]


def fake_cpf_hash() -> str:
    """Generates a synthetic CPF and returns only its SHA-256 hash — never exposes the CPF."""
    cpf = f"{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(100, 999)}-{random.randint(10, 99)}"
    return hashlib.sha256(cpf.encode()).hexdigest()


def build_event() -> dict:
    return {
        "id_atendimento": str(uuid4()),
        "id_paciente_hash": fake_cpf_hash(),
        "id_cnes": random.choice(CNES_SAMPLE),
        "ts_evento": datetime.now(UTC).isoformat(),
        "tipo_atendimento": random.choice(TIPOS_ATENDIMENTO),
        "triagem": random.choice(TRIAGENS),
    }


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] delivery failed: {err}")


def main():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "stream-producer",
        "acks": "1",
    }
    producer = Producer(conf)
    interval = 1.0 / EVENTS_PER_SECOND
    print(f"Publicando {EVENTS_PER_SECOND} eventos/s em {TOPIC} @ {KAFKA_BOOTSTRAP}")

    while True:
        evento = build_event()
        producer.produce(
            TOPIC,
            key=evento["id_atendimento"],
            value=json.dumps(evento, ensure_ascii=False),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(interval + random.uniform(-interval * 0.2, interval * 0.2))


if __name__ == "__main__":
    main()
