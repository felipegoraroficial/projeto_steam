import json
import os
import time

from datetime import datetime

from kafka import KafkaConsumer
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()


def deserialize_message(message):
    try:
        return json.loads(message.decode("utf-8"))

    except Exception as e:
        print(f"Mensagem ignorada: {e}")
        return None


print("======================================", flush=True)
print("Iniciando Mongo Consumer", flush=True)
print("KAFKA_BOOTSTRAP =", os.getenv("KAFKA_BOOTSTRAP"), flush=True)
print("MONGO_URI =", os.getenv("MONGO_URI"), flush=True)
print("MONGO_DATABASE =", os.getenv("MONGO_DATABASE"), flush=True)
print("MONGO_COLLECTION =", os.getenv("MONGO_COLLECTION"), flush=True)
print("======================================", flush=True)


consumer = None

for attempt in range(30):

    try:

        print(
            f"Tentativa {attempt + 1}/30 conectando ao Kafka...",
            flush=True
        )

        consumer = KafkaConsumer(
            "steam-playtime",
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP"
            ),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="mongo-playtime-group",
            value_deserializer=deserialize_message
        )

        print(
            "✅ Consumer conectado ao Kafka",
            flush=True
        )

        break

    except Exception as e:

        print(
            f"❌ Falha ao conectar ao Kafka: {e}",
            flush=True
        )

        time.sleep(5)

if consumer is None:

    raise Exception(
        "Não foi possível conectar ao Kafka após 30 tentativas"
    )


mongo_client = MongoClient(
    os.getenv("MONGO_URI")
)

db = mongo_client[
    os.getenv("MONGO_DATABASE")
]

collection = db[
    os.getenv("MONGO_COLLECTION")
]

print(
    "✅ Conectado ao MongoDB",
    flush=True
)

print(
    "✅ Consumer iniciado",
    flush=True
)


for message in consumer:

    if message.value is None:
        continue

    try:

        payload = message.value

        if "captured_at" in payload:

            payload["captured_at"] = (
                datetime.fromisoformat(
                    payload["captured_at"]
                )
            )

        print(
            f"Recebido: {payload}",
            flush=True
        )

        collection.insert_one(payload)

        print(
            "✅ Inserido no MongoDB",
            flush=True
        )

    except Exception as e:

        print(
            f"❌ Erro ao inserir no MongoDB: {e}",
            flush=True
        )