import os
import json
import time
import requests

from datetime import datetime, UTC

from kafka import KafkaProducer
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

INTERVAL_SECONDS = int(
    os.getenv("INTERVAL_SECONDS", "86400")
)

print("======================================", flush=True)
print("Steam Details Producer", flush=True)
print("======================================", flush=True)

#
# Kafka
#

producer = None

for attempt in range(20):

    try:

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        print(
            "✅ Conectado ao Kafka",
            flush=True
        )

        break

    except Exception as e:

        print(
            f"Tentativa {attempt + 1}/20 -> {e}",
            flush=True
        )

        time.sleep(5)

if producer is None:

    raise Exception(
        "Não foi possível conectar ao Kafka"
    )

#
# Mongo
#

mongo_client = MongoClient(MONGO_URI)

db = mongo_client[MONGO_DATABASE]

collection = db[MONGO_COLLECTION]

print(
    "✅ Conectado ao MongoDB",
    flush=True
)


while True:

    try:

        appids = collection.distinct("appid")

        print(
            f"{len(appids)} appids encontrados",
            flush=True
        )

        total_sent = 0

        for appid in appids:

            try:

                response = requests.get(
                    f"https://store.steampowered.com/api/appdetails?appids={appid}",
                    timeout=30
                )

                response.raise_for_status()

                data = response.json()

                app_data = data.get(
                    str(appid),
                    {}
                )

                if not app_data.get("success"):
                    continue

                details = app_data.get(
                    "data",
                    {}
                )

                payload = {
                    "appid": appid,
                    "name": details.get("name"),
                    "game_type": details.get("type"),
                    "developers": details.get(
                        "developers",
                        []
                    ),
                    "publishers": details.get(
                        "publishers",
                        []
                    ),
                    "release_date": details.get(
                        "release_date",
                        {}
                    ).get("date"),
                    "genres": [
                        g.get("description")
                        for g in details.get(
                            "genres",
                            []
                        )
                    ],
                    "captured_at":
                        datetime.now(
                            UTC
                        ).isoformat()
                }

                producer.send(
                    "steam-games",
                    value=payload
                )

                total_sent += 1

                print(
                    f"✅ App {appid} enviado",
                    flush=True
                )

            except Exception as e:

                print(
                    f"❌ Erro appid={appid}: {e}",
                    flush=True
                )

        producer.flush()

        print(
            f"✅ {total_sent} jogos enviados para steam-games",
            flush=True
        )

    except Exception as e:

        print(
            f"❌ Erro geral: {e}",
            flush=True
        )

    print(
        f"Próxima execução em {INTERVAL_SECONDS} segundos",
        flush=True
    )

    time.sleep(INTERVAL_SECONDS)