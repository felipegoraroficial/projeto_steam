import os
import json
import time
import requests

from datetime import datetime, UTC

from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
STEAM_ID = os.getenv("STEAM_ID")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")

INTERVAL_SECONDS = int(
    os.getenv("INTERVAL_SECONDS", "300")
)

print("Aguardando Kafka...", flush=True)

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


while True:

    try:

        print(
            "Consultando Steam API...",
            flush=True
        )

        url = (
            "http://api.steampowered.com/"
            "IPlayerService/GetOwnedGames/v0001/"
            f"?key={API_KEY}"
            f"&steamid={STEAM_ID}"
            "&format=json"
        )

        response = requests.get(
            url,
            timeout=30
        )

        response.raise_for_status()

        data = response.json()

        games = data["response"]["games"]

        captured_at = datetime.now(
            UTC
        ).isoformat()

        print(
            f"{len(games)} jogos encontrados",
            flush=True
        )

        total_sent = 0

        for game in games:

            payload = {
                "steam_id": STEAM_ID,
                "appid": game.get("appid"),
                "playtime_forever": game.get(
                    "playtime_forever",
                    0
                ),
                "playtime_windows_forever": game.get(
                    "playtime_windows_forever",
                    0
                ),
                "playtime_mac_forever": game.get(
                    "playtime_mac_forever",
                    0
                ),
                "playtime_linux_forever": game.get(
                    "playtime_linux_forever",
                    0
                ),
                "captured_at": captured_at
            }

            producer.send(
                "steam-playtime",
                value=payload
            )

            total_sent += 1

        producer.flush()

        print(
            f"✅ {total_sent} mensagens enviadas",
            flush=True
        )

    except Exception as e:

        print(
            f"❌ Erro: {e}",
            flush=True
        )

    print(
        f"Próxima execução em "
        f"{INTERVAL_SECONDS} segundos",
        flush=True
    )

    time.sleep(INTERVAL_SECONDS)