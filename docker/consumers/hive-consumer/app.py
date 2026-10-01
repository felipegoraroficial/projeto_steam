import json
import os
import time

from kafka import KafkaConsumer
from pyhive import hive
from dotenv import load_dotenv

load_dotenv()


def deserialize_message(message):

    try:

        return json.loads(
            message.decode("utf-8")
        )

    except Exception as e:

        print(
            f"Mensagem ignorada: {e}",
            flush=True
        )

        return None


print(
    "======================================",
    flush=True
)

print(
    "Hive Consumer",
    flush=True
)

print(
    "======================================",
    flush=True
)

consumer = None

for attempt in range(30):

    try:

        consumer = KafkaConsumer(
            "steam-games",
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP"
            ),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="hive-games-group",
            value_deserializer=deserialize_message
        )

        print(
            "✅ Conectado ao Kafka",
            flush=True
        )

        break

    except Exception as e:

        print(
            f"Erro Kafka: {e}",
            flush=True
        )

        time.sleep(10)

if consumer is None:

    raise Exception(
        "Não foi possível conectar ao Kafka"
    )


print(
    "Conectando ao Hive...",
    flush=True
)

while True:
    try:
        conn = hive.Connection(
            host=os.getenv("HIVE_HOST"),
            port=int(os.getenv("HIVE_PORT")),
            database=os.getenv("HIVE_DATABASE")
        )

        cursor = conn.cursor()

        print("✅ Conectado ao Hive", flush=True)
        break

    except Exception as e:
        print(f"Hive indisponível: {e}", flush=True)
        time.sleep(5)


for message in consumer:

    if message.value is None:
        continue

    try:

        payload = message.value

        appid = payload.get("appid")

        print(
            f"Recebido: {appid}",
            flush=True
        )

        name = (
            payload.get("name", "")
            .replace("'", " ")
        )

        game_type = (
            payload.get("game_type", "")
            .replace("'", " ")
        )

        developers = ",".join(
            payload.get(
                "developers",
                []
            )
        ).replace("'", " ")

        publishers = ",".join(
            payload.get(
                "publishers",
                []
            )
        ).replace("'", " ")

        genres = ",".join(
            payload.get(
                "genres",
                []
            )
        ).replace("'", " ")

        release_date = (
            payload.get(
                "release_date",
                ""
            )
            .replace("'", " ")
        )

        captured_at = str(
            payload.get(
                "captured_at", "")
        )[:19]

        query = f"""
        INSERT INTO steam_games
        VALUES (
            {appid},
            '{name}',
            '{game_type}',
            '{developers}',
            '{publishers}',
            '{release_date}',
            '{genres}',
            TIMESTAMP '{captured_at}'
        )
        """

        cursor.execute(query)

        print(
            f"✅ Inserido appid={appid}",
            flush=True
        )

    except Exception as e:

        print(
            f"❌ Erro: {e}",
            flush=True
        )