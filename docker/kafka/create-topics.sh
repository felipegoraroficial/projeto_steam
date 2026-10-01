#!/bin/bash

sleep 20

kafka-topics \
 --create \
 --if-not-exists \
 --bootstrap-server kafka1:9092 \
 --topic steam-games \
 --partitions 1 \
 --replication-factor 1

kafka-topics \
 --create \
 --if-not-exists \
 --bootstrap-server kafka1:9092 \
 --topic steam-playtime \
 --partitions 1 \
 --replication-factor 1a#!/bin/bash

sleep 20

echo "Criando tópico steam-games..."

kafka-topics \
 --create \
 --if-not-exists \
 --bootstrap-server kafka1:9092 \
 --topic steam-games \
 --partitions 1 \
 --replication-factor 1 \
 --config retention.ms=86400000

echo "Criando tópico steam-playtime..."

kafka-topics \
 --create \
 --if-not-exists \
 --bootstrap-server kafka1:9092 \
 --topic steam-playtime \
 --partitions 1 \
 --replication-factor 1 \
 --config retention.ms=86400000

echo "Tópicos criados com sucesso."