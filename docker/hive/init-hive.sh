#!/bin/bash

set -e

echo "Esperando Hive Server2 iniciar..."

until beeline \
  -u jdbc:hive2://hive-server:10000/default \
  -e "SHOW DATABASES;" >/dev/null 2>&1
do
  echo "Hive ainda não está pronto. Aguardando 5 segundos..."
  sleep 5
done

echo "Hive Server2 está pronto."

echo "Executando init.sql..."

beeline \
  -u jdbc:hive2://hive-server:10000/default \
  -f /scripts/init.sql

echo "Hive inicializado com sucesso."