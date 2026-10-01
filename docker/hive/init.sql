SET hive.metastore.warehouse.dir=/tmp/warehouse;

CREATE DATABASE IF NOT EXISTS estudo;

USE estudo;

CREATE TABLE IF NOT EXISTS clientes (
    id INT,
    nome STRING,
    cidade STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/data/sample.csv'
OVERWRITE INTO TABLE clientes;

CREATE DATABASE IF NOT EXISTS steam;

USE steam;

CREATE TABLE IF NOT EXISTS steam_games (
    appid BIGINT,
    name STRING,
    game_type STRING,
    developers STRING,
    publishers STRING,
    release_date STRING,
    genres STRING,
    captured_at TIMESTAMP
)
STORED AS PARQUET;