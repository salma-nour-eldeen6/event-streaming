--- Bronze Job SQL
SET 'state.backend' = 'rocksdb';
SET 'state.backend.incremental' = 'true';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '10s';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'parallelism.default' = '1';

ADD JAR '/opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar';
ADD JAR '/opt/flink/lib/flink-json-1.18.1.jar';
ADD JAR '/opt/flink/lib/iceberg-flink-runtime-1.18-1.5.0.jar';
ADD JAR '/opt/flink/lib/hadoop-common-2.8.3.jar';
ADD JAR '/opt/flink/lib/hadoop-hdfs-2.8.3.jar';
ADD JAR '/opt/flink/lib/hadoop-client-2.8.3.jar';
ADD JAR '/opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar';
ADD JAR '/opt/flink/lib/bundle-2.20.18.jar';

SHOW JARS;


CREATE DATABASE IF NOT EXISTS iceberg.atlas_db;

-- 1.Define Flink SQL Kafka source schema that tells Flink how to read JSON fields from the Kafka topic.
DROP TABLE IF EXISTS atlas_source;

CREATE TABLE IF NOT EXISTS atlas_source (
    fw INT,
    mver STRING,
    lts INT,
    dst_name STRING,
    af INT,
    dst_addr STRING,
    src_addr STRING,
    proto STRING,
    ttl INT,
    size INT,
    dup INT,
    rcvd INT,
    sent INT,
    `min` DOUBLE,
    `max` DOUBLE,
    `avg` DOUBLE,
    msm_id BIGINT,
    prb_id BIGINT,
    `timestamp` BIGINT,
    msm_name STRING,
    `from` STRING,
    `type` STRING,
    step INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'atlas_measurements',
    'properties.bootstrap.servers' = 'kafka:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);
-- 2. Define Bronze table where Flink writes the selected fields
DROP TABLE IF EXISTS iceberg.atlas_db.bronze_measurements;

CREATE TABLE iceberg.atlas_db.bronze_measurements (
    fw INT,
    mver STRING,
    lts INT,
    dst_name STRING,
    af INT,
    dst_addr STRING,
    src_addr STRING,
    proto STRING,
    ttl INT,
    size INT,
    dup INT,
    rcvd INT,
    sent INT,
    min_value DOUBLE,
    max_value DOUBLE,
    avg_value DOUBLE,
    msm_id BIGINT,
    prb_id BIGINT,
    event_timestamp BIGINT,
    msm_name STRING,
    src_public_ip STRING,
    measurement_type STRING,
    step INT
)
WITH (
    'catalog-name' = 'iceberg',
    'format' = 'parquet'
);
-- 3.mapping from Kafka JSON into Iceberg  bronze table
INSERT INTO iceberg.atlas_db.bronze_measurements
SELECT
    fw,
    mver,
    lts,
    dst_name,
    af,
    dst_addr,
    src_addr,
    proto,
    ttl,
    size,
    dup,
    rcvd,
    sent,
    `min` AS min_value,
    `max` AS max_value,
    `avg` AS avg_value,
    msm_id,
    prb_id,
    `timestamp` AS event_timestamp,
    msm_name,
    `from` AS src_public_ip,
    `type` AS measurement_type,
    step
FROM atlas_source;