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

DROP CATALOG IF EXISTS iceberg;
CREATE CATALOG iceberg WITH (
    'type' = 'iceberg',
    'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://warehouse/',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    'client.region' = 'us-east-1',
    's3.access-key-id' = 'admin',
    's3.secret-access-key' = 'password'
);

CREATE DATABASE IF NOT EXISTS iceberg.atlas_db;
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
    min DOUBLE,
    max DOUBLE,
    avg DOUBLE,
    msm_id BIGINT,
    prb_id BIGINT,
    `timestamp` BIGINT,
    msm_name STRING,
    `from` STRING,
    `type` STRING,
    step INT
)
WITH (
    'catalog-name' = 'iceberg',
    'format' = 'parquet'
);


DROP TABLE IF EXISTS atlas_source;
CREATE TABLE IF NOT EXISTS atlas_source (
    fw INT,
    mver STRING,
    dst_addr STRING,
    `avg` DOUBLE,
    `min` DOUBLE,
    `max` DOUBLE,
    sent INT,
    rcvd INT,
    msm_id BIGINT,
    prb_id BIGINT,
    `timestamp` BIGINT,
    `type` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'atlas_measurements',
    'properties.bootstrap.servers' = 'kafka:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

INSERT INTO iceberg.atlas_db.measurements
SELECT
    fw,
    mver,
    dst_addr,
    `avg` AS avg_value,
    `min` AS min_value,
    `max` AS max_value,
    sent,
    rcvd,
    msm_id,
    prb_id,
    `timestamp` AS event_timestamp,
    `type` AS measurement_type,

    CAST(
        CASE
            WHEN sent > 0 THEN (sent - rcvd) * 1.0 / sent
            ELSE 0
        END AS DOUBLE
    ) AS packet_loss,

    DATE_FORMAT(TO_TIMESTAMP_LTZ(`timestamp`, 3), 'yyyy-MM-dd') AS event_date,

    CAST(
        HOUR(TO_TIMESTAMP_LTZ(`timestamp`, 3))
        AS INT
    ) AS event_hour
FROM atlas_source;