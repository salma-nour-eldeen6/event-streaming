SET 'execution.runtime-mode' = 'batch';
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

USE CATALOG iceberg;

CREATE DATABASE IF NOT EXISTS atlas_db;

USE atlas_db;

DROP TABLE IF EXISTS iceberg.atlas_db.fact_network_quality;
DROP TABLE IF EXISTS iceberg.atlas_db.dim_probe;
DROP TABLE IF EXISTS iceberg.atlas_db.dim_destination;
DROP TABLE IF EXISTS iceberg.atlas_db.dim_datetime;

CREATE TABLE iceberg.atlas_db.dim_probe (
    prb_id BIGINT,
    src_addr STRING,
    fw INT,
    mver STRING
)
WITH (
    'catalog-name' = 'iceberg',
    'format' = 'parquet'
);

CREATE TABLE iceberg.atlas_db.dim_destination (
    dst_addr STRING,
    proto STRING,
    ip_version STRING
)
WITH (
    'catalog-name' = 'iceberg',
    'format' = 'parquet'
);

CREATE TABLE iceberg.atlas_db.dim_datetime (
    datetime_key STRING,
    event_date STRING,
    event_hour INT,
    event_year INT,
    event_month INT,
    event_day INT,
    day_period STRING
)
WITH (
    'catalog-name' = 'iceberg',
    'format' = 'parquet'
);
CREATE TABLE iceberg.atlas_db.fact_network_quality (
    datetime_key STRING,
    prb_id BIGINT,
    dst_addr STRING,

    total_measurements BIGINT,
    successful_measurements BIGINT,
    failed_measurements BIGINT,

    avg_latency_ms DOUBLE,
    min_latency_ms DOUBLE,
    max_latency_ms DOUBLE,

    avg_packet_loss DOUBLE,
    availability_rate DOUBLE,
    failure_rate DOUBLE,
    avg_packet_size DOUBLE
)
WITH (
    'catalog-name' = 'iceberg',
    'format' = 'parquet'
);
INSERT INTO iceberg.atlas_db.dim_probe
SELECT DISTINCT
    prb_id,
    src_addr,
    fw,
    mver
FROM iceberg.atlas_db.silver_ping;

INSERT INTO iceberg.atlas_db.dim_destination
SELECT DISTINCT
    dst_addr,
    proto,
    ip_version
FROM iceberg.atlas_db.silver_ping;

INSERT INTO iceberg.atlas_db.dim_datetime
SELECT DISTINCT
    CONCAT(event_date, '-', CAST(event_hour AS STRING)) AS datetime_key,
    event_date,
    event_hour,
    CAST(SUBSTRING(event_date, 1, 4) AS INT) AS event_year,
    CAST(SUBSTRING(event_date, 6, 2) AS INT) AS event_month,
    CAST(SUBSTRING(event_date, 9, 2) AS INT) AS event_day,
    CASE
        WHEN event_hour BETWEEN 0 AND 5 THEN 'night'
        WHEN event_hour BETWEEN 6 AND 11 THEN 'morning'
        WHEN event_hour BETWEEN 12 AND 17 THEN 'afternoon'
        ELSE 'evening'
    END AS day_period
FROM iceberg.atlas_db.silver_ping;

INSERT INTO iceberg.atlas_db.fact_network_quality
SELECT
    CONCAT(event_date, '-', CAST(event_hour AS STRING)) AS datetime_key,
    prb_id,
    dst_addr,

    COUNT(*) AS total_measurements,
    SUM(is_success) AS successful_measurements,
    SUM(is_failed) AS failed_measurements,

    AVG(avg_latency_ms) AS avg_latency_ms,
    MIN(min_latency_ms) AS min_latency_ms,
    MAX(max_latency_ms) AS max_latency_ms,

    AVG(packet_loss) AS avg_packet_loss,

    CAST(SUM(is_success) AS DOUBLE) / COUNT(*) AS availability_rate,
    CAST(SUM(is_failed) AS DOUBLE) / COUNT(*) AS failure_rate,

    AVG(size) AS avg_packet_size

FROM iceberg.atlas_db.silver_ping
GROUP BY
    event_date,
    event_hour,
    prb_id,
    dst_addr;