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

DROP TABLE IF EXISTS iceberg.atlas_db.fact_network_quality;
DROP TABLE IF EXISTS iceberg.atlas_db.dim_probe;
DROP TABLE IF EXISTS iceberg.atlas_db.dim_destination;
DROP TABLE IF EXISTS iceberg.atlas_db.dim_datetime;

CREATE TABLE iceberg.atlas_db.dim_probe (
    prb_id BIGINT,
    src_addr STRING,
    fw INT,
    mver STRING,
    ip_version STRING
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
    year INT,
    month INT,
    day INT,
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