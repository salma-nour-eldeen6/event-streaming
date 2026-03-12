# flink_config.py
import os
from dataclasses import dataclass
from typing import List, Dict
from dotenv import load_dotenv

load_dotenv()

@dataclass
class FlinkConfig:
    """Minimal Flink configuration for Kafka → Iceberg → MinIO"""

    # Kafka
    kafka_bootstrap_servers: str = f"kafka:{os.getenv('KAFKA_INTERNAL_PORT', '29092')}"
    kafka_topic: str = "atlas_measurements"
    kafka_group_id: str = "flink-atlas-iceberg-consumer"
    kafka_startup_mode: str = "earliest-offset"

    # MinIO
    minio_endpoint: str = f"http://minio:{os.getenv('MINIO_API_PORT', '9000')}"
    minio_access_key: str = os.getenv('MINIO_ROOT_USER', 'none')
    minio_secret_key: str = os.getenv('MINIO_ROOT_PASSWORD', 'none')
    minio_bucket: str = "atlasevents"

    # Iceberg
    iceberg_catalog_name: str = "iceberg_catalog"
    iceberg_database: str = "atlas_db"
    iceberg_table: str = "measurements"
    iceberg_warehouse: str = f"s3a://atlasevents/warehouse"

    # Flink job
    job_parallelism: int = int(os.getenv('FLINK_PARALLELISM', '1'))
    job_name: str = "Atlas Kafka to Iceberg Pipeline"

    # Required JARs
    flink_lib_dir: str = "/opt/flink/flink/lib"
    required_jars: List[str] = None

    def __post_init__(self):
        if self.required_jars is None:
            self.required_jars = [
                "flink-sql-connector-kafka-3.1.0-1.18.jar",
                "iceberg-flink-runtime-1.18-1.9.2.jar",
                "aws-java-sdk-bundle-1.12.262.jar",
                "hadoop-aws-3.3.4.jar",
                "hadoop-common-3.3.4.jar",
                "hadoop-client-3.3.4.jar",
                "hadoop-hdfs-3.3.4.jar",
                "hadoop-auth-3.3.4.jar",
                "hadoop-hdfs-client-3.3.4.jar",
                "woodstox-core-6.4.0.jar",
                "stax2-api-4.2.1.jar"
            ]
        self.iceberg_warehouse = f"s3a://{self.minio_bucket}/warehouse"

    # Helpers with try-except for debugging
    def get_jar_paths(self) -> List[str]:
        try:
            return [f"file://{self.flink_lib_dir}/{jar}" for jar in self.required_jars]
        except Exception as e:
            print(f"Error in get_jar_paths: {e}")
            raise

    def get_minio_config(self) -> Dict[str, str]:
        try:
            return {
                "fs.s3a.endpoint": self.minio_endpoint,
                "fs.s3a.access.key": self.minio_access_key,
                "fs.s3a.secret.key": self.minio_secret_key,
                "fs.s3a.path.style.access": "true",
                "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "fs.s3a.connection.ssl.enabled": "false"
            }
        except Exception as e:
            print(f"Error in get_minio_config: {e}")
            raise

    def get_kafka_source_ddl(self) -> str:
        try:
            return f"""
            CREATE TEMPORARY TABLE atlas_source (
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
                `type` STRING,
                proc_time AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.kafka_topic}',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
                'properties.group.id' = '{self.kafka_group_id}',
                'scan.startup.mode' = '{self.kafka_startup_mode}',
                'format' = 'json',
                'json.ignore-parse-errors' = 'true'
            )
            """
        except Exception as e:
            print(f"Error in get_kafka_source_ddl: {e}")
            raise

    def get_iceberg_catalog_ddl(self) -> str:
        try:
            return f"""
            CREATE CATALOG {self.iceberg_catalog_name} WITH (
                'type' = 'iceberg',
                'catalog-type' = 'hadoop',
                'warehouse' = '{self.iceberg_warehouse}'
            )
            """
        except Exception as e:
            print(f"Error in get_iceberg_catalog_ddl: {e}")
            raise

    def get_iceberg_table_ddl(self) -> str:
        try:
            return f"""
            CREATE TABLE IF NOT EXISTS {self.iceberg_database}.{self.iceberg_table} (
                fw INT,
                mver STRING,
                dst_addr STRING,
                avg_value DOUBLE,
                min_value DOUBLE,
                max_value DOUBLE,
                sent INT,
                rcvd INT,
                msm_id BIGINT,
                prb_id BIGINT,
                event_timestamp BIGINT,
                measurement_type STRING,
                packet_loss DOUBLE,
                event_date STRING,
                event_hour INT,
                processing_time TIMESTAMP(3)
            ) PARTITIONED BY (event_date, event_hour) WITH (
                'connector'='iceberg'
            )
            """
        except Exception as e:
            print(f"Error in get_iceberg_table_ddl: {e}")
            raise
        
    def get_transform_query(self) -> str:
        try:
            return f"""
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
                CASE 
                    WHEN sent > 0 THEN (sent - rcvd) * 1.0 / sent
                    ELSE 0
                END AS packet_loss,
                DATE_FORMAT(
                    TO_TIMESTAMP_LTZ(`timestamp`, 3),
                    'yyyy-MM-dd'
                ) AS event_date,
                HOUR(
                    TO_TIMESTAMP_LTZ(`timestamp`, 3)
                ) AS event_hour,
                proc_time AS processing_time
            FROM atlas_source
            """
        except Exception as e:
            print(f"Error in get_transform_query: {e}")
            raise


flink_config = FlinkConfig()