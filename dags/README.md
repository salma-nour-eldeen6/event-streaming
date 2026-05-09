# Airflow DAGs

This folder contains Apache Airflow DAGs used to orchestrate the end-to-end event streaming pipeline.

## DAG Overview

The DAG automates the execution flow of the RIPE Atlas streaming pipeline using Apache Airflow and Dockerized services.

The workflow coordinates:

- Kafka topic creation
- Flink Bronze job execution
- Flink Silver job execution
- Kafka producer execution
- Gold layer execution after sufficient processed data is generated

## DAG Flow

The orchestration follows this sequence:

1. Verify mounted shell scripts inside the Airflow container
2. Verify Docker access from Airflow
3. Create Kafka topic
4. Start Bronze Flink streaming job
5. Start Silver Flink transformation job
6. Start Kafka producer to stream RIPE Atlas measurements
7. Wait for Silver layer data generation
8. Execute Gold aggregation jobs

## Technologies Used

- Apache Airflow
- BashOperator
- TimeDeltaSensor
- Docker Compose
- Apache Kafka
- Apache Flink
- Apache Iceberg
- MinIO Object Storage

## Workflow Architecture

RIPE Atlas → Kafka Producer → Kafka Topic → Flink Bronze → Flink Silver → Flink Gold → Iceberg/MinIO → Grafana

## Notes

The DAG uses `TimeDeltaSensor` to delay Gold layer execution until enough processed Silver data becomes available for aggregation.

Streaming jobs are executed using Bash scripts mounted inside the Airflow container.
