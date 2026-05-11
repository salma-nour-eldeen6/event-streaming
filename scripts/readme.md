# RIPE Atlas → Kafka → Flink → Iceberg Pipeline

A production-grade streaming data pipeline that ingests real-time network measurements from [RIPE Atlas](https://atlas.ripe.net/), processes them through Apache Kafka and Apache Flink, and stores them in Apache Iceberg tables on MinIO (S3-compatible storage) following the **Medallion Architecture**.

---

## Architecture

```
RIPE Atlas WebSocket
        │
        ▼
  producer.py ──────► Kafka (atlas_measurements, 4 partitions)
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
                 Bronze     Silver     Gold
               (raw data) (cleaned) (aggregated)
                    └─────────┴─────────┘
                              │
                              ▼
                    Iceberg Tables (MinIO / S3)
```

### Medallion Layers

| Layer | Content | Mode |
|---|---|---|
| **Bronze** | Raw data exactly as received from WebSocket | Continuous |
| **Silver** | Validated, cleaned, and enriched records | Continuous |
| **Gold** | Aggregated metrics (e.g. daily averages per probe) | Scheduled |

---

## Project Structure

```
scripts/
├── create_kafka_topic.sh   # Creates the Kafka topic
├── run_kafka_producer.sh   # Starts the Python WebSocket → Kafka producer
├── run_bronze_job.sh       # Submits Flink SQL job for Bronze layer
├── run_silver_job.sh       # Submits Flink SQL job for Silver layer
├── run_gold_job.sh         # Submits Flink SQL job for Gold layer
 
```

---

## Script Reference

### `create_kafka_topic.sh`
Creates the `atlas_measurements` Kafka topic inside the Kafka Docker container.

- **Partitions:** 4 (allows up to 4 parallel consumers)
- **Replication factor:** 1
- Safe to re-run — uses `--if-not-exists`

```bash
bash scripts/create_kafka_topic.sh
```

---

### `run_kafka_producer.sh`
Sets up a Python virtual environment and runs `producer.py`, which streams measurement events from the RIPE Atlas WebSocket into Kafka.

- Creates `.venv` if it doesn't exist
- Installs dependencies from `requirements.txt`
- Deactivates the environment cleanly on exit

```bash
bash scripts/run_kafka_producer.sh
```

---

### `run_bronze_job.sh`
Submits a Flink SQL job that reads raw events from Kafka and writes them to the Bronze Iceberg table. Runs in **detached mode** (background).

```bash
bash scripts/run_bronze_job.sh
```

---

### `run_silver_job.sh`
Submits a Flink SQL job that reads from Bronze, applies validation and enrichment, and writes to the Silver Iceberg table. Runs in **detached mode** (background).

```bash
bash scripts/run_silver_job.sh
```

---

### `run_gold_job.sh`
Submits a Flink SQL job that aggregates Silver data and writes business-level metrics to the Gold Iceberg table. Runs in **foreground** (waits for completion).

```bash
bash scripts/run_gold_job.sh
```

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- Services running: Kafka, Flink (with `sql-client`), MinIO

### Run Steps Individually

```bash
bash scripts/create_kafka_topic.sh
bash scripts/run_bronze_job.sh
bash scripts/run_silver_job.sh
bash scripts/run_gold_job.sh
bash scripts/run_kafka_producer.sh
```
---

## Storage

| Property | Value |
|---|---|
| Iceberg Database | `atlas_db` |
| MinIO Bucket | `warehouse` |
 