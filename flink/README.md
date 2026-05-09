# Flink Streaming Layer

This folder contains the Apache Flink SQL streaming and batch processing jobs used in the RIPE Atlas event-streaming pipeline.

## Overview

Apache Flink is responsible for processing RIPE Atlas measurements using a Medallion Architecture approach:

- Bronze Layer → Raw ingestion
- Silver Layer → Cleaned and transformed data
- Gold Layer → Aggregated analytical tables

The pipeline integrates Apache Kafka, Apache Iceberg, and MinIO object storage through Flink SQL jobs running inside a custom SQL Client container.

## Custom SQL Client

A custom Flink SQL Client Docker image was built using:

- Flink 1.18.1
- Scala 2.12
- Java 11

Additional connectors and dependencies were manually added to support:

- Kafka streaming integration
- JSON parsing
- Apache Iceberg catalogs
- Hadoop filesystem support
- S3-compatible MinIO storage access

## Flink Configuration

The Flink environment was configured with:

- RocksDB state backend
- Incremental checkpointing
- EXACTLY_ONCE processing guarantees
- Streaming checkpoint intervals
- Filesystem checkpoint storage
- Custom parallelism settings

## Bronze Layer

The Bronze layer consumes raw RIPE Atlas measurements from Kafka topics using Flink SQL Kafka connectors.

### Responsibilities

- Read streaming JSON measurements from Kafka
- Parse network measurement fields
- Preserve near-raw event data
- Store streaming records in Iceberg tables

The Bronze layer acts as the ingestion layer of the pipeline while keeping the original measurement structure as intact as possible.

## Silver Layer

The Silver layer processes and transforms ping measurements from the Bronze tables into cleaner analytical datasets.

### Transformations

- IPv4/IPv6 classification
- Packet loss calculation
- Success/failure flag generation
- Timestamp conversion
- Event date and hour extraction
- Latency normalization

### Validation Logic

The Silver pipeline filters invalid or inconsistent records by validating:

- Packet counts
- Latency relationships
- TTL ranges
- Measurement completeness
- Supported IP protocol families

Only valid ping measurements are promoted to the Silver layer.

## Gold Layer

The Gold layer generates analytical dimension and fact tables for monitoring and visualization.

### Dimension Tables

- Probe dimension
- Destination dimension
- Datetime dimension

### Fact Table Metrics

The fact table aggregates:

- Total measurements
- Successful measurements
- Failed measurements
- Average latency
- Packet loss
- Availability rates
- Failure rates
- Average packet size

Gold jobs run in batch mode to generate business-ready network quality analytics for Grafana dashboards.

## Processing Modes

The project uses:

- Streaming mode for Bronze and Silver layers
- Batch mode for Gold aggregations
