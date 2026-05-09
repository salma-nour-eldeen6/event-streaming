# Sample Data

This folder contains sample exported data generated from the streaming pipeline.

## Overview

The dataset consists of RIPE Atlas network measurement records processed through the Bronze and Silver layers of the pipeline.

The sample data is stored in Parquet format for efficient analytical querying and columnar storage optimization.

## Purpose

The exported sample files are used for:

- Exploratory Data Analysis (EDA)
- Data validation
- Pipeline verification
- Visualization preparation
- Notebook experimentation

## Data Contents

The dataset includes network measurement attributes such as:

- Probe identifiers
- Destination addresses
- Packet statistics
- Latency measurements
- Packet loss metrics
- Event timestamps
- Measurement types

## File Format

The project uses Apache Parquet as the primary analytical storage format due to:

- Columnar storage efficiency
- Compression support
- Faster analytical reads
- Compatibility with big data tools

## Notes

The data stored in this folder represents sample exported records and not the complete streaming dataset stored in Iceberg tables and MinIO object storage.
