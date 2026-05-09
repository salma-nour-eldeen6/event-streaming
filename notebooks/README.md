# Exploratory Data Analysis (EDA)

This folder contains exploratory analysis notebooks used during the development of the RIPE Atlas streaming pipeline.

The analysis was performed on sampled Parquet data exported from the Bronze layer Iceberg tables.

## Purpose

The EDA phase was used to better understand the structure and behavior of the incoming RIPE Atlas measurements before designing the Silver transformation layer.

The analysis focused on:

- measurement distributions
- field completeness
- schema variability
- latency-related metrics
- data consistency across measurement types

## Observations

### Multiple Measurement Types

The incoming RIPE Atlas stream contains several measurement types, including:

- ping
- http
- traceroute
- dns
- sslcert
- ntp

During analysis, it became clear that the dataset does not follow a single unified schema.
Different measurement types populate different fields depending on the protocol and measurement behavior.

For example:

- ping measurements contain latency and packet statistics
- DNS and HTTP measurements do not contain RTT-related fields
- some records naturally contain null values for fields that are irrelevant to their measurement type

## Understanding Null Values

Initial inspection showed high null percentages in columns such as:

- avg_value
- min_value
- max_value
- sent
- rcvd

At first glance, this appeared to be a data quality issue.

However, after grouping records by measurement type, it became clear that the null values were primarily caused by schema differences between measurement categories rather than corrupted or missing data.

This analysis prevented incorrect assumptions about dataset quality.

## Data-Driven Pipeline Decisions

The EDA directly influenced the design of the Medallion Architecture layers.

### Bronze Layer Design

The Bronze layer was intentionally designed to preserve near-raw incoming measurements with minimal transformation.

This approach:

- keeps all measurement types
- preserves original event structures
- supports future extensibility
- avoids premature filtering

### Silver Layer Design

The analysis showed that ping measurements were the most complete and consistent source for network quality analytics.

As a result, the Silver layer was designed specifically around ping measurements.

The Silver Flink job:

- filters non-ping measurements
- validates latency relationships
- checks packet consistency
- validates TTL and protocol values
- generates derived analytical fields

Additional derived columns include:

- packet_loss
- is_success
- is_failed
- event_date
- event_hour
- ip_version

This resulted in a cleaner and more reliable analytical dataset with significantly reduced irrelevant null values.

## Impact on the Project

The EDA phase played an important role in shaping the streaming architecture and transformation logic.

It helped:

- explain schema variability in the dataset
- validate assumptions about data quality
- guide Silver layer transformations
- improve analytical consistency
- support more reliable Gold layer aggregations

## Summary

The exploratory analysis revealed that the RIPE Atlas stream behaves as a multi-schema dataset where field availability depends heavily on the measurement type.

These findings directly influenced the design of the Bronze and Silver Flink jobs and led to more reliable downstream analytics.
