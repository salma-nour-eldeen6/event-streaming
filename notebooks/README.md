## 📊 Exploratory Data Analysis (EDA)

As part of building the real-time data pipeline, an exploratory data analysis (EDA) step was performed on a sampled dataset extracted from the Iceberg Bronze layer.

---

### 🎯 Objective

The goal of this EDA was to:

- Understand the structure of the RIPE Atlas streaming data
- Identify data quality issues
- Analyze field distributions and completeness
- Guide the design of the Silver layer

---

### 🔍 Key Findings

#### 1. Heterogeneous Data Structure

The RIPE Atlas stream contains multiple measurement types, including:

- ping
- http
- traceroute
- dns
- sslcert
- ntp

Each measurement type has its own schema and semantics, meaning not all fields are present in every record.

---

#### 2. High Null Values Explained

Initial analysis showed high percentages of missing values across several columns such as:

- avg_value
- min_value
- max_value
- sent
- rcvd

However, deeper inspection revealed that:

> These null values are not data quality issues, but are caused by mixing multiple measurement types with different schemas.

Examples:

- Ping measurements include latency and packet statistics → fields are populated
- HTTP, DNS, SSL, and NTP measurements do not include these metrics → fields appear as null

---

#### 3. Measurement-Type-Specific Field Availability

By analyzing each measurement type separately:

- Ping data was found to be clean, consistent, and complete for network performance analysis
- Other measurement types have different structures and therefore contain nulls in unrelated fields

---

### 🧠 Data-Driven Design Decisions

Based on the EDA results, the following decisions were made:

#### ✅ Focus Silver Layer on Ping Measurements

To ensure clean and meaningful analytics:

- The Silver layer filters data using:

This allows:

- Accurate latency analysis
- Reliable packet loss computation
- Consistent schema without excessive null values

---

#### 🥉 Bronze Layer Role

The Bronze layer stores all incoming data with minimal transformation:

- Preserves raw structure
- Supports future extensibility
- Keeps all measurement types for potential future use

---

#### 🥈 Silver Layer Role

The Silver layer:

- Filters relevant data (ping only)
- Cleans and standardizes fields
- Adds derived metrics such as:
  - packet loss
  - event_date
  - event_hour

---

### 🚀 Impact of EDA

This EDA step was critical in:

- Identifying that the dataset is multi-schema
- Correctly explaining high null values
- Avoiding incorrect assumptions about data quality
- Enabling data-driven modeling decisions
- Improving the design of the medallion architecture

---

### 📌 Summary

EDA revealed that null values were primarily caused by schema differences across measurement types, not by missing or corrupted data.

This insight led to filtering strategies in the Silver layer, ensuring clean, reliable, and meaningful analytical datasets.
