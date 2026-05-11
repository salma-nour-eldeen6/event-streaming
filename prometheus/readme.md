# Prometheus Scrape Config

Prometheus configuration that scrapes metrics from three services every **15 seconds**.

---

## Scrape Jobs

| Job | Target | Description |
|---|---|---|
| `flink` | `flink-jobmanager:18081` | Flink Job Manager metrics |
| `kafka` | `kafka-exporter:9308` | Kafka broker metrics via exporter |
| `cadvisor` | `cadvisor:8080` | Container resource usage metrics |

---

## Usage

```bash
prometheus --config.file=prometheus.yml
```

Or with Docker Compose:

```yaml
volumes:
  - ./prometheus.yml:/etc/prometheus/prometheus.yml
```