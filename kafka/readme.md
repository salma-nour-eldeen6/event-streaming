# RIPE Atlas → Kafka Producer

A lightweight Python service that streams real-time network measurement data from the [RIPE Atlas](https://atlas.ripe.net/) WebSocket API and publishes it to a Kafka topic.

---

## Project Structure

```
.
├── producer.py       # Main entry point
└── README.md
```

---

## How It Works

1. Opens a WebSocket connection to the RIPE Atlas stream (`atlas-stream.ripe.net`)
2. Subscribes to live measurement results
3. Validates and enriches each incoming payload (adds `event_id` and `ingestion_time`)
4. Publishes messages to the Kafka topic `atlas_measurements`, keyed by `prb_id`

---

## Kafka Topic

| Property | Value |
|---|---|
| Topic | `atlas_measurements` |
| Key | `prb_id` (probe ID) |
| Value | JSON-encoded measurement payload |
| Broker | `localhost:9092` |

---

## Payload Fields

Each message sent to Kafka includes the original RIPE Atlas fields plus:

| Field | Description |
|---|---|
| `event_id` | Unique UUID generated at ingestion |
| `ingestion_time` | UTC timestamp of when the message was received |

---

## Stopping

Press `Ctrl+C` — the producer will flush and close cleanly before exiting.