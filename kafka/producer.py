import asyncio
import json
from datetime import datetime
import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError 
import uuid
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Kafka setup
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Kafka producer initialized successfully")
except KafkaError as e:
    print("Failed to initialize Kafka producer:", e)
    exit(1)

TOPIC = "atlas_measurements"
INVALID_TOPIC = "atlas_invalid_measurements"

# WebSocket callbacks
def on_open(ws):
    logger.info("Connected to RIPE Atlas stream")
    try:
        subscribe_msg = json.dumps([
            "atlas_subscribe", {"streamType": "result"}
        ])
        ws.send(subscribe_msg)
        logger.info("Subscription message sent")
    except Exception as e:
        logger.error("Error sending subscription message:", e)

def on_message(ws, message):
    try:
        event_type, payload = json.loads(message)

        if not validate(payload):
            producer.send(INVALID_TOPIC, value={
                "error": "validation_failed",
                "payload": payload
})
            return

        payload = enrich(payload)

        key = str(payload.get("prb_id", "unknown"))

        future = producer.send(TOPIC, key=key, value=payload)
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)

    except Exception as e:
        logger.error("Error parsing or sending message:", e)


def on_send_success(record_metadata):
    logger.info(f"Message sent to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    logger.error("Error sending message to Kafka:", excp)

def on_error(ws, error):
    logger.error("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    logger.info("WebSocket closed:", close_status_code, close_msg)

# add metadata enrichment to the payload before sending to Kafka
def enrich(payload):
    enriched = payload.copy()
    enriched["event_id"] = str(uuid.uuid4())
    enriched["ingestion_time"] = datetime.utcnow().isoformat()
    return enriched

# Basic validation to ensure payload has required fields before sending to Kafka
def validate(payload):
    if not isinstance(payload, dict):
        return False
    
    # required fields (RIPE Atlas important ones)
    if "prb_id" not in payload:
        return False
    
    if "timestamp" not in payload:
        return False

    return True


if __name__ == "__main__":
    ws_url = "wss://atlas-stream.ripe.net/stream/?client=docs-example"
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    try:
        ws.run_forever()
    except KeyboardInterrupt:
        logger.info("WebSocket stopped by user")
    except Exception as e:
        logger.error("Unexpected error running WebSocket:", e)
    finally:
        logger.info("Flushing and closing Kafka producer...")
        producer.flush()
        producer.close()
