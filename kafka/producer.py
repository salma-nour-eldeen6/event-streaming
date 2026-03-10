import asyncio
import json
import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError 

# Kafka setup
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka producer initialized successfully")
except KafkaError as e:
    print("Failed to initialize Kafka producer:", e)
    exit(1)

TOPIC = "atlas_measurements"

# WebSocket callbacks
def on_open(ws):
    print("Connected to RIPE Atlas stream")
    try:
        subscribe_msg = json.dumps([
            "atlas_subscribe", {"streamType": "result"}
        ])
        ws.send(subscribe_msg)
        print("Subscription message sent")
    except Exception as e:
        print("Error sending subscription message:", e)

def on_message(ws, message):
    try:
        event_type, payload = json.loads(message)
        key = str(payload.get("prb_id", "unknown"))
        future = producer.send(TOPIC, key=key, value=payload)
        # Add callback for success / error
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
    except Exception as e:
        print("Error parsing or sending message:", e)

def on_send_success(record_metadata):
    print(f"Message sent to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    print("Error sending message to Kafka:", excp)

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed:", close_status_code, close_msg)

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
        print("WebSocket stopped by user")
    except Exception as e:
        print("Unexpected error running WebSocket:", e)