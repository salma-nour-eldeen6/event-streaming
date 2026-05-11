#!/bin/bash

# Config
KAFKA_CONTAINER_NAME=kafka     
TOPIC_NAME=atlas_measurements       
PARTITIONS=4                   
REPLICATION=1                   
BOOTSTRAP_SERVER=kafka:9092     

# Create topic
echo "Creating topic '$TOPIC_NAME' in Kafka..."
docker exec -i $KAFKA_CONTAINER_NAME kafka-topics \
  --create \
  --topic $TOPIC_NAME \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION \
  --if-not-exists \
  --bootstrap-server $BOOTSTRAP_SERVER

# List topics
echo "Current topics in Kafka:"
docker exec -i $KAFKA_CONTAINER_NAME kafka-topics \
  --list \
  --bootstrap-server $BOOTSTRAP_SERVER
