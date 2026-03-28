#!/bin/bash
echo "Starting Docker..."
docker compose up -d
sleep 30
echo "Copying scripts..."
docker cp scripts/spark_stream_bronze_ingestion_data.py de_spark_master:/opt/bitnami/spark/scripts/
docker cp scripts/spark_stream_silver_transform_data.py de_spark_master:/opt/bitnami/spark/scripts/
docker cp scripts/spark_stream_gold_aggregate_modelling_data.py de_spark_master:/opt/bitnami/spark/scripts/
docker cp .env de_spark_master:/opt/bitnami/spark/scripts/.env
echo "Ready! Now run the Kafka producer and Spark jobs."
