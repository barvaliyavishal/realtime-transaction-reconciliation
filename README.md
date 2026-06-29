# realtime-transaction-reconciliation
Real-time card transaction pipeline with streaming and batch reconciliation. Kafka to Spark Structured Streaming to Apache Iceberg on S3, with a daily Airflow batch job that recomputes aggregates and reconciles them against streaming output to catch drift.
