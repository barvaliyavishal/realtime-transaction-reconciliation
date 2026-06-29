# Real-Time Transaction Reconciliation

Streaming card-transaction pipeline with batch reconciliation.
Kafka to Spark Structured Streaming to Apache Iceberg on S3, with a daily Airflow batch job that reconciles streaming output against recomputed aggregates to catch drift.

## Status
Phase 1: local Kafka producer streaming synthetic transactions. Done.