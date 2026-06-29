# Real-Time Transaction Reconciliation

Streaming card-transaction pipeline with batch reconciliation.
Kafka to Spark Structured Streaming to Apache Iceberg on S3, with a daily Airflow batch job that reconciles streaming output against recomputed aggregates to catch drift.

## Status
Phase 1: local Kafka producer streaming synthetic card transactions. Done.

Phase 2: Spark Structured Streaming consumes the Kafka stream, parses transactions into typed columns, and writes them into an Apache Iceberg bronze table. Verified by reading the table back. Done.

Next: swap the local Iceberg catalog for S3 via AWS Glue, so the bronze table lands in the cloud.
