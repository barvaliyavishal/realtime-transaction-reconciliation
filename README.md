# Real-Time Transaction Reconciliation

A streaming card-transaction pipeline with batch reconciliation, built on Kafka, Spark Structured Streaming, Apache Iceberg, and AWS.

Synthetic card transactions are produced to Kafka, consumed and parsed by Spark Structured Streaming, and written into a governed Apache Iceberg table on S3 through the AWS Glue Data Catalog. The table is queryable directly in Athena. A daily batch job (in progress) independently recomputes aggregates and reconciles them against the streaming output to catch drift.

## Architecture

Python producer to a Kafka topic, Spark Structured Streaming consumes and parses the JSON into typed columns, writes to an Iceberg bronze table on S3 registered in the Glue Data Catalog, and Athena queries the table with plain SQL. A daily batch reconciliation job is planned.

The streaming path writes the data. Athena reads it back through a different engine. Glue is the shared catalog both sides talk to, which is what makes the lakehouse pattern work: write with one tool, read with another, the catalog is the source of truth for schema and table location.

## Tech stack

- Apache Kafka 3.7.0 (KRaft mode, local via Docker Compose)
- Python producer (kafka-python) generating synthetic card transactions
- Apache Spark 3.5.1 (PySpark, Structured Streaming)
- Apache Iceberg 1.9.1 (bronze table format)
- AWS S3 (storage), AWS Glue Data Catalog (table catalog), Amazon Athena (query)
- Airflow for the batch reconciliation job (planned, Phase 3)

## Status

- Phase 1: Local Kafka producer streaming synthetic card transactions. Done.
- Phase 2: Spark Structured Streaming consumes the Kafka stream, parses transactions into typed columns, and writes them into an Apache Iceberg bronze table on S3 via the Glue catalog. Verified by reading the table back in Athena. Done.
- Phase 3: Daily batch reconciliation job on Airflow that recomputes aggregates from source and reconciles them against the streaming output to flag drift. In progress.

## Running locally

1. Start Kafka: `docker compose up -d`
2. Install dependencies: `pip install kafka-python pyspark==3.5.1`
3. Start the producer and leave it running: `python dev/producer.py`
4. Run the streaming job, which writes to Iceberg on S3 via Glue: `python dev/spark_stream.py`
5. Query the table in Athena: `SELECT * FROM db.transactions_bronze LIMIT 20;`

AWS credentials are read from the local AWS CLI configuration. An S3 bucket and the us-east-1 region are assumed.

## Notes on production tradeoffs

A few choices here are deliberately simplified for a local-compute, cloud-storage setup, and would change in a real deployment:

- The streaming checkpoint is kept on local disk. In production it would live in durable cloud storage (S3 via the hadoop-aws filesystem) so the job can recover across machines.
- The IAM user uses broad managed policies for convenience. Production would use a least-privilege policy scoped to the specific bucket and Glue database.
- Spark runs in local mode, which produces some harmless heartbeat warnings on Windows that do not occur on a real cluster.