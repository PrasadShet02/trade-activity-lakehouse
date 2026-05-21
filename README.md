# 🌊 Real-Time Trade Activity Lakehouse

![Python](https://img.shields.io/badge/Python-3.11-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.5.1-orange.svg)
![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-1.5.0-blue)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.4.0-black)

A production-grade, real-time data lakehouse built to ingest, process, and store financial trade events. This project demonstrates enterprise data engineering patterns including **Exactly-Once Semantics (EOS)**, **Partition Hashing**, and **Slowly Changing Dimensions (SCD) Type 2** using Apache Iceberg's ACID capabilities.

## 🏗️ Architecture

1. **Producer Layer**: A Python mock generator that enforces event ordering by hashing Kafka partitions via the `symbol` key.
2. **Ingestion Layer**: A dockerized single-node Apache Kafka & Zookeeper cluster.
3. **Compute Layer**: PySpark Structured Streaming consumer utilizing Write-Ahead Logs (WAL) and checkpointing for failure resiliency.
4. **Storage Layer**: Apache Iceberg acting as a Lakehouse ledger, processing `MERGE INTO` operations via Spark SQL to atomically append and close out trade states.

## ✨ Key Features

- **SCD Type 2 Ledger**: Uses an advanced `UNION ALL` technique within Iceberg's `MERGE INTO` to atomically process updates (closing old records and inserting new ones) in a single transaction.
- **Partition Hashing**: Guarantees strict chronological event ordering for identical stock tickers.
- **Zero-Downtime Schema Evolution**: Demonstrates Iceberg's native schema evolution capabilities (`ALTER TABLE`) without needing full-table rewrites.
- **Storage Optimization**: Automated Spark batch procedures for data file compaction, snapshot expiration, and orphaned file vacuuming to prevent metadata bloat.

## 🚀 Quickstart & Local Deployment

### Prerequisites
- Docker Desktop
- Python 3.11+
- Java 8+ (Required for PySpark)

### 1. Boot Infrastructure
Start the Kafka and Zookeeper containers:
```powershell
docker-compose up -d
docker-compose ps
```

### 2. Configure Python Environment
Create a virtual environment and install the required dependencies:
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install pyspark==3.5.1 confluent-kafka==2.3.0
$env:PYTHONPATH="."
```

### 3. Initialize the Lakehouse
Initialize the Iceberg namespace and local Hadoop catalog:
```powershell
python schema\create_trade_table.py
```

### 4. Start the Pipeline
Run the PySpark Structured Streaming consumer (Listens for Kafka events and writes to Iceberg):
```powershell
python pipeline\trade_consumer.py
```

### 5. Generate Trade Events
Open a **new** PowerShell terminal, activate the environment, and start generating mock trades:
```powershell
cd path\to\trade-activity-lakehouse
.\venv\Scripts\Activate.ps1
python producer\trade_data_generator.py
```

## 🛠️ Maintenance & Validation

Validate the ingested data and run Iceberg maintenance procedures (Compaction & Vacuuming):
```powershell
python schema\check_data.py
python pipeline\optimize_table.py
python schema\evolve_schema.py
```

## 📂 Repository Structure

```text
trade-activity-lakehouse/
├── docker-compose.yml           # Infrastructure 
├── config/                      # PySpark & Iceberg Config
├── producer/                    # Kafka Event Generator
├── pipeline/                    # Streaming Consumer & Optimizer
└── schema/                      # DDL & Schema Evolution Scripts
```
