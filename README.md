# Spotify Real-Time Data Engineering Pipeline

Data Flow Diagram

                   ┌──────────────────────────┐
                   │   Spotify Event Stream   │
                   │  (User Plays, Skips etc) │
                   └────────────┬─────────────┘
                                │
                                ▼
                     ┌───────────────────┐
                     │      Kafka        │
                     │ (Event Streaming) │
                     └─────────┬────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │        MinIO         │
                    │   (Object Storage)   │
                    │ ┌─────────┬────────┐ │
                    │ │ Bronze  │  Raw   │ │
                    │ └─────────┴────────┘ │
                    └────────────┬─────────┘
                                 │
                                 ▼
                     ┌──────────────────────┐
                     │      Snowflake       │
                     │  Medallion Layers    │
                     │ ┌─────────┬────────┐ │
                     │ │ Bronze  │  Raw   │ │
                     │ │ Silver  │ Clean  │ │
                     │ │ Gold    │ Aggr.  │ │
                     │ └─────────┴────────┘ │
                     └────────────┬─────────┘
                                  │
                                  ▼
                   ┌──────────────────────────┐
                   │           dbt            │
                   │ (SQL Models & Transform) │
                   └────────────┬─────────────┘
                                │
                                ▼
                   ┌──────────────────────────┐
                   │      Airflow DAGs        │
                   │ (End-to-End Orchestration)│
                   └────────────┬─────────────┘
                                │
                                ▼
                   ┌──────────────────────────┐
                   │          Docker          │
                   │ (Containerized Pipeline) │
                   └──────────────────────────┘




### Project Overview
This project is a fully automated, real-time data engineering pipeline for Spotify streaming events. It demonstrates end-to-end ingestion, storage, and transformation of streaming data, enabling analytics and business insights.
The pipeline:
Collects real-time Spotify user activity events.
Streams events through Kafka.
Stores raw events in MinIO (S3-compatible object store).
Loads data into Snowflake using a Medallion Architecture (Bronze → Silver → Gold).
Transforms raw events into analytics-ready datasets using dbt.
Orchestrates the entire workflow via Airflow.
Runs in a Dockerized environment for portability and scalability.
### Business Problem Solved
Spotify generates massive streams of data, such as:
Song plays, skips, and pauses.
Device usage and geographic location.
User engagement metrics.
Challenges solved:
Real-Time Data Capture – Collect user events as they happen.
Centralized Storage – Organize raw and transformed datasets efficiently.
Automated ETL/ELT – Eliminate manual data processing.
Actionable Insights – Enable dashboards, trend analysis, and business intelligence.
### Architecture & Workflow
1. Event Generation
Python-based simulator generates realistic Spotify events.
Each event includes:
user_id, song_id, artist_name, song_name, event_type, device_type, country, timestamp
Events are sent to a Kafka topic (spotify-events).
2. Kafka → MinIO
Kafka acts as the streaming broker.
MinIO stores raw JSON events in date/hour partitions.
Batches are written from Kafka to MinIO using a Python consumer.
3. MinIO → Snowflake (Medallion Architecture)
Bronze Layer: Raw JSON events loaded into Snowflake tables (Airflow DAG).
Silver Layer: dbt transforms raw data into clean, deduplicated tables.
Gold Layer: Aggregated datasets for analytics and reporting.
4. Orchestration
Apache Airflow manages ETL/ELT DAGs:
Extract → Load → Transform
Scheduled hourly for near real-time data processing.
### Tech Stack
- Layer	Technology
- Orchestration	Apache Airflow
- Streaming	Kafka
- Object Storage	MinIO (S3-compatible)
- ETL/ELT	Python, boto3, Snowflake Connector
- Data Warehouse	Snowflake
- Data Transformation	dbt
- Containerization	Docker
- Environment	Python virtualenv, dotenv

                                             
# Step-by-Step Implementation
- Dockerized the Environment
- Containers for Kafka, Zookeeper, MinIO, Postgres, and Airflow.
- Kafdrop for Kafka monitoring.
Event Generation
Python simulator produces random Spotify events.
Streams them to Kafka in real-time.
- Kafka → MinIO
Kafka consumer batches events.
Writes them to MinIO in JSON format, partitioned by date/hour.
- MinIO → Snowflake (Bronze)
Airflow DAG extracts events and loads raw data into Snowflake.
- Transformation (Silver & Gold)
dbt cleans, deduplicates, and enriches Bronze data into Silver tables.
Aggregates data for Gold layer (top songs, engagement metrics, trends).


### Business Impact
Fully automated real-time data pipeline.
Centralized, analytics-ready datasets for dashboards and ML.
Scalable and maintainable architecture using medallion design.
Supports user behavior analysis, top-song tracking, device usage trends, and geographical insights.
