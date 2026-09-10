# LiveCart AI

## Real-Time Product Intelligence & Q&A

LiveCart AI is a real-time e-commerce intelligence platform that combines modern data engineering, real-time streaming, Delta Lakehouse architecture, and Retrieval-Augmented Generation (RAG).

The platform is designed to keep product knowledge continuously updated while providing grounded answers based on retrieved product information.

---

## Problem

E-commerce product information is distributed across multiple sources, including:

- Product catalogs
- Customer reviews
- Support content
- Pricing
- Inventory information

Traditional chatbots may depend on stale or incomplete information, which can result in inaccurate answers to customer questions.

LiveCart AI addresses this challenge by combining real-time data ingestion, schema validation, lakehouse processing, hybrid retrieval, and data quality controls.

---

## Solution

LiveCart AI provides an end-to-end data and AI pipeline that:

1. Ingests real-time commerce events using Apache Kafka.
2. Validates incoming events using Pydantic schemas.
3. Routes malformed records to a Kafka Dead Letter Queue (DLQ).
4. Processes validated data through Bronze, Silver, and Gold Delta Lake layers.
5. Performs business-key MERGE / UPSERT operations.
6. Chunks product knowledge into retrieval-ready documents.
7. Generates vector embeddings using Sentence Transformers.
8. Stores vectors in Qdrant.
9. Performs hybrid Dense + BM25 retrieval.
10. Combines retrieval results using Reciprocal Rank Fusion (RRF).
11. Applies Cross-Encoder reranking.
12. Produces grounded answers with source citations.
13. Uses Apache Airflow for pipeline orchestration.
14. Applies Great Expectations as a data quality gate.
15. Emits OpenLineage START, COMPLETE, and FAIL events.

---

## Architecture

```text
Commerce Events
       |
       v
Apache Kafka
       |
       v
Pydantic Schema Validation
       |
   +---+---+
   |       |
 Valid   Invalid
   |       |
   v       v
Bronze   Kafka DLQ
   |
   v
Silver Delta
   |
   +------> Business MERGE / UPSERT
   |                 |
   |                 v
   |             Gold Delta
   |
   v
RAG Pipeline
   |
   +--> Document Chunking
   |
   +--> Embeddings
   |
   +--> Qdrant Vector Store
   |
   +--> Dense Search
   |
   +--> BM25 Search
             |
             v
         RRF Fusion
             |
             v
   Cross-Encoder Reranking
             |
             v
   Grounded Answer + Citations

Apache Airflow orchestrates the pipeline.
Great Expectations provides the quality gate.
OpenLineage tracks START / COMPLETE / FAIL events.
```

---

## Pipeline Flow

```text
Ingestion
    |
    v
Delta Lakehouse
    |
    v
RAG
    |
    v
Quality Gate
    |
    v
Publish
```

The pipeline is designed so that failures are propagated through Airflow and downstream processing is not executed when an upstream stage fails.

---

## Real-Time Ingestion

Apache Kafka is used for real-time product event streaming.

Incoming product events are validated at the ingestion boundary using Pydantic.

Malformed records are rejected and routed to:

`product-events-dlq`

Each rejected record preserves useful debugging metadata, including:

- Original payload
- Rejection reason
- Source topic
- Partition
- Offset

### Example Product Event

```json
{
  "event_id": "demo-001",
  "event_type": "product_updated",
  "product_id": "P001",
  "product_name": "Travel Laptop",
  "price": 2999.0,
  "quantity": 5,
  "region": "SA",
  "event_time": "2026-09-10T00:00:00Z"
}
```

---

## Delta Lakehouse

The data processing layer implements a Bronze / Silver / Gold Delta Lakehouse architecture using Apache Spark and Delta Lake.

### Bronze

Stores validated incoming product events.

### Silver

Contains cleaned and transformed product records.

### Gold

Contains business-level aggregated data for downstream analytics.

The implementation includes real business-key MERGE / UPSERT behavior for both updates and inserts.

Schema enforcement is also applied to prevent invalid data structures from entering the lakehouse.

---

## RAG Pipeline

LiveCart AI implements a Retrieval-Augmented Generation pipeline designed to provide grounded answers from product knowledge.

The pipeline includes:

- Document chunking
- Sentence Transformer embeddings
- Qdrant vector storage
- Dense vector retrieval
- BM25 keyword retrieval
- Reciprocal Rank Fusion (RRF)
- Cross-Encoder reranking
- Grounded answers
- Source citations

The hybrid retrieval strategy combines semantic similarity with keyword-based retrieval, supporting both natural-language questions and exact product identifiers.

---

## Hybrid Search

The retrieval layer combines complementary search strategies.

### Dense Retrieval

Uses vector embeddings to identify semantically relevant product information.

### BM25

Provides keyword-based retrieval for exact terms, product names, and identifiers.

### Reciprocal Rank Fusion

RRF combines dense and keyword rankings into a unified candidate ranking.

### Cross-Encoder Reranking

A Cross-Encoder evaluates retrieved candidates and produces a refined ranking before the final grounded answer is produced.

---

## Data Quality

Great Expectations is used as the pipeline quality gate.

Implemented validation checks include:

- `product_id` is not null
- `product_name` is not null
- `price` is within the accepted range
- `quantity` is within the accepted range

A failed validation raises an error and prevents downstream processing.

---

## Data Lineage

OpenLineage is used to track pipeline execution.

Pipeline stages emit:

- START
- COMPLETE
- FAIL

Lineage evidence is stored under:

`outputs/lineage/`

This provides visibility into successful stages and pipeline failure points.

---

## Orchestration

Apache Airflow is used to coordinate the major pipeline stages.

The project DAG is:

`livecart_ai_pipeline`

The intended dependency chain is:

```text
Ingestion
    |
    v
Lakehouse
    |
    v
RAG
    |
    v
Quality Gate
    |
    v
Publish
```

The orchestration layer is designed to enforce stage dependencies and prevent downstream processing after an upstream failure.

---

## Technology Stack

| Layer | Technology |
|---|---|
| Streaming | Apache Kafka |
| Schema Validation | Pydantic |
| Processing | Apache Spark |
| Lakehouse | Delta Lake |
| Vector Database | Qdrant |
| Embeddings | Sentence Transformers |
| Keyword Search | BM25 |
| Retrieval Fusion | Reciprocal Rank Fusion |
| Reranking | Cross-Encoder |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations |
| Data Lineage | OpenLineage |
| Containerization | Docker |
| Programming Language | Python |

---

## Repository Structure

```text
livecart-ai/
|
+-- dags/
|   +-- livecart_pipeline.py
|
+-- src/
|   +-- ingestion/
|   |   +-- producer.py
|   |   +-- consumer.py
|   |   +-- schema.py
|   |
|   +-- lakehouse/
|   |   +-- delta_lakehouse.py
|   |
|   +-- rag/
|   |   +-- chunking.py
|   |   +-- embeddings.py
|   |   +-- vector_store.py
|   |   +-- hybrid_search.py
|   |   +-- reranker.py
|   |   +-- pipeline.py
|   |   +-- server.py
|   |
|   +-- quality/
|   |   +-- quality_gate.py
|   |
|   +-- orchestration/
|
+-- outputs/
|   +-- lineage/
|
+-- Dockerfile.airflow
+-- docker-compose.yml
+-- requirements-airflow.txt
+-- requirements-rag.txt
+-- README.md
```

---

## Getting Started

### Prerequisites

The project requires:

- Docker Desktop
- Docker Compose
- Git
- Python 3.11+

Verify the installation:

```bash
docker --version
docker compose version
python --version
```

---

## Start the Platform

From the project root:

```bash
docker compose up -d
```

Check running services:

```bash
docker compose ps
```

The platform uses Dockerized services including:

- Apache Kafka
- Qdrant
- PostgreSQL
- Apache Airflow Scheduler
- Apache Airflow Webserver

---

## Kafka Topics

Create the main product event topic:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic product-events --partitions 1 --replication-factor 1
```

Create the Dead Letter Queue:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic product-events-dlq --partitions 1 --replication-factor 1
```

---

## Run the Airflow DAG

List available DAGs:

```bash
docker compose exec airflow-scheduler airflow dags list
```

The project DAG is:

`livecart_ai_pipeline`

Run a DAG test:

```bash
docker compose exec airflow-scheduler airflow dags test livecart_ai_pipeline 2026-09-10
```

---

## Validation Evidence

The project includes implementation evidence for the main capstone requirements.

### Ingestion

- Real Kafka producer and consumer
- Pydantic schema validation
- Invalid-event rejection
- Kafka Dead Letter Queue
- OpenLineage lifecycle events

### Delta Lakehouse

- Bronze Delta layer
- Silver Delta layer
- Gold aggregation
- Business-key MERGE / UPSERT
- Schema enforcement

### RAG

- Document chunking
- Real embeddings
- Qdrant vector search
- BM25 retrieval
- RRF fusion
- Cross-Encoder reranking
- Grounded answers
- Source citations

### Quality and Lineage

- Great Expectations validation
- Quality gate behavior
- OpenLineage START / COMPLETE / FAIL events

---

## Project Status

**Capstone Project — Demonstration Ready**

The core LiveCart AI components have been implemented across real-time ingestion, schema validation, Delta Lakehouse processing, hybrid RAG retrieval, data quality, orchestration, and lineage.

The repository provides a Docker-based environment for demonstrating the architecture and individual pipeline components.

---

## Training Program

This project was developed as part of the:

**Modern Data Engineering for AI Systems**

**SDAIA Academy**

---

## Learning Outcomes

This project demonstrates practical implementation of:

- Real-time data engineering
- Event-driven architecture
- Schema validation
- Dead Letter Queue patterns
- Delta Lakehouse architecture
- Incremental MERGE / UPSERT processing
- Data quality engineering
- Vector databases
- Hybrid information retrieval
- Retrieval-Augmented Generation
- Cross-Encoder reranking
- Workflow orchestration
- Data lineage
- Containerized data platforms

---

## Future Improvements

Potential production extensions include:

- Debezium-based CDC
- Automatic vector-index updates from CDC events
- Feature-store integration
- Production LLM tool calling
- Authentication and authorization
- PII governance
- Monitoring and observability
- Cloud deployment
- CI/CD automation

---

## Author

**Afnan Alharbi**

Developed as a capstone project for the **Modern Data Engineering for AI Systems** training program at **SDAIA Academy**.
