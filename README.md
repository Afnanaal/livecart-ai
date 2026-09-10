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

Traditional chatbot systems may depend on stale or incomplete information, which can result in inaccurate answers to customer questions.

LiveCart AI addresses this challenge by combining real-time data ingestion, data quality validation, lakehouse processing, and grounded AI retrieval.

---

## Solution

LiveCart AI provides an end-to-end data and AI pipeline that:

1. Ingests real-time commerce events using Apache Kafka.
2. Validates incoming events using Pydantic schemas.
3. Routes malformed records to a Kafka Dead Letter Queue (DLQ).
4. Processes validated data through Bronze, Silver, and Gold Delta Lake layers.
5. Performs business-key MERGE / UPSERT operations.
6. Chunks product knowledge into retrieval-ready documents.
7. Generates vector embeddings.
8. Stores embeddings in Qdrant.
9. Performs hybrid dense + BM25 retrieval.
10. Combines retrieval results using Reciprocal Rank Fusion (RRF).
11. Applies Cross-Encoder reranking.
12. Produces grounded answers with source citations.
13. Uses Apache Airflow for pipeline orchestration.
14. Applies Great Expectations as a data quality gate.
15. Emits OpenLineage START, COMPLETE, and FAIL events for pipeline stages.

---

# Architecture

```mermaid
flowchart LR

    A[Commerce Events] --> B[Apache Kafka]

    B --> C[Pydantic Schema Validation]

    C -->|Valid Events| D[Bronze Delta]
    C -->|Invalid Events| DLQ[Kafka Dead Letter Queue]

    D --> E[Silver Delta]

    E --> F[Business Key MERGE / UPSERT]

    F --> G[Gold Delta Aggregates]

    E --> H[Document Chunking]
    H --> I[Embedding Model]
    I --> J[Qdrant Vector Store]

    Q[User Question] --> K[Dense Search]
    Q --> L[BM25 Keyword Search]

    J --> K

    K --> M[RRF Fusion]
    L --> M

    M --> N[Cross-Encoder Reranking]

    N --> O[Grounded Answer + Citations]

    B --> P[Apache Airflow]
    D --> P
    O --> P

    P --> R[Great Expectations Quality Gate]
    P --> S[OpenLineage]
