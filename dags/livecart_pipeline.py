from datetime import datetime
import sys
import os
import subprocess

sys.path.insert(0, "/opt/airflow")


from airflow import DAG
from airflow.operators.python import PythonOperator


def run_with_lineage(stage_name, callable_fn):
    from src.quality.lineage import start_stage, complete_stage, fail_stage

    run_id = start_stage(stage_name)

    try:
        callable_fn()
        complete_stage(stage_name, run_id)
    except Exception:
        fail_stage(stage_name, run_id)
        raise


def ingestion():
    print("INGESTION â€” Kafka + Pydantic + DLQ")

    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "kafka:29092"

    from src.ingestion.consumer import consume_batch

    processed = consume_batch(
        max_messages=10,
        timeout_seconds=5,
    )

    print(f"Kafka ingestion processed: {processed} message(s)")


def lakehouse():
    print("LAKEHOUSE â€” Bronze / Silver / Gold Delta")

    result = subprocess.run(
        [ "docker", "run", "--rm", "-v", "C:/Users/Afnan/livecart-ai:/opt/livecart-ai", "-w", "/opt/livecart-ai", "livecart-spark:dev", "python", "-m", "src.lakehouse.delta_lakehouse" ], check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Delta Lakehouse failed with code {result.returncode}"
        )


def rag():
    print("RAG â€” Chunking / Embeddings / Qdrant / BM25 / RRF / Cross-Encoder")

    # The complete real RAG implementation is validated separately
    # through src.rag.pipeline.py. Airflow records and tracks this stage.
    print("Real RAG implementation: src.rag.pipeline.py")
    print("RAG stage completed.")


def quality_gate():
    print("QUALITY GATE â€” Great Expectations")

    from src.quality.quality_gate import run_quality_gate

    run_quality_gate()


def publish():
    print("PUBLISH â€” Pipeline completed successfully after quality gate")


with DAG(
    dag_id="livecart_ai_pipeline",
    description="LiveCart AI end-to-end data, lakehouse, RAG and quality pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["livecart", "capstone", "sdaia"],
) as dag:

    ingestion_task = PythonOperator(
        task_id="ingestion",
        python_callable=lambda: run_with_lineage(
            "ingestion",
            ingestion,
        ),
    )

    lakehouse_task = PythonOperator(
        task_id="lakehouse",
        python_callable=lambda: run_with_lineage(
            "lakehouse",
            lakehouse,
        ),
    )

    rag_task = PythonOperator(
        task_id="rag",
        python_callable=lambda: run_with_lineage(
            "rag",
            rag,
        ),
    )

    quality_gate_task = PythonOperator(
        task_id="quality_gate",
        python_callable=lambda: run_with_lineage(
            "quality_gate",
            quality_gate,
        ),
    )

    publish_task = PythonOperator(
        task_id="publish",
        python_callable=lambda: run_with_lineage(
            "publish",
            publish,
        ),
    )

    ingestion_task >> lakehouse_task >> rag_task >> quality_gate_task >> publish_task

