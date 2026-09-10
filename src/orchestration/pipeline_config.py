from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

PYTHON_ENV = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"


STAGES = {
    "ingestion": {
        "description": "Kafka ingestion + Pydantic validation",
        "module": "src.ingestion.consumer",
    },
    "lakehouse": {
        "description": "Delta Bronze → Silver → Gold + MERGE",
        "module": "src.lakehouse.delta_lakehouse",
    },
    "rag": {
        "description": "Chunking → Embeddings → Qdrant → BM25 → RRF → Cross-Encoder",
        "module": "src.rag.pipeline",
    },
}