from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from src.rag.chunking import DocumentChunk


QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "livecart_products"
VECTOR_SIZE = 384


class QdrantVectorStore:
    def __init__(
        self,
        url: str = QDRANT_URL,
        collection_name: str = COLLECTION_NAME,
    ):
        self.client = QdrantClient(url=url)
        self.collection_name = collection_name

    def create_collection(self) -> None:
        collections = self.client.get_collections().collections
        existing_names = {collection.name for collection in collections}

        if self.collection_name in existing_names:
            self.client.delete_collection(self.collection_name)

        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(
                size=VECTOR_SIZE,
                distance=Distance.COSINE,
            ),
        )

        print(f"✅ Qdrant collection created: {self.collection_name}")

    def upsert_chunks(
        self,
        chunks: list[DocumentChunk],
        embeddings: list[list[float]],
    ) -> None:
        if len(chunks) != len(embeddings):
            raise ValueError("Each chunk must have exactly one embedding")

        points = []

        for index, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            points.append(
                PointStruct(
                    id=index + 1,
                    vector=embedding,
                    payload={
                        "chunk_id": chunk.chunk_id,
                        "document_id": chunk.document_id,
                        "text": chunk.text,
                        "source": chunk.source,
                        "product_id": chunk.product_id,
                    },
                )
            )

        self.client.upsert(
            collection_name=self.collection_name,
            points=points,
        )

        print(f"✅ Upserted {len(points)} chunks into Qdrant")

    def dense_search(
        self,
        query_embedding: list[float],
        limit: int = 5,
    ) -> list[dict]:
        response = self.client.query_points(
            collection_name=self.collection_name,
            query=query_embedding,
            limit=limit,
            with_payload=True,
        )

        results = []

        for point in response.points:
            results.append(
                {
                    "id": point.id,
                    "score": point.score,
                    "chunk_id": point.payload["chunk_id"],
                    "document_id": point.payload["document_id"],
                    "text": point.payload["text"],
                    "source": point.payload["source"],
                    "product_id": point.payload["product_id"],
                }
            )

        return results


if __name__ == "__main__":
    from src.rag.embeddings import EmbeddingModel
    from src.rag.chunking import chunk_document

    documents = [
        (
            "DOC001",
            "Lightweight laptop suitable for travel with long battery life.",
            "product_catalog",
            "P001",
        ),
        (
            "DOC002",
            "High performance gaming laptop with powerful graphics.",
            "product_catalog",
            "P002",
        ),
        (
            "DOC003",
            "Wireless mouse with ergonomic design and long battery life.",
            "product_catalog",
            "P003",
        ),
    ]

    chunks = []

    for document_id, text, source, product_id in documents:
        chunks.extend(
            chunk_document(
                document_id=document_id,
                text=text,
                source=source,
                product_id=product_id,
            )
        )

    embedding_model = EmbeddingModel()
    embeddings = embedding_model.encode(
        [chunk.text for chunk in chunks]
    )

    store = QdrantVectorStore()
    store.create_collection()
    store.upsert_chunks(chunks, embeddings)

    query = "I need a lightweight laptop for traveling"
    query_embedding = embedding_model.encode([query])[0]

    results = store.dense_search(
        query_embedding=query_embedding,
        limit=3,
    )

    print("\n🔎 Dense Search Results")
    print("=" * 60)

    for rank, result in enumerate(results, start=1):
        print(
            f"{rank}. "
            f"{result['product_id']} | "
            f"score={result['score']:.4f} | "
            f"{result['text']}"
        )