from rank_bm25 import BM25Okapi

from src.rag.chunking import DocumentChunk, chunk_document
from src.rag.embeddings import EmbeddingModel
from src.rag.vector_store import QdrantVectorStore


# RRF constant from the standard reciprocal-rank fusion formula.
RRF_K = 60


class BM25Search:
    """
    Keyword-based retrieval using BM25.
    """

    def __init__(self, chunks: list[DocumentChunk]):
        self.chunks = chunks

        self.tokenized_documents = [
            chunk.text.lower().split()
            for chunk in chunks
        ]

        self.bm25 = BM25Okapi(self.tokenized_documents)

    def search(
        self,
        query: str,
        limit: int = 5,
    ) -> list[dict]:
        """
        Search documents using BM25.
        """

        query_tokens = query.lower().split()

        scores = self.bm25.get_scores(query_tokens)

        ranked_indices = sorted(
            range(len(scores)),
            key=lambda index: scores[index],
            reverse=True,
        )[:limit]

        results = []

        for rank, index in enumerate(ranked_indices, start=1):
            chunk = self.chunks[index]

            results.append(
                {
                    "rank": rank,
                    "score": float(scores[index]),
                    "chunk_id": chunk.chunk_id,
                    "document_id": chunk.document_id,
                    "text": chunk.text,
                    "source": chunk.source,
                    "product_id": chunk.product_id,
                }
            )

        return results


def reciprocal_rank_fusion(
    dense_results: list[dict],
    bm25_results: list[dict],
    k: int = RRF_K,
) -> list[dict]:
    """
    Combine Dense and BM25 rankings using
    Reciprocal Rank Fusion (RRF).

    Formula:

        RRF score = Σ 1 / (k + rank)
    """

    fused: dict[str, dict] = {}

    # ---------------------------------------------------------
    # Dense results
    # ---------------------------------------------------------

    for fallback_rank, result in enumerate(
        dense_results,
        start=1,
    ):
        chunk_id = result["chunk_id"]

        # Some retrievers may not return rank.
        # We assign it based on the returned order.
        dense_rank = result.get(
            "rank",
            fallback_rank,
        )

        if chunk_id not in fused:
            fused[chunk_id] = {
                "chunk_id": chunk_id,
                "document_id": result["document_id"],
                "text": result["text"],
                "source": result["source"],
                "product_id": result["product_id"],
                "dense_rank": None,
                "bm25_rank": None,
                "rrf_score": 0.0,
            }

        fused[chunk_id]["dense_rank"] = dense_rank

        fused[chunk_id]["rrf_score"] += (
            1.0 / (k + dense_rank)
        )

    # ---------------------------------------------------------
    # BM25 results
    # ---------------------------------------------------------

    for fallback_rank, result in enumerate(
        bm25_results,
        start=1,
    ):
        chunk_id = result["chunk_id"]

        bm25_rank = result.get(
            "rank",
            fallback_rank,
        )

        if chunk_id not in fused:
            fused[chunk_id] = {
                "chunk_id": chunk_id,
                "document_id": result["document_id"],
                "text": result["text"],
                "source": result["source"],
                "product_id": result["product_id"],
                "dense_rank": None,
                "bm25_rank": None,
                "rrf_score": 0.0,
            }

        fused[chunk_id]["bm25_rank"] = bm25_rank

        fused[chunk_id]["rrf_score"] += (
            1.0 / (k + bm25_rank)
        )

    # ---------------------------------------------------------
    # Sort by final RRF score
    # ---------------------------------------------------------

    return sorted(
        fused.values(),
        key=lambda result: result["rrf_score"],
        reverse=True,
    )


def build_demo_documents() -> list[DocumentChunk]:
    """
    Create a small product dataset for the D3 demonstration.
    """

    documents = [
        (
            "DOC001",
            (
                "Lightweight laptop suitable for travel "
                "with long battery life."
            ),
            "product_catalog",
            "P001",
        ),
        (
            "DOC002",
            (
                "High performance gaming laptop "
                "with powerful graphics."
            ),
            "product_catalog",
            "P002",
        ),
        (
            "DOC003",
            (
                "Wireless mouse with ergonomic design "
                "and long battery life."
            ),
            "product_catalog",
            "P003",
        ),
    ]

    chunks: list[DocumentChunk] = []

    for (
        document_id,
        text,
        source,
        product_id,
    ) in documents:

        chunks.extend(
            chunk_document(
                document_id=document_id,
                text=text,
                source=source,
                product_id=product_id,
            )
        )

    return chunks


if __name__ == "__main__":

    # =========================================================
    # 1. Prepare documents
    # =========================================================

    chunks = build_demo_documents()

    print(
        f"📄 Prepared {len(chunks)} document chunk(s)"
    )

    # =========================================================
    # 2. Generate embeddings
    # =========================================================

    embedding_model = EmbeddingModel()

    embeddings = embedding_model.encode(
        [
            chunk.text
            for chunk in chunks
        ]
    )

    print(
        f"🧠 Generated {len(embeddings)} embeddings"
    )

    print(
        f"📐 Embedding dimension: "
        f"{len(embeddings[0])}"
    )

    # =========================================================
    # 3. Store vectors in Qdrant
    # =========================================================

    vector_store = QdrantVectorStore()

    vector_store.create_collection()

    vector_store.upsert_chunks(
        chunks=chunks,
        embeddings=embeddings,
    )

    # =========================================================
    # 4. Query
    # =========================================================

    query = (
        "I need a lightweight laptop for traveling"
    )

    print()
    print(
        f"🔍 Query: {query}"
    )

    # =========================================================
    # 5. Dense Vector Search
    # =========================================================

    query_embedding = embedding_model.encode(
        [query]
    )[0]

    dense_results = vector_store.dense_search(
        query_embedding=query_embedding,
        limit=3,
    )

    # Ensure Dense results always have ranks.
    for rank, result in enumerate(
        dense_results,
        start=1,
    ):
        result["rank"] = rank

    print()
    print(
        "🔵 DENSE SEARCH RESULTS"
    )
    print("=" * 70)

    for result in dense_results:
        print(
            f"{result['rank']}. "
            f"{result['product_id']} | "
            f"score={result['score']:.4f} | "
            f"{result['text']}"
        )

    # =========================================================
    # 6. BM25 Keyword Search
    # =========================================================

    bm25_search = BM25Search(
        chunks=chunks
    )

    bm25_results = bm25_search.search(
        query=query,
        limit=3,
    )

    print()
    print(
        "🟢 BM25 KEYWORD SEARCH RESULTS"
    )
    print("=" * 70)

    for result in bm25_results:
        print(
            f"{result['rank']}. "
            f"{result['product_id']} | "
            f"score={result['score']:.4f} | "
            f"{result['text']}"
        )

    # =========================================================
    # 7. Hybrid Search + RRF
    # =========================================================

    hybrid_results = reciprocal_rank_fusion(
        dense_results=dense_results,
        bm25_results=bm25_results,
        k=RRF_K,
    )

    print()
    print(
        "🟣 HYBRID SEARCH — DENSE + BM25 + RRF"
    )
    print("=" * 70)

    for rank, result in enumerate(
        hybrid_results,
        start=1,
    ):
        print(
            f"{rank}. "
            f"{result['product_id']} | "
            f"RRF={result['rrf_score']:.6f} | "
            f"DenseRank={result['dense_rank']} | "
            f"BM25Rank={result['bm25_rank']} | "
            f"{result['text']}"
        )

    # =========================================================
    # 8. Validation
    # =========================================================

    if not hybrid_results:
        raise RuntimeError(
            "Hybrid search returned no results."
        )

    if hybrid_results[0]["product_id"] != "P001":
        raise AssertionError(
            "Expected P001 to be the top hybrid result."
        )

    print()
    print(
        "🎯 DELIVERABLE 3 — HYBRID SEARCH: PASSED"
    )
    print(
        "Dense retrieval + BM25 + RRF fusion are working."
    )