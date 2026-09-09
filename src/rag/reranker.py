from sentence_transformers import CrossEncoder


MODEL_NAME = "cross-encoder/ms-marco-MiniLM-L-6-v2"


class CrossEncoderReranker:
    """
    Reranks retrieved candidates using a Cross-Encoder.

    Dense/BM25 retrieval is optimized for recall.
    Cross-Encoder reranking is used to improve precision
    on the final candidate set.
    """

    def __init__(
        self,
        model_name: str = MODEL_NAME,
    ):
        print(
            f"🤖 Loading Cross-Encoder: {model_name}"
        )

        self.model = CrossEncoder(model_name)

        print(
            "✅ Cross-Encoder loaded successfully"
        )

    def rerank(
        self,
        query: str,
        candidates: list[dict],
        top_k: int = 3,
    ) -> list[dict]:
        """
        Rerank candidate documents against the query.
        """

        if not candidates:
            return []

        pairs = [
            (query, candidate["text"])
            for candidate in candidates
        ]

        scores = self.model.predict(
            pairs,
            show_progress_bar=False,
        )

        reranked = []

        for candidate, score in zip(
            candidates,
            scores,
        ):
            result = dict(candidate)

            result["rerank_score"] = float(score)

            reranked.append(result)

        reranked.sort(
            key=lambda result: result["rerank_score"],
            reverse=True,
        )

        return reranked[:top_k]


if __name__ == "__main__":

    # =========================================================
    # Demo candidates from the hybrid retrieval stage
    # =========================================================

    query = (
        "I need a lightweight laptop for traveling"
    )

    candidates = [
        {
            "chunk_id": "DOC001-chunk-0",
            "document_id": "DOC001",
            "product_id": "P001",
            "source": "product_catalog",
            "text": (
                "Lightweight laptop suitable for travel "
                "with long battery life."
            ),
            "rrf_score": 0.032787,
            "dense_rank": 1,
            "bm25_rank": 1,
        },
        {
            "chunk_id": "DOC002-chunk-0",
            "document_id": "DOC002",
            "product_id": "P002",
            "source": "product_catalog",
            "text": (
                "High performance gaming laptop "
                "with powerful graphics."
            ),
            "rrf_score": 0.032258,
            "dense_rank": 2,
            "bm25_rank": 2,
        },
        {
            "chunk_id": "DOC003-chunk-0",
            "document_id": "DOC003",
            "product_id": "P003",
            "source": "product_catalog",
            "text": (
                "Wireless mouse with ergonomic design "
                "and long battery life."
            ),
            "rrf_score": 0.031746,
            "dense_rank": 3,
            "bm25_rank": 3,
        },
    ]

    print()
    print("🔎 CROSS-ENCODER RERANKING")
    print("=" * 70)

    reranker = CrossEncoderReranker()

    reranked_results = reranker.rerank(
        query=query,
        candidates=candidates,
        top_k=3,
    )

    print()
    print("🏆 RERANKED RESULTS")
    print("=" * 70)

    for rank, result in enumerate(
        reranked_results,
        start=1,
    ):
        print(
            f"{rank}. "
            f"{result['product_id']} | "
            f"RerankScore="
            f"{result['rerank_score']:.4f} | "
            f"RRF="
            f"{result['rrf_score']:.6f} | "
            f"{result['text']}"
        )

    # =========================================================
    # Validation
    # =========================================================

    if not reranked_results:
        raise RuntimeError(
            "Cross-Encoder returned no results."
        )

    if reranked_results[0]["product_id"] != "P001":
        raise AssertionError(
            "Expected P001 to remain the top reranked result."
        )

    print()
    print(
        "🎯 CROSS-ENCODER RERANKING: PASSED"
    )

    print(
        "Hybrid candidates were reranked "
        "using a real Cross-Encoder model."
    )