from sentence_transformers import SentenceTransformer

MODEL_NAME = "all-MiniLM-L6-v2"


class EmbeddingModel:
    def __init__(self, model_name: str = MODEL_NAME):
        self.model = SentenceTransformer(model_name)

    def encode(self, texts: list[str]) -> list[list[float]]:
        embeddings = self.model.encode(
            texts,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        return embeddings.tolist()


if __name__ == "__main__":
    model = EmbeddingModel()

    texts = [
        "Lightweight laptop suitable for travel",
        "High performance gaming laptop",
    ]

    vectors = model.encode(texts)

    print(f"Model: {MODEL_NAME}")
    print(f"Documents: {len(vectors)}")
    print(f"Embedding dimension: {len(vectors[0])}")