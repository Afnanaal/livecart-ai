from dataclasses import dataclass


CHUNK_SIZE = 512
CHUNK_OVERLAP = 50


@dataclass(frozen=True)
class DocumentChunk:
    chunk_id: str
    document_id: str
    text: str
    source: str
    product_id: str


def chunk_document(
    document_id: str,
    text: str,
    source: str,
    product_id: str,
    chunk_size: int = CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
) -> list[DocumentChunk]:
    if not text.strip():
        return []

    if overlap >= chunk_size:
        raise ValueError("overlap must be smaller than chunk_size")

    words = text.split()
    chunks: list[DocumentChunk] = []

    step = chunk_size - overlap

    for index, start in enumerate(range(0, len(words), step)):
        chunk_words = words[start : start + chunk_size]

        if not chunk_words:
            break

        chunks.append(
            DocumentChunk(
                chunk_id=f"{document_id}-chunk-{index}",
                document_id=document_id,
                text=" ".join(chunk_words),
                source=source,
                product_id=product_id,
            )
        )

        if start + chunk_size >= len(words):
            break

    return chunks


if __name__ == "__main__":
    sample_text = """
    LiveCart AI product catalog contains product descriptions, specifications,
    pricing, inventory information, customer reviews, and support content.
    Customers can ask questions using natural language and receive grounded
    answers based on the retrieved product context.
    """

    chunks = chunk_document(
        document_id="DOC001",
        text=sample_text,
        source="product_catalog",
        product_id="P001",
    )

    print(f"Created {len(chunks)} chunk(s)")
    for chunk in chunks:
        print(f"{chunk.chunk_id}: {chunk.text}")