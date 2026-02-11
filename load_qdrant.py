import json
import time
import requests
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from qdrant_client import QdrantClient
from qdrant_client.models import (
    PointStruct,
    VectorParams,
    Distance
)

# =====================
# CONFIG
# =====================
COLLECTION = "rpd_rag"
EMBED_MODEL = "bge-m3"   # рекомендуется multilingual модель
OLLAMA_URL = "http://localhost:11434/api/embeddings"
QDRANT_URL = "http://localhost:6333"

BATCH_EMBED = 8
UPSERT_BATCH = 128
RETRIES = 5
RETRY_SLEEP = 1.5

CHUNKS_FILE = "chunks.jsonl"


# =====================
# LOAD CHUNKS
# =====================
chunks = []
with open(CHUNKS_FILE, encoding="utf-8") as f:
    for line in f:
        chunks.append(json.loads(line))

print(f"Чанков загружено: {len(chunks)}")


# =====================
# EMBEDDING
# =====================
def embed_text(text: str) -> List[float]:
    # режим passage для retrieval-моделей
    text = f"passage: {text}"

    for _ in range(RETRIES):
        try:
            response = requests.post(
                OLLAMA_URL,
                json={
                    "model": EMBED_MODEL,
                    "prompt": text
                },
                timeout=120
            )
            response.raise_for_status()
            data = response.json()

            # совместимость с разными форматами ответа
            return data.get("embedding") or data["data"][0]["embedding"]

        except Exception:
            time.sleep(RETRY_SLEEP)

    return []


# =====================
# PARALLEL EMBEDDING
# =====================
results: List[Tuple[List[float], dict]] = []

with ThreadPoolExecutor(max_workers=BATCH_EMBED) as executor:
    futures = {executor.submit(embed_text, ch["text"]): ch for ch in chunks}

    for future in as_completed(futures):
        chunk = futures[future]
        vector = future.result()

        if vector:
            results.append((vector, chunk))

print(f"Embeddings успешно получены: {len(results)}")


# =====================
# QDRANT INIT
# =====================
client = QdrantClient(QDRANT_URL, check_compatibility=False)

if not results:
    raise RuntimeError("Нет полученных embeddings.")

vector_size = len(results[0][0])

client.recreate_collection(
    collection_name=COLLECTION,
    vectors_config=VectorParams(
        size=vector_size,
        distance=Distance.COSINE
    )
)

print(f"Коллекция {COLLECTION} создана. Размер вектора: {vector_size}")


# =====================
# UPSERT
# =====================
points = []

for i, (vector, chunk) in enumerate(results, 1):

    points.append(
        PointStruct(
            id=chunk["id"],
            vector=vector,
            payload=chunk
        )
    )

    if len(points) == UPSERT_BATCH or i == len(results):
        client.upsert(collection_name=COLLECTION, points=points)
        points = []

print("Qdrant успешно заполнен.")
