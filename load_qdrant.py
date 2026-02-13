import json
import requests
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance

COLLECTION = "rpd_rag"
EMBED_MODEL = "bge-m3"
OLLAMA_URL = "http://localhost:11434/api/embeddings"
QDRANT_URL = "http://localhost:6333"
BATCH_EMBED = 8
UPSERT_BATCH = 128
CHUNKS_FILE = "chunks.jsonl"


with open(CHUNKS_FILE, encoding="utf-8") as f:
    chunks = [json.loads(line) for line in f]


def embed_text(text: str) -> List[float]:
    text = f"passage: {text}"
    response = requests.post(
        OLLAMA_URL,
        json={"model": EMBED_MODEL, "prompt": text},
        timeout=120
    )
    data = response.json()
    return data.get("embedding") or data["data"][0]["embedding"]


results = []

with ThreadPoolExecutor(max_workers=BATCH_EMBED) as executor:
    futures = {executor.submit(embed_text, ch["text"]): ch for ch in chunks}
    
    for future in as_completed(futures):
        chunk = futures[future]
        vector = future.result()
        results.append((vector, chunk))


client = QdrantClient(QDRANT_URL, check_compatibility=False)

vector_size = len(results[0][0])

if client.collection_exists(COLLECTION):
    client.delete_collection(COLLECTION)

client.create_collection(
    collection_name=COLLECTION,
    vectors_config=VectorParams(
        size=vector_size,
        distance=Distance.COSINE
    )
)


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


print(f"Загружено векторов: {len(results)}")
