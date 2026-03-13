"""
load_qdrant.py — загрузка чанков в Qdrant через чистый HTTP.
Совместимо с Qdrant 1.16+ (batch-формат).

Баги: критических нет. thread-safety list.append защищён GIL в CPython.
"""

import json
import time
import requests
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

COLLECTION     = "rpd_rag"
EMBED_MODEL    = "bge-m3"
OLLAMA_URL     = "http://localhost:11434/api/embeddings"
QDRANT_URL     = "http://localhost:6333"
BATCH_EMBED    = 8
UPSERT_BATCH   = 64
CHUNKS_FILE    = "chunks.jsonl"
RETRY_COUNT    = 3
RETRY_DELAY    = 2.0
PROGRESS_EVERY = 50


def embed_text(text: str) -> Optional[List[float]]:
    delay = RETRY_DELAY
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            r = requests.post(
                OLLAMA_URL,
                json={"model": EMBED_MODEL, "prompt": f"passage: {text}"},
                timeout=120,
            )
            r.raise_for_status()
            d = r.json()
            return d.get("embedding") or d["data"][0]["embedding"]
        except Exception as e:
            if attempt < RETRY_COUNT:
                print(f"  Попытка {attempt}/{RETRY_COUNT}: {e}. Повтор через {delay:.0f}с...")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"  Embedding не получен: {e}")
                return None


def upsert_batch(ids: list, vectors: list, payloads: list) -> bool:
    """Загрузка батча в формате batch (ids/vectors/payloads)."""
    body = {
        "batch": {
            "ids":      ids,
            "vectors":  vectors,
            "payloads": payloads,
        }
    }
    r = requests.put(
        f"{QDRANT_URL}/collections/{COLLECTION}/points",
        json=body,
        timeout=60,
    )
    if r.status_code not in (200, 206):
        print(f"  Ошибка upsert: {r.status_code} {r.text[:300]}")
        return False
    return True


def main():
    with open(CHUNKS_FILE, encoding="utf-8") as f:
        chunks = [json.loads(line) for line in f]

    print(f"Чанков к загрузке: {len(chunks)}")
    print(f"Модель: {EMBED_MODEL}, потоков: {BATCH_EMBED}")

    # Embedding
    results = []
    skipped = 0

    with ThreadPoolExecutor(max_workers=BATCH_EMBED) as executor:
        futures = {executor.submit(embed_text, ch["text"]): ch for ch in chunks}
        done = 0
        for future in as_completed(futures):
            chunk  = futures[future]
            vector = future.result()
            done  += 1
            if vector is None:
                skipped += 1
            else:
                results.append((vector, chunk))
            if done % PROGRESS_EVERY == 0 or done == len(chunks):
                print(f"  [{done}/{len(chunks)}] {done/len(chunks)*100:.0f}%  пропущено: {skipped}")

    print(f"\nEmbedding готов. Успешно: {len(results)}, пропущено: {skipped}")

    if not results:
        print("Нет данных для загрузки.")
        return

    # Проверка Qdrant
    print("\nПроверка Qdrant...")
    try:
        requests.get(f"{QDRANT_URL}/collections", timeout=10).raise_for_status()
        print("  Qdrant готов")
    except Exception as e:
        print(f"  Qdrant недоступен: {e}")
        return

    vector_size = len(results[0][0])

    # Пересоздание коллекции
    r = requests.get(f"{QDRANT_URL}/collections/{COLLECTION}", timeout=10)
    if r.status_code == 200:
        requests.delete(f"{QDRANT_URL}/collections/{COLLECTION}", timeout=10)
        print(f"Старая коллекция '{COLLECTION}' удалена.")

    r = requests.put(
        f"{QDRANT_URL}/collections/{COLLECTION}",
        json={"vectors": {"size": vector_size, "distance": "Cosine"}},
        timeout=10,
    )
    r.raise_for_status()
    print(f"Коллекция '{COLLECTION}' создана (вектор: {vector_size}d).")

    # Загрузка батчами
    uploaded = 0
    i = 0

    while i < len(results):
        batch = results[i : i + UPSERT_BATCH]

        ids      = [ch["id"]     for _, ch in batch]
        vectors  = [vec          for vec, _ in batch]
        payloads = [{
            "id":            ch["id"],
            "doc_id":        ch.get("doc_id", ""),
            "source":        ch.get("source", ""),
            "section_title": ch.get("section_title", ""),
            "section_level": ch.get("section_level", ""),
            "text":          ch["text"],
            "metadata":      ch.get("metadata", {}),
        } for _, ch in batch]

        if upsert_batch(ids, vectors, payloads):
            uploaded += len(batch)
            print(f"  Загружено: {uploaded}/{len(results)}")

        i += UPSERT_BATCH

    print(f"\nГотово. Загружено: {uploaded}, пропущено: {skipped}")


if __name__ == "__main__":
    main()
