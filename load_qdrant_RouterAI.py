"""
load_qdrant_RouterAI.py — загрузка чанков РПД в Qdrant.

Версия RouterAI: эмбеддинги через BAAI/bge-m3 via RouterAI API
вместо локального Ollama.

Изменения относительно оригинала:
  - Ollama /api/embed → OpenAI-совместимый клиент RouterAI.
  - Добавлен _build_payload(): section_type выносится на верхний
    уровень payload, чтобы фильтр Qdrant в rpd_generate.py
    {"key": "section_type", "match": ...} находил его корректно.
  - BATCH_EMBED снижен до 4 (bge-m3 тяжелее text-embedding-3-large).
"""

import json
import math
import time
import requests
from tqdm import tqdm
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI

# =====================
# CONFIG
# =====================
COLLECTION = "rpd_rag"
QDRANT_URL = "http://localhost:6333"

ROUTERAI_API_KEY  = "sk-KnAptJMGtv69zxhmW2v8f7LILGs8umvT"
ROUTERAI_BASE_URL = "https://routerai.ru/api/v1"
# Уточни точное название модели в личном кабинете routerai.ru.
# Возможные варианты: "BAAI/bge-m3" / "bge-m3"
EMBED_MODEL = "qwen/qwen3-embedding-4b"

BATCH_EMBED  = 8    # параллельных embed-запросов (bge-m3 тяжелее 3-large)
UPSERT_BATCH = 100
RETRIES      = 5
RETRY_SLEEP  = 2

# =====================
# RouterAI CLIENT
# =====================
client_ai = OpenAI(
    api_key=ROUTERAI_API_KEY,
    base_url=ROUTERAI_BASE_URL,
    timeout=120.0,
)

# =====================
# LOAD CHUNKS
# =====================
chunks: List[dict] = []
with open("chunks.jsonl", encoding="utf-8") as f:
    for line in f:
        chunks.append(json.loads(line))
print(f"📦 Чанков: {len(chunks)}")

# =====================
# EMBEDDING
# =====================
def embed_text(text: str) -> List[float]:
    for attempt in range(RETRIES):
        try:
            response = client_ai.embeddings.create(
                model=EMBED_MODEL,
                input=text,
                encoding_format="float",
            )
            return response.data[0].embedding
        except Exception as e:
            print(f"⚠️ embedding ошибка: {e}, попытка {attempt+1}/{RETRIES}")
            time.sleep(RETRY_SLEEP * (attempt + 1))
    return []

# =====================
# PARALLEL EMBEDDING
# =====================
results: List[Tuple[List[float], dict]] = []

with ThreadPoolExecutor(max_workers=BATCH_EMBED) as executor:
    futures = {executor.submit(embed_text, ch["text"]): ch for ch in chunks}
    for future in tqdm(as_completed(futures), total=len(futures), desc="Embedding"):
        ch  = futures[future]
        emb = future.result()
        if emb:
            results.append((emb, ch))

print(f"✅ Получено embeddings: {len(results)}")

# =====================
# QDRANT (raw REST)
# =====================
QDRANT_HEADERS = {"Content-Type": "application/json"}

def wait_qdrant():
    for _ in range(20):
        try:
            r = requests.get(f"{QDRANT_URL}/collections", timeout=5)
            if r.status_code == 200:
                print("✅ Qdrant готов")
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("❌ Qdrant не доступен")

wait_qdrant()

cols   = requests.get(f"{QDRANT_URL}/collections", headers=QDRANT_HEADERS, timeout=10).json()
exists = any(c["name"] == COLLECTION for c in cols["result"]["collections"])

if exists:
    print("🧹 Удаляю старую коллекцию...")
    requests.delete(
        f"{QDRANT_URL}/collections/{COLLECTION}",
        headers=QDRANT_HEADERS, timeout=10,
    )

# bge-m3 → 1024 измерения
emb_size = len(results[0][0]) if results else 1024
print(f"📐 Размерность вектора: {emb_size}")

print("📦 Создаю коллекцию...")
requests.put(
    f"{QDRANT_URL}/collections/{COLLECTION}",
    headers=QDRANT_HEADERS,
    json={"vectors": {"size": emb_size, "distance": "Cosine"}},
    timeout=10,
)

# =====================
# PAYLOAD
# =====================
def _build_payload(ch: dict) -> dict:
    """
    Формирует payload Qdrant-точки.

    section_type выносится на верхний уровень: rpd_generate.py
    фильтрует по {"key": "section_type", "match": {"value": ...}},
    поэтому поле должно быть доступно напрямую как payload.section_type,
    а не только внутри вложенного section_metadata / metadata.
    """
    section_type = (
        ch.get("section_metadata", {}).get("section_type")
        or ch.get("metadata", {}).get("section_type")
        or ch.get("section_type", "other")
    )
    return {
        **ch,
        "section_type": section_type,  # верхний уровень для Qdrant-фильтра
    }

# =====================
# UPSERT
# =====================
points: List[dict] = []
with tqdm(total=len(results), desc="Загрузка в Qdrant") as pbar:
    for i, (emb, ch) in enumerate(results, 1):
        points.append({
            "id":      ch["id"],
            "vector":  emb,
            "payload": _build_payload(ch),
        })
        if len(points) == UPSERT_BATCH or i == len(results):
            requests.put(
                f"{QDRANT_URL}/collections/{COLLECTION}/points",
                headers=QDRANT_HEADERS,
                json={"points": points},
                timeout=60,
            )
            pbar.update(len(points))
            points = []

print("✅ Qdrant полностью заполнен")
