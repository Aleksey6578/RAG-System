"""
load_qdrant_RouterAI.py — загрузка чанков РПД в Qdrant.

Версия RouterAI: эмбеддинги через RouterAI API (qwen3-embedding-4b)
вместо локального Ollama.

Изменения относительно load_qdrant.py:
  - Ollama /api/embed → OpenAI-совместимый клиент RouterAI.
  - BATCH_EMBED = 8 (параллельные запросы к внешнему API).

Синхронизировано с load_qdrant.py:
  - [B] Поля direction/level/department в payload + индексы.
  - [S] section_type на верхнем уровне payload.
  - [I] --append режим (пересборка vs. добавление точек).
  - [O] upsert_batch_with_retry: retry при HTTP 206 / ошибках.
  - [P] chunk_id как именованное поле.
  - [N] MAX_EMBED_CHARS = 4000 (обрезка длинных чанков).
  - EMBED_DIM validation перед созданием коллекции.
  - create_payload_indexes() для ускорения metadata filtering.
"""

import argparse
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI

# =====================
# CONFIG
# =====================
COLLECTION     = "rpd_rag"
QDRANT_URL     = "http://localhost:6333"
CHUNKS_FILE    = "chunks.jsonl"

ROUTERAI_API_KEY  = "sk-KnAptJMGtv69zxhmW2v8f7LILGs8umvT"
ROUTERAI_BASE_URL = "https://routerai.ru/api/v1"
EMBED_MODEL       = "qwen/qwen3-embedding-4b"

BATCH_EMBED    = 8      # параллельных embed-запросов к RouterAI API
UPSERT_BATCH   = 64
RETRY_COUNT    = 3
RETRY_DELAY    = 2.0
PROGRESS_EVERY = 50

EMBED_DIM = 2560  # qwen/qwen3-embedding-4b возвращает 2560-мерные векторы

# [N] Максимальная длина текста для embedding.
# qwen3-embedding-4b: безопасный порог аналогичен bge-m3 (~8192 токенов).
# 4000 символов ≈ 1000–1300 токенов русского текста.
MAX_EMBED_CHARS = 4000

# =====================
# RouterAI CLIENT
# =====================
client_ai = OpenAI(
    api_key=ROUTERAI_API_KEY,
    base_url=ROUTERAI_BASE_URL,
    timeout=120.0,
)

QDRANT_HEADERS = {"Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

def embed_text(text: str) -> list | None:
    """
    Получает embedding для одного текста через RouterAI API.
    [N] Тексты длиннее MAX_EMBED_CHARS обрезаются.

    [FIX-NONE] Отчёт §3.2: 3 всплеска ошибок NoneType (object is not
    subscriptable) на позициях ~2600, ~4300, ~5000–5400. Причина: RouterAI
    API изредка возвращает пустой response.data (пустой список) или None,
    тогда response.data[0].embedding падает с TypeError. Добавлены:
      • проверка на пустой/невалидный text (ранний выход — не тратим квоту)
      • явная проверка response, response.data, len > 0 и embedding is not None
      • исключение логируется с номером попытки, но не прерывает retry-цикл.
    """
    # [FIX-NONE] Валидация входа: не отправляем пустой текст в API
    if not text or not text.strip():
        print(f"  ⚠️  embed_text: пустой текст — пропуск")
        return None

    if len(text) > MAX_EMBED_CHARS:
        print(f"  ⚠️  Текст обрезан: {len(text)} → {MAX_EMBED_CHARS} символов")
        text = text[:MAX_EMBED_CHARS]

    for attempt in range(RETRY_COUNT):
        try:
            response = client_ai.embeddings.create(
                model=EMBED_MODEL,
                input=text,
                encoding_format="float",
            )
            # [FIX-NONE] Полная валидация ответа API
            if response is None:
                print(f"  ⚠️  embedding: response is None "
                      f"(попытка {attempt + 1}/{RETRY_COUNT})")
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            if not getattr(response, "data", None) or len(response.data) == 0:
                print(f"  ⚠️  embedding: response.data пусто "
                      f"(попытка {attempt + 1}/{RETRY_COUNT})")
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            vec = getattr(response.data[0], "embedding", None)
            if not vec:
                print(f"  ⚠️  embedding: vec пуст "
                      f"(попытка {attempt + 1}/{RETRY_COUNT})")
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return vec
        except Exception as e:
            print(f"  ⚠️  embedding ошибка: {e}, попытка {attempt + 1}/{RETRY_COUNT}")
            time.sleep(RETRY_DELAY * (attempt + 1))
    return None


# ---------------------------------------------------------------------------
# Payload индексы
# ---------------------------------------------------------------------------

def create_payload_indexes(collection: str) -> None:
    """
    [B][S] Создаём индексы для полей фильтрации — ускоряет metadata filtering.
    """
    fields = [
        ("section_type",          "keyword"),  # [S] верхний уровень
        ("metadata.section_type", "keyword"),  # обратная совместимость
        ("content_type",          "keyword"),  # [FIX-TEXTBOOK] textbook vs syllabus
        ("source",                "keyword"),
        ("doc_id",                "keyword"),
        ("section_title",         "keyword"),
        ("direction",             "keyword"),  # [B]
        ("level",                 "keyword"),  # [B]
        ("department",            "keyword"),  # [B]
    ]
    for field_name, schema_type in fields:
        try:
            r = requests.put(
                f"{QDRANT_URL}/collections/{collection}/index",
                json={"field_name": field_name, "field_schema": schema_type},
                headers=QDRANT_HEADERS,
                timeout=15,
            )
            if r.status_code in (200, 206):
                print(f"  Индекс создан: {field_name}")
            else:
                print(f"  Индекс {field_name}: {r.status_code}")
        except Exception as e:
            print(f"  Индекс {field_name} не создан: {e}")


# ---------------------------------------------------------------------------
# Upsert с retry
# ---------------------------------------------------------------------------

def upsert_batch(ids: list, vectors: list, payloads: list) -> tuple[bool, list]:
    """
    Загрузка батча. Возвращает (успех, список failed point ids).
    HTTP 206 Partial Content обрабатывается отдельно.
    """
    body = {"batch": {"ids": ids, "vectors": vectors, "payloads": payloads}}
    r = requests.put(
        f"{QDRANT_URL}/collections/{COLLECTION}/points",
        json=body,
        headers=QDRANT_HEADERS,
        timeout=60,
    )
    if r.status_code == 206:
        try:
            failed = r.json().get("result", {}).get("failed", [])
        except Exception:
            failed = []
        if failed:
            print(f"  ⚠️  HTTP 206: {len(failed)} точек не загружено")
        return True, [f["id"] for f in failed] if failed else []
    if r.status_code not in (200,):
        print(f"  Ошибка upsert: {r.status_code} {r.text[:300]}")
        return False, ids
    return True, []


def upsert_batch_with_retry(ids: list, vectors: list, payloads: list) -> bool:
    """
    [O] Retry для upsert_batch: при HTTP 206 итеративно перезагружает
    failed_ids до RETRY_COUNT раз с экспоненциальной задержкой.
    """
    delay = RETRY_DELAY
    for attempt in range(1, RETRY_COUNT + 1):
        ok, failed_ids = upsert_batch(ids, vectors, payloads)
        if ok and not failed_ids:
            return True
        if ok and failed_ids:
            print(f"  Retry {len(failed_ids)} failed points (попытка {attempt}/{RETRY_COUNT})...")
            failed_set  = set(failed_ids)
            retry_pairs = [(i, v, p) for i, v, p in zip(ids, vectors, payloads)
                           if i in failed_set]
            ids      = [x[0] for x in retry_pairs]
            vectors  = [x[1] for x in retry_pairs]
            payloads = [x[2] for x in retry_pairs]
            time.sleep(delay)
            delay *= 2
            continue
        if attempt < RETRY_COUNT:
            print(f"  Retry upsert {attempt}/{RETRY_COUNT}... ждём {delay:.0f}с")
            time.sleep(delay)
            delay *= 2
    print(f"  ❌ {len(ids)} точек не загружены после {RETRY_COUNT} попыток")
    return False


# ---------------------------------------------------------------------------
# Payload builder
# ---------------------------------------------------------------------------

def _build_payload(ch: dict) -> dict:
    """
    [S][B][P] Формирует payload Qdrant-точки.

    section_type выносится на верхний уровень: rpd_generate.py фильтрует
    по {"key": "section_type", "match": {"value": ...}}.
    Добавлены direction/level/department для доменной фильтрации.
    chunk_id дублируется как именованное поле [P].
    """
    meta = ch.get("metadata", {})
    section_type = (
        ch.get("section_metadata", {}).get("section_type")
        or meta.get("section_type")
        or ch.get("section_type", "other")
    )
    return {
        "chunk_id":      ch["id"],
        "id":            ch["id"],
        "doc_id":        ch.get("doc_id", ""),
        "source":        ch.get("source", ""),
        "source_file":   ch.get("source", ""),
        "section_title": ch.get("section_title", ""),
        "section_level": ch.get("section_level", ""),
        "doc_position":  ch.get("doc_position", 0),
        "text":          ch["text"],
        "section_type":  section_type,          # [S] верхний уровень
        "metadata":      meta,
        "direction":     ch.get("direction", ""),  # [B]
        "level":         ch.get("level", ""),       # [B]
        "department":    ch.get("department", ""),  # [B]
        "embedding_model": EMBED_MODEL,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(append_mode: bool = False):
    with open(CHUNKS_FILE, encoding="utf-8") as f:
        chunks = [json.loads(line) for line in f]

    print(f"Чанков к загрузке: {len(chunks)}")
    print(f"Модель: {EMBED_MODEL}, потоков: {BATCH_EMBED}")
    if append_mode:
        print("Режим: APPEND (коллекция не пересоздаётся)")
    else:
        print("Режим: RECREATE (коллекция будет пересоздана)")

    # Embedding батчами по 200 (предотвращает накопление очереди)
    EMBED_QUEUE_BATCH = 200
    results: list = []
    skipped = 0
    done    = 0

    for batch_start in range(0, len(chunks), EMBED_QUEUE_BATCH):
        batch_chunks = chunks[batch_start: batch_start + EMBED_QUEUE_BATCH]
        with ThreadPoolExecutor(max_workers=BATCH_EMBED) as executor:
            futures = {executor.submit(embed_text, ch["text"]): ch for ch in batch_chunks}
            for future in as_completed(futures):
                chunk  = futures[future]
                vector = future.result()
                done  += 1
                if vector is None:
                    skipped += 1
                else:
                    results.append((vector, chunk))
                if done % PROGRESS_EVERY == 0 or done == len(chunks):
                    print(f"  [{done}/{len(chunks)}] {done / len(chunks) * 100:.0f}%  "
                          f"пропущено: {skipped}")

    # Сортировка по id — предсказуемый порядок [Q]
    results.sort(key=lambda x: x[1]["id"])
    print(f"\nEmbedding готов. Успешно: {len(results)}, пропущено: {skipped}")

    if not results:
        print("Нет данных для загрузки.")
        return

    # Проверка Qdrant
    print("\nПроверка Qdrant...")
    try:
        requests.get(f"{QDRANT_URL}/collections", headers=QDRANT_HEADERS, timeout=10).raise_for_status()
        print("  Qdrant готов")
    except Exception as e:
        print(f"  Qdrant недоступен: {e}")
        return

    vector_size = len(results[0][0])
    if vector_size != EMBED_DIM:
        print(
            f"❌ Размерность вектора {vector_size} ≠ EMBED_DIM={EMBED_DIM}.\n"
            f"   Модель: {EMBED_MODEL}. Обновите EMBED_DIM в load_qdrant_RouterAI.py."
        )
        return

    # Создание / проверка коллекции
    collection_exists = (
        requests.get(f"{QDRANT_URL}/collections/{COLLECTION}",
                     headers=QDRANT_HEADERS, timeout=10).status_code == 200
    )

    if not append_mode:
        if collection_exists:
            requests.delete(f"{QDRANT_URL}/collections/{COLLECTION}",
                            headers=QDRANT_HEADERS, timeout=10)
            print(f"Старая коллекция '{COLLECTION}' удалена.")
        r = requests.put(
            f"{QDRANT_URL}/collections/{COLLECTION}",
            headers=QDRANT_HEADERS,
            json={"vectors": {"size": EMBED_DIM, "distance": "Cosine"}},
            timeout=10,
        )
        r.raise_for_status()
        print(f"Коллекция '{COLLECTION}' создана (вектор: {EMBED_DIM}d).")
        create_payload_indexes(COLLECTION)
    else:
        if not collection_exists:
            print(f"  ❌ Коллекция '{COLLECTION}' не найдена. "
                  f"Запустите без --append для первоначальной загрузки.")
            return
        print(f"  Коллекция '{COLLECTION}' существует — добавляем новые точки.")

    # Upsert батчами с retry
    uploaded = 0
    i = 0
    while i < len(results):
        batch    = results[i: i + UPSERT_BATCH]
        ids      = [ch["id"]    for _, ch in batch]
        vectors  = [vec         for vec, _ in batch]
        payloads = [_build_payload(ch) for _, ch in batch]

        if upsert_batch_with_retry(ids, vectors, payloads):
            uploaded += len(batch)
            print(f"  Загружено: {uploaded}/{len(results)}")

        i += UPSERT_BATCH

    print(f"\nГотово. Загружено: {uploaded}, пропущено: {skipped}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Загрузка чанков РПД в Qdrant (RouterAI)")
    parser.add_argument(
        "--append", action="store_true",
        help="[I] Добавить новые точки в существующую коллекцию без пересоздания"
    )
    args = parser.parse_args()
    main(append_mode=args.append)
