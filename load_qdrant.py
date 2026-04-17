"""
load_qdrant.py — загрузка чанков в Qdrant через чистый HTTP.
Совместимо с Qdrant 1.16+ (batch-формат).

Исправления v3:
  - [КРИТИЧЕСКИЙ БАГ] embed_text() была вложена внутрь create_payload_indexes()
    и никогда не вызывалась корректно. Вынесена в отдельную функцию.
  - [M] Batch-embedding: вместо 1 HTTP-запрос = 1 чанк используется
    ThreadPoolExecutor с BATCH_EMBED потоками — эффективнее на GPU/CPU Ollama.
  - [N] MAX_EMBED_CHARS = 2000 символов (~500–600 токенов): тексты длиннее
    предупреждают и обрезаются до безопасного порога bge-m3.
  - [O] Retry для upsert_batch: при ошибке Qdrant (503 и т.п.) батч
    повторяется до RETRY_COUNT раз с экспоненциальной задержкой.
  - [P] chunk_id дублируется в payload как именованное поле — удобно
    при retrieval debugging и построении цитат источников.
  - [Q] Результаты после as_completed сортируются по chunk["id"] —
    предсказуемый порядок для отладки и восстановления контекста документа.
  - [I] Incremental indexing: флаг --append пропускает delete/create коллекции
    и только upsert-ит новые точки. Дедупликация по doc_id на стороне Qdrant.
  - [B] Поля direction/level/department добавлены в payload и проиндексированы
    для доменной фильтрации retrieval в rpd_generate.py.
  - [S] section_type вынесен на верхний уровень payload (не только в metadata)
    для корректной работы фильтра {"key": "section_type", ...} в rpd_generate.py.

Исправления v3.4:
  - [З-7] ИСПРАВЛЕНО: upsert_batch_with_retry при HTTP 206 (частичная ошибка)
    делал ровно один дополнительный вызов upsert_batch и возвращал его результат,
    не используя оставшиеся RETRY_COUNT попытки. Исправление: вспомогательная
    функция _retry_failed_ids() итерирует до RETRY_COUNT раз, на каждой итерации
    нарезая failed_ids из ответа предыдущей.
  - [N] ИСПРАВЛЕНО: MAX_EMBED_CHARS поднят с 2000 до 4000 символов.
    16 табличных чанков корпуса (2042–2778 символов) обрезались при каждом
    прогоне, снижая качество их векторов. Новый порог ≈ 1000–1300 токенов —
    достаточный запас до лимита bge-m3 (8192 токена).
"""

import argparse
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
# [FIX-#18] Единая embed-функция из utils.py
# [FIX-§5] EMBED_MODEL импортируется из utils: единая точка изменения модели.
#           OLLAMA_URL удалён — мёртвый код после делегирования embed в utils.py.
from utils import get_embedding as _embed_raw_utils, EMBED_MODEL

COLLECTION     = "rpd_rag"
QDRANT_URL     = "http://localhost:6333"
BATCH_EMBED    = 1       # [M] количество параллельных потоков embedding
# [FIX-#16] Прежний комментарий «снижено с 8 до 4» расходился с фактическим
# значением 1. Значение 1 оптимально для локального Ollama: параллельные запросы
# к одному GPU не дают прироста, а только создают очередь.
UPSERT_BATCH   = 64
CHUNKS_FILE    = "chunks.jsonl"
RETRY_COUNT    = 3
RETRY_DELAY    = 2.0
PROGRESS_EVERY = 50

EMBED_DIM = 1024  # bge-m3 размерность — фиксируем явно

# [N] Максимальная длина текста для embedding в символах.
# bge-m3 лимит 8192 токенов; ~3 символа/токен для русского → 24 000 символов.
# Прежний порог 2000 был излишне консервативным: 16 табличных чанков корпуса
# (2042–2778 символов) обрезались при каждом прогоне, снижая качество их векторов.
# Поднято до 4000 символов ≈ 1000–1300 токенов — достаточный запас до лимита bge-m3.
MAX_EMBED_CHARS = 4000


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

def embed_text(text: str) -> list | None:
    """
    Получает embedding для одного текста через utils.get_embedding.
    [FIX-#18] Делегируем в единую реализацию; prefix='passage' для индексируемых текстов.
    [N] Тексты длиннее MAX_EMBED_CHARS обрезаются внутри utils.get_embedding.
    """
    if len(text) > MAX_EMBED_CHARS:
        print(f"  ⚠️  Текст обрезан: {len(text)} → {MAX_EMBED_CHARS} символов")
    vec = _embed_raw_utils(text, prefix="passage", retry=RETRY_COUNT)
    return vec if vec else None


# ---------------------------------------------------------------------------
# Payload индексы
# ---------------------------------------------------------------------------

def create_payload_indexes(collection: str) -> None:
    """
    Создаём индексы для полей фильтрации — ускоряет metadata filtering.

    [B] Добавлены поля direction/level/department для доменной фильтрации.
    [S] section_type на верхнем уровне (не только в metadata).
    """
    fields = [
        ("section_type",          "keyword"),  # [S] верхний уровень
        ("metadata.section_type", "keyword"),  # обратная совместимость
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
                timeout=15,
            )
            if r.status_code in (200, 206):
                print(f"  Индекс создан: {field_name}")
            else:
                print(f"  Индекс {field_name}: {r.status_code}")
        except Exception as e:
            print(f"  Индекс {field_name} не создан: {e}")


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def upsert_batch(ids: list, vectors: list, payloads: list) -> tuple[bool, list]:
    """Загрузка батча в формате batch (ids/vectors/payloads).
    
    [БАГ 11 ИСПРАВЛЕНО]: возвращает (успех, список failed point ids).
    HTTP 206 Partial Content раньше считался полным успехом, но тело содержит
    failed points — они не перезагружались даже при retry.
    """
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
    if r.status_code == 206:
        # Частичная ошибка — разбираем failed points
        try:
            failed = r.json().get("result", {}).get("failed", [])
        except Exception:
            failed = []
        if failed:
            print(f"  ⚠️  HTTP 206: {len(failed)} точек не загружено: {failed[:5]}")
        return True, [f["id"] for f in failed] if failed else []
    if r.status_code not in (200,):
        print(f"  Ошибка upsert: {r.status_code} {r.text[:300]}")
        return False, ids  # всё провалилось
    return True, []


def upsert_batch_with_retry(ids: list, vectors: list, payloads: list) -> bool:
    """
    [O] Retry для upsert_batch при ошибках Qdrant (503, timeout и т.п.).
    [З-L2] ИСПРАВЛЕНО: при HTTP 206 (частичная ошибка) прежняя реализация делала
    ровно один retry для failed_ids и возвращала результат — без цикла до RETRY_COUNT.
    Теперь failed_ids итеративно перезагружаются до RETRY_COUNT раз с экспоненциальной
    задержкой. Точки, не загруженные после всех попыток, явно логируются.
    """
    delay = RETRY_DELAY

    for attempt in range(1, RETRY_COUNT + 1):
        ok, failed_ids = upsert_batch(ids, vectors, payloads)
        if ok and not failed_ids:
            return True
        if ok and failed_ids:
            # Частичная ошибка: сужаем батч до failed points и продолжаем цикл
            print(f"  Retry {len(failed_ids)} failed points (попытка {attempt}/{RETRY_COUNT})...")
            failed_set  = set(failed_ids)
            retry_pairs = [(i, v, p) for i, v, p in zip(ids, vectors, payloads)
                           if i in failed_set]
            ids      = [x[0] for x in retry_pairs]
            vectors  = [x[1] for x in retry_pairs]
            payloads = [x[2] for x in retry_pairs]
            time.sleep(delay)
            delay *= 2
            continue  # следующая итерация — только оставшиеся failed
        # Полная ошибка — retry всего батча
        if attempt < RETRY_COUNT:
            print(f"  Retry upsert {attempt}/{RETRY_COUNT}... ждём {delay:.0f}с")
            time.sleep(delay)
            delay *= 2

    # [БАГ-Б ИСПРАВЛЕНО]: функция возвращала None вместо False после исчерпания
    # всех попыток — implicit falsy, но нарушает сигнатуру -> bool.
    print(f"  ❌ {len(ids)} точек не загружены после {RETRY_COUNT} попыток")
    return False




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

    # [З-L1] ИСПРАВЛЕНО: вместо submit() для всех чанков сразу (до ~2000 pending
    # futures в памяти) обрабатываем батчами по EMBED_QUEUE_BATCH. При медленном
    # Ollama это предотвращает накопление очереди и даёт равномерный прогресс.
    EMBED_QUEUE_BATCH = 200
    results: list = []
    skipped = 0
    done = 0

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
                    print(f"  [{done}/{len(chunks)}] {done/len(chunks)*100:.0f}%  "
                          f"пропущено: {skipped}")

    # [Q] Сортировка результатов по chunk id — предсказуемый порядок
    results.sort(key=lambda x: x[1]["id"])

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
    # [З-L3] ИСПРАВЛЕНО: информативное сообщение вместо голого AssertionError.
    # bge-m3 поддерживает несколько вариантов размерности (1024 / 512 при квантизации).
    # При несоответствии теперь выводится чёткая инструкция по исправлению.
    if vector_size != EMBED_DIM:
        print(
            f"❌ Размерность вектора {vector_size} ≠ EMBED_DIM={EMBED_DIM}.\n"
            f"   Модель: {EMBED_MODEL}. Обновите EMBED_DIM в load_qdrant.py "
            f"или проверьте конфигурацию Ollama."
        )
        return

    # Создание / проверка коллекции
    collection_exists = (
        requests.get(f"{QDRANT_URL}/collections/{COLLECTION}", timeout=10).status_code == 200
    )

    if not append_mode:
        # [I] RECREATE — полная пересборка коллекции
        if collection_exists:
            requests.delete(f"{QDRANT_URL}/collections/{COLLECTION}", timeout=10)
            print(f"Старая коллекция '{COLLECTION}' удалена.")
        r = requests.put(
            f"{QDRANT_URL}/collections/{COLLECTION}",
            json={"vectors": {"size": EMBED_DIM, "distance": "Cosine"}},
            timeout=10,
        )
        r.raise_for_status()
        print(f"Коллекция '{COLLECTION}' создана (вектор: {EMBED_DIM}d).")
        create_payload_indexes(COLLECTION)
    else:
        # [I] APPEND — коллекция должна существовать
        if not collection_exists:
            print(f"  ❌ Коллекция '{COLLECTION}' не найдена. "
                  f"Запустите без --append для первоначальной загрузки.")
            return
        print(f"  Коллекция '{COLLECTION}' существует — добавляем новые точки.")

    # Загрузка батчами с retry
    uploaded = 0
    i = 0

    while i < len(results):
        batch = results[i: i + UPSERT_BATCH]

        ids     = [ch["id"] for _, ch in batch]
        vectors = [vec for vec, _ in batch]

        payloads = []
        for vec, ch in batch:
            meta = ch.get("metadata", {})
            payloads.append({
                # [P] chunk_id дублируется как именованное поле
                "chunk_id":      ch["id"],
                "id":            ch["id"],
                "doc_id":        ch.get("doc_id", ""),
                "source":        ch.get("source", ""),
                # [FIX-#8] Дублируем source как source_file для совместимости
                # с book_loader: retrieve_for_section() читает оба ключа,
                # но явный source_file исключает скрытый double-get.
                "source_file":   ch.get("source", ""),
                "section_title": ch.get("section_title", ""),
                "section_level": ch.get("section_level", ""),
                "doc_position":  ch.get("doc_position", 0),  # [H] из chunking.py
                "text":          ch["text"],
                # [S] section_type на верхнем уровне для прямой фильтрации
                "section_type":  meta.get("section_type", "other"),
                "metadata":      meta,
                # [B] Доменные поля для фильтрации по направлению/уровню
                "direction":     ch.get("direction", ""),
                "level":         ch.get("level", ""),
                "department":    ch.get("department", ""),
                "embedding_model": EMBED_MODEL,
            })

        if upsert_batch_with_retry(ids, vectors, payloads):  # [O]
            uploaded += len(batch)
            print(f"  Загружено: {uploaded}/{len(results)}")

        i += UPSERT_BATCH

    print(f"\nГотово. Загружено: {uploaded}, пропущено: {skipped}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Загрузка чанков РПД в Qdrant")
    parser.add_argument(
        "--append", action="store_true",
        help="[I] Добавить новые точки в существующую коллекцию без пересоздания"
    )
    args = parser.parse_args()
    main(append_mode=args.append)