"""
prepare_texts.py — очистка и дедупликация JSON-блоков РПД в data_clean.jsonl.

Исправления v3:
  - [F] Нормализация маркеров списков: •, ●, ▪, –, — → "- item".
  - [U] Сохранение word_count в каждой записи.
  - Предварительная фильтрация MIN_WORDS.

Исправления v3.1:
  - [T] Сохранение table_data для записей с type="table"/"table_row".

Исправления v3.2:
  - [W] Сохранение token_count_est в каждой записи.
    Замечание: "Нет сохранения длины текста. Полезно сохранять word_count, token_count."
    Точный подсчёт токенов требует tiktoken (отсутствует в prepare_texts.py).
    Используется оценка: token_count_est = round(word_count × 1.5), что соответствует
    среднему коэффициенту слова→токены для русского текста. Downstream-скрипты
    (chunking.py, load_qdrant.py) используют эту метрику для контроля размера.
  - [V] ИСПРАВЛЕНО: поддержка нового формата converter.py v3.2.
    Замечание: "document_metadata хранится внутри первого chunk —
    ошибка архитектуры. Правильная структура: {document: {...}, chunks: [...]}."
    converter.py теперь сохраняет dict {"document_id", "metadata", "chunks"}.
    process_file() поддерживает ОБА формата:
      • новый: dict с ключом "chunks" → итерируем по data["chunks"]
      • старый: list                   → итерируем как раньше
    document-level metadata (title, author) добавляется в каждую запись
    как поле "document_meta" для downstream-использования в retrieval.

Исправления v3.3:
  - [X] ИСПРАВЛЕНО: direction/level/department никогда не записывались в данные.
    Замечание: chunking.py всегда читал "" → load_qdrant хранил "" → фильтр [B]
    в rpd_generate.py не совпадал ни с одним чанком → доменная фильтрация была
    мертва. Добавлена поддержка corpus_meta.json — опциональный файл в DATA_DIR
    формата {"filename.json": {"direction": "...", "level": "...", "department": "..."}}
    process_file() читает из него поля для текущего файла и добавляет в каждую
    запись. Без corpus_meta.json поведение прежнее (пустые строки)."""

import os
import re
import json
import hashlib
import unicodedata
from typing import Tuple

DATA_DIR    = "rpd_json"
OUTPUT_FILE = "data_clean.jsonl"
# [X] Опциональный файл метаданных корпуса.
# Формат: {"имя_файла.json": {"direction": "...", "level": "...", "department": "..."}}
# Позволяет проставить доменные поля для фильтрации в Qdrant / rpd_generate.
CORPUS_META_FILE = os.path.join(DATA_DIR, "corpus_meta.json")

MIN_WORDS = 10


def normalize_list_markers(text: str) -> str:
    """[F] Унифицирует маркеры списков в формат '- item'."""
    text = re.sub(r"^[ \t]*[•●▪◦]\s+", "- ", text, flags=re.MULTILINE)
    text = re.sub(r"^[ \t]*[–—]\s+", "- ", text, flags=re.MULTILINE)
    text = re.sub(r"^[ \t]*\(?(\d+)[.)]\s+", "- ", text, flags=re.MULTILINE)
    return text


def clean_text(text: str) -> str:
    """Очищает текст, сохраняя структуру параграфов и нормализуя маркеры."""
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\x00", "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = normalize_list_markers(text)
    paragraphs = text.split("\n\n")
    cleaned = []
    for para in paragraphs:
        lines = [l.strip() for l in para.split("\n") if l.strip()]
        if lines:
            cleaned.append("\n".join(lines))
    return "\n\n".join(cleaned).strip()


def text_hash(text: str, source: str = "") -> str:
    """Хеш включает source — одинаковые тексты из разных РПД не схлопываются."""
    return hashlib.sha256(f"{source}\x00{text}".encode("utf-8")).hexdigest()


def load_corpus_meta() -> dict:
    """
    [X] Загружает corpus_meta.json если он существует.

    Ожидаемый формат:
    {
      "rpd_ml.json":  {"direction": "09.03.01 Информатика и ВТ",
                       "level": "бакалавриат", "department": "ВТИК"},
      "rpd_ai.json":  {"direction": "09.03.01 Информатика и ВТ", ...},
      ...
    }
    Если файл отсутствует или повреждён — возвращает пустой dict,
    что не меняет поведение относительно предыдущих версий.
    """
    if not os.path.exists(CORPUS_META_FILE):
        return {}
    try:
        with open(CORPUS_META_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception as e:
        print(f"  ⚠️  corpus_meta.json не загружен: {e}")
    return {}


def process_record(
    record: dict, out_file, source: str, seen: set,
    document_meta: dict = None,  # [V] document-level metadata из нового формата
    domain_meta: dict = None,    # [X] direction/level/department из corpus_meta.json
) -> Tuple[bool, bool]:
    """
    Возвращает (записан, пропущен_как_дубль).

    [T] Для type="table"/"table_row" сохраняет table_data.
    [V] Добавляет document_meta из верхнего уровня JSON (новый формат).
    [X] Добавляет direction/level/department из corpus_meta.json.
    """
    if "text" not in record:
        return False, False

    cleaned = clean_text(record["text"])
    if not cleaned:
        return False, False

    word_count = len(cleaned.split())
    if word_count < MIN_WORDS:
        return False, False

    h = text_hash(cleaned, source)
    if h in seen:
        return False, True
    seen.add(h)

    record_type = record.get("type", "text")

    # [W] Оценка token_count: для русского текста ≈ 1.5 токена/слово.
    # Точное значение вычисляется tiktoken в chunking.py,
    # здесь сохраняем быструю оценку для downstream-фильтрации.
    token_count_est = round(word_count * 1.5)

    output_record = {
        "source":           source,
        "document_id":      record.get("document_id", ""),
        "title":            record.get("title"),
        "section_title":    record.get("section_title"),
        "section_level":    record.get("section_level", 0),   # [6] уже int из converter v3.2
        "section_type":     record.get("section_type"),
        "type":             record_type,
        "text":             cleaned,
        "word_count":       word_count,
        "token_count_est":  token_count_est,   # [W]
    }

    # [V] Document-level metadata из нового формата converter
    if document_meta:
        output_record["document_meta"] = document_meta

    # [X] Доменные поля для фильтрации в Qdrant / rpd_generate.py.
    # Берутся из corpus_meta.json; если не заданы — пустая строка (фильтр не применяется).
    dm = domain_meta or {}
    output_record["direction"]  = dm.get("direction",  "")
    output_record["level"]      = dm.get("level",      "")
    output_record["department"] = dm.get("department", "")

    # [T] Для табличных записей сохраняем структуру
    if record_type in ("table", "table_row"):
        table_data = record.get("table_data")
        if table_data:
            output_record["table_data"] = table_data

    out_file.write(json.dumps(output_record, ensure_ascii=False) + "\n")
    return True, False


def process_file(path: str, out_file, seen: set,
                 corpus_meta: dict = None) -> Tuple[int, int]:
    """
    [V] Поддерживает оба формата:
      • новый (converter v3.2): dict {"document_id", "metadata", "chunks"}
      • старый (converter v3.0): list блоков

    [X] Принимает corpus_meta (из load_corpus_meta()) и передаёт
    соответствующие доменные поля в каждую запись через process_record().
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    source  = os.path.basename(path)
    written = dups = 0

    # [X] Доменные поля для текущего файла из corpus_meta.json
    domain_meta = (corpus_meta or {}).get(source)

    # Определяем формат и извлекаем блоки и document_meta
    if isinstance(data, dict) and "chunks" in data:
        # Новый формат converter v3.2
        records       = data["chunks"]
        document_meta = data.get("metadata")
    else:
        # Старый формат: список блоков
        records       = data if isinstance(data, list) else [data]
        document_meta = None

    for r in records:
        ok, dup = process_record(
            r, out_file, source, seen,
            document_meta=document_meta,
            domain_meta=domain_meta,      # [X]
        )
        if ok:
            written += 1
        elif dup:
            dups += 1

    return written, dups


def main():
    seen: set = set()
    total_written = total_dups = 0
    table_count = 0

    # [X] Загружаем corpus_meta.json один раз для всего прогона
    corpus_meta = load_corpus_meta()
    if corpus_meta:
        print(f"corpus_meta.json: загружено {len(corpus_meta)} записей")
    else:
        print("corpus_meta.json: не найден — direction/level/department будут пустыми")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for fn in sorted(os.listdir(DATA_DIR)):
            if not fn.endswith(".json") or fn == "corpus_meta.json":
                continue
            path = os.path.join(DATA_DIR, fn)
            try:
                w, d = process_file(path, out, seen, corpus_meta=corpus_meta)
                total_written += w
                total_dups    += d
                dm = corpus_meta.get(fn, {})
                meta_info = f" [{dm.get('direction','—')}]" if dm else ""
                print(f"  {fn}{meta_info}: записано={w}, дублей={d}")
            except Exception as e:
                print(f"  ❌ {fn}: {e}")

    with open(OUTPUT_FILE, encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("type") in ("table", "table_row") and rec.get("table_data"):
                table_count += 1

    print(f"\nГотово → {OUTPUT_FILE}")
    print(f"  Записано    : {total_written}")
    print(f"  Дублей      : {total_dups}")
    print(f"  Таблиц с table_data: {table_count}")
    print(f"  Уникальность: {total_written / max(total_written + total_dups, 1) * 100:.1f}%")

    # [W] Статистика token_count_est
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        all_toks = [json.loads(l).get("token_count_est", 0) for l in f]
    if all_toks:
        print(f"  token_count_est: min={min(all_toks)}, "
              f"max={max(all_toks)}, avg={sum(all_toks)//len(all_toks)}")

    # [X] Статистика доменных полей
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        directions = {json.loads(l).get("direction", "") for l in f}
    filled = {d for d in directions if d}
    if filled:
        print(f"  direction-значений: {len(filled)} уникальных")
    else:
        print("  ⚠️  direction пуст — создайте corpus_meta.json для доменной фильтрации")


if __name__ == "__main__":
    main()
