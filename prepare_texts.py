"""
prepare_texts.py — очистка и дедупликация JSON-блоков РПД в data_clean.jsonl.

Исправления v3:
  - [F] Нормализация маркеров списков: •, ●, ▪, –, — → "- item";
        нумерованные «1.», «2)» → "- item".
        LLM лучше понимает унифицированный формат при генерации компетенций.
  - [U] Сохранение word_count в каждой записи: chunking.py использует его
        для downstream-фильтрации, не пересчитывая split() повторно.
  - Добавлена предварительная фильтрация записей с MIN_WORDS — отсекает
    служебные фрагменты типа «Таблица 3», «Продолжение», «Примечание».

Логика:
  - Дедупликация по SHA-256(text + source): одинаковые тексты из РАЗНЫХ РПД
    НЕ схлопываются — вариативность корпуса сохраняется для retrieval.
  - clean_text сохраняет двойные переносы строк (структуру параграфов),
    которые chunking.py использует для разбивки.
"""

import os
import re
import json
import hashlib
import unicodedata
from typing import Tuple

DATA_DIR    = "rpd_json"
OUTPUT_FILE = "data_clean.jsonl"

# [U] Минимальный размер текста — фильтрует «Таблица 3», «Продолжение» и т.п.
MIN_WORDS = 10


def normalize_list_markers(text: str) -> str:
    """
    [F] Унифицирует маркеры списков в формат "- item".

    Обрабатывает варианты из реальных РПД:
      • item, ● item, ▪ item   →  - item
      – item, — item (в начале строки)  →  - item
      1. item, 2) item, (3) item  →  - item

    Двойные переносы строк (разделители параграфов) не трогаем —
    они нужны chunking.py как boundary сигнал.
    """
    # Маркированные списки (символьные маркеры)
    text = re.sub(r"^[ \t]*[•●▪◦]\s+", "- ", text, flags=re.MULTILINE)
    # En-dash / em-dash только в начале строки
    # (не трогаем тире внутри предложений)
    text = re.sub(r"^[ \t]*[–—]\s+", "- ", text, flags=re.MULTILINE)
    # Нумерованные списки: «1.», «2)», «(3)»
    text = re.sub(r"^[ \t]*\(?(\d+)[.)]\s+", "- ", text, flags=re.MULTILINE)
    return text


def clean_text(text: str) -> str:
    """Очищает текст, сохраняя структуру параграфов и нормализуя маркеры."""
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\x00", "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Схлопываем 3+ переносов в двойной, двойные НЕ трогаем
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    # [F] Нормализация маркеров до разбивки на параграфы
    text = normalize_list_markers(text)
    paragraphs = text.split("\n\n")
    cleaned = []
    for para in paragraphs:
        lines = [l.strip() for l in para.split("\n") if l.strip()]
        if lines:
            cleaned.append("\n".join(lines))
    return "\n\n".join(cleaned).strip()


def text_hash(text: str, source: str = "") -> str:
    """
    Хеш включает source — одинаковые тексты из разных РПД НЕ схлопываются.

    Это сохраняет вариативность корпуса: стандартный блок «Место дисциплины
    в структуре ОПОП» из разных РПД несёт разный контекст для retrieval.
    """
    return hashlib.sha256(f"{source}\x00{text}".encode("utf-8")).hexdigest()


def process_record(
    record: dict, out_file, source: str, seen: set
) -> Tuple[bool, bool]:
    """Возвращает (записан, пропущен_как_дубль)."""
    if "text" not in record:
        return False, False

    cleaned = clean_text(record["text"])
    if not cleaned:
        return False, False

    # [U] Считаем word_count один раз и фильтруем шум
    word_count = len(cleaned.split())
    if word_count < MIN_WORDS:
        return False, False

    h = text_hash(cleaned, source)
    if h in seen:
        return False, True
    seen.add(h)

    out_file.write(json.dumps({
        "source":        source,
        "title":         record.get("title"),
        "section_title": record.get("section_title"),
        "section_level": record.get("section_level"),
        "section_type":  record.get("section_type"),
        "text":          cleaned,
        "word_count":    word_count,  # [U] используется chunking.py
    }, ensure_ascii=False) + "\n")
    return True, False


def process_file(path: str, out_file, seen: set) -> Tuple[int, int]:
    """Возвращает (записано, дублей)."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    source  = os.path.basename(path)
    written = dups = 0

    for r in (data if isinstance(data, list) else [data]):
        ok, dup = process_record(r, out_file, source, seen)
        if ok:
            written += 1
        elif dup:
            dups += 1

    return written, dups


def main():
    seen: set = set()
    total_written = total_dups = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for fn in sorted(os.listdir(DATA_DIR)):
            if not fn.endswith(".json"):
                continue
            path = os.path.join(DATA_DIR, fn)
            try:
                w, d = process_file(path, out, seen)
                total_written += w
                total_dups    += d
                print(f"  {fn}: записано={w}, дублей={d}")
            except Exception as e:
                print(f"  ❌ {fn}: {e}")

    print(f"\nГотово → {OUTPUT_FILE}")
    print(f"  Записано    : {total_written}")
    print(f"  Дублей      : {total_dups}")
    print(f"  Уникальность: {total_written / max(total_written + total_dups, 1) * 100:.1f}%")


if __name__ == "__main__":
    main()
