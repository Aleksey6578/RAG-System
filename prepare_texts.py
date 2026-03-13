"""
prepare_texts.py — очистка и дедупликация JSON-блоков РПД в data_clean.jsonl.

Баги: отсутствуют. Скрипт корректен.

Логика:
  - Дедупликация по SHA-256 от текста: одинаковые тексты из разных РПД
    записываются только один раз (первое вхождение сохраняется).
  - clean_text сохраняет двойные переносы строк (структуру параграфов),
    которые chunking.py использует для разбивки.
  - Статистика: сколько записей пропущено как дубли.
"""

import os
import re
import json
import hashlib
import unicodedata
from typing import Tuple

DATA_DIR    = "rpd_json"
OUTPUT_FILE = "data_clean.jsonl"


def clean_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\x00", "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Схлопываем 3+ переносов в двойной, двойные НЕ трогаем —
    # они нужны chunking.py как разделители параграфов
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    paragraphs = text.split("\n\n")
    cleaned = []
    for para in paragraphs:
        lines = [l.strip() for l in para.split("\n") if l.strip()]
        if lines:
            cleaned.append("\n".join(lines))
    return "\n\n".join(cleaned).strip()


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def process_record(
    record: dict, out_file, source: str, seen: set
) -> Tuple[bool, bool]:
    """Возвращает (записан, пропущен_как_дубль)."""
    if "text" not in record:
        return False, False

    cleaned = clean_text(record["text"])
    if not cleaned:
        return False, False

    h = text_hash(cleaned)
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
    print(f"  Записано  : {total_written}")
    print(f"  Дублей    : {total_dups}")
    print(f"  Уникальность: {total_written / max(total_written + total_dups, 1) * 100:.1f}%")


if __name__ == "__main__":
    main()
