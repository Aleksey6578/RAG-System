"""
chunking.py — нарезка очищенных текстов РПД на чанки.

Исправления v3:
  - [1] БАГ: smart_split → flush(): current_size пересчитывался через
    len(last.split()), но last — это текст, склеенный через "\\n\\n",
    и split() по пробелам давал заниженный wc, нарушая overlap.
    Исправлено: word count хранится отдельно в current_words list.
  - [2] БАГ: sliding window для длинных параграфов генерировал чанки без
    проверки MIN_WORDS внутри smart_split — теперь фильтрация добавлена.
  - [3] БАГ: f.write() стоял внутри цикла без открытого файла — исправлено,
    запись вынесена в отдельный with-блок после всех циклов.
  - [G] MAX_TOKENS переименован в MAX_WORDS с явным комментарием о токенах:
    bge-m3 работает в токенах (лимит 8192), но для простоты считаем в словах.
    300 слов ≈ 450–600 токенов для русского текста — в пределах лимита.
  - [H] Добавлен doc_position: порядковый номер чанка внутри документа.
    Позволяет восстанавливать контекст соседних чанков при retrieval.
  - GROUPABLE без competencies/learning_outcomes: каждая компетенция/результат
    остаётся отдельным чанком, иначе embedding размывается.
  - Лимит per (source, section_type) вместо per source: предотвращает потерю
    хвостовых разделов (библиография, ФОС) при большом числе чанков в начале.
  - Дедупликация чанков по SHA-256(text + source).
  - classify_section расширен: ловит реальные заголовки РПД.
  - Статистика по типам разделов в итоговом выводе.
"""

import json
import hashlib
import re
from collections import Counter

INPUT_FILE  = "data_clean.jsonl"
OUTPUT_FILE = "chunks.jsonl"

# [G] Размер чанка в словах (НЕ в токенах).
# bge-m3 работает в токенах, лимит 8192.
# Для русского текста: 1 слово ≈ 1.5–2 токена.
# 300 слов ≈ 450–600 токенов — безопасно в пределах лимита модели.
# Для точного подсчёта токенов использовать tiktoken или sentencepiece.
MAX_WORDS = 300   # ≈ 450–600 токенов bge-m3

OVERLAP   = 50    # слов overlap между соседними чанками
MIN_WORDS = 30    # минимальный размер чанка в словах

# [H] Лимит чанков на один раздел (section_type) одного источника.
# Замена MAX_CHUNKS_PER_SOURCE (на весь документ) → по 15 на тип раздела:
# предотвращает потерю хвостовых разделов при большом объёме начальных.
MAX_CHUNKS_PER_SECTION_TYPE = 15

NOISE_TITLES = {
    "УТВЕРЖДАЮ", "СОГЛАСОВАНО", "СВЕДЕНИЯ",
    "РАБОЧАЯ ПРОГРАММА ДИСЦИПЛИНЫ",
    "рабочая программа дисциплины",
}


def classify_section(title: str) -> str:
    if not title:
        return "other"
    t = title.lower()
    if re.search(r"цел[ьи]|задач[аи]", t):                                     return "goals"
    if re.search(r"компетенц", t):                                               return "competencies"
    # ОВЗ/доступность — РАНЬШЕ learning_outcomes, чтобы «обучения лиц с ОВЗ»
    # не классифицировалось как learning_outcomes через слово «обучен».
    if re.search(r"доступн|инвалид|огранич.{0,15}возможн|здоровь|овз", t):     return "accessibility"
    # learning_outcomes: требуем «результат» + «обучен» рядом, либо «индикатор»
    if re.search(r"результат.{0,10}обучен|индикатор", t):                       return "learning_outcomes"
    if re.search(r"содержан|лекц|лаборатор|практич|тем[аы]", t):               return "content"
    if re.search(r"фос|фонд оценочн|оценочн|аттестац|контрол|виды\s+сро|самостоятельн", t): return "assessment"
    if re.search(r"литература|библиограф|учебно.метод", t):                     return "bibliography"
    if re.search(r"методическ", t):                                             return "methodical"
    if re.search(r"место.{0,15}дисципл|структур.{0,10}опоп", t):               return "place"
    if re.search(r"матери.{0,10}техн|аудитор|оборудован", t):                  return "infrastructure"
    if re.search(r"час[ыа]|трудоёмк|трудоем|семестр", t):                      return "hours"
    return "other"


def extract_metadata(text: str, section_title: str, block_stype: str = None) -> dict:
    title_stype = classify_section(section_title)
    if block_stype and block_stype != "other":
        INCOMPATIBLE = {
            ("learning_outcomes", "accessibility"),
            ("learning_outcomes", "hours"),
            ("learning_outcomes", "assessment"),
            ("competencies",      "accessibility"),
        }
        if (block_stype, title_stype) in INCOMPATIBLE:
            stype = title_stype
        else:
            stype = block_stype
    else:
        stype = title_stype
    return {
        "has_competencies":      bool(re.search(r"УК-\d+|ОПК-\d+|ПК-\d+", text)),
        "has_learning_outcomes": bool(re.search(r"\b(знать|уметь|владеть)\b", text.lower())),
        "has_list":              bool(re.search(r"(^\d+\.|^•|^-)", text, re.MULTILINE)),
        "word_count":            len(text.split()),
        "section_type":          stype,
        "is_substantive":        len(text.split()) > 50,
    }


def text_hash(text: str, source: str = "") -> str:
    """Хеш включает source — одинаковые тексты из разных РПД НЕ схлопываются."""
    return hashlib.sha256(f"{source}\x00{text.strip()}".encode("utf-8")).hexdigest()


# Служебные фразы внутри текста чанка — фильтруем строки, а не заголовки
NOISE_LINE_PATTERNS = re.compile(
    r"^(продолжение\s+таблицы|таблица\s+\d+|окончание\s+таблицы|примечание[\s:—]|"
    r"рисунок\s+\d+|рис\.\s+\d+|источник:|составлено\s+автором)",
    re.IGNORECASE
)


def filter_noise_lines(text: str) -> str:
    """Удаляет служебные строки из тела чанка."""
    lines = [l for l in text.split("\n")
             if not NOISE_LINE_PATTERNS.match(l.strip())]
    return "\n".join(lines).strip()


def smart_split(text: str, max_words: int = MAX_WORDS, overlap: int = OVERLAP) -> list[str]:
    """
    Разбивает текст на чанки по параграфам с overlap.

    ИСПРАВЛЕНИЕ 1: word-count для overlap хранится в current_wcs
    (список отдельных wc для каждого параграфа), а не пересчитывается через
    split() от склеенной строки — что давало неверный результат при \n\n.

    ИСПРАВЛЕНИЕ 2: чанки из sliding window длинных параграфов проверяются
    на MIN_WORDS до добавления в результат.
    """
    paragraphs = text.split("\n\n")
    chunks:       list[str] = []
    current:      list[str] = []
    current_wcs:  list[int] = []
    current_size: int       = 0

    def flush(keep_last: bool = True):
        nonlocal current, current_wcs, current_size
        if current:
            chunks.append("\n\n".join(current))
        if keep_last and current:
            last     = current[-1]
            last_wc  = current_wcs[-1]
            current      = [last]
            current_wcs  = [last_wc]
            current_size = last_wc
        else:
            current      = []
            current_wcs  = []
            current_size = 0

    for para in paragraphs:
        words = para.split()
        wc    = len(words)
        if wc == 0:
            continue

        if wc > max_words:
            if current:
                flush(keep_last=False)
            start = 0
            while start < len(words):
                end   = start + max_words
                chunk = " ".join(words[start:end])
                if len(chunk.split()) >= MIN_WORDS:
                    chunks.append(chunk)
                start += max_words - overlap
            continue

        if current_size + wc > max_words and current:
            flush(keep_last=True)

        current.append(para)
        current_wcs.append(wc)
        current_size += wc

    if current:
        chunks.append("\n\n".join(current))

    return chunks


def generate_doc_id(source: str) -> str:
    return hashlib.md5(source.encode()).hexdigest()


def group_short_chunks(records: list, max_group_words: int = 150) -> list:
    """
    Группируем короткие строки таблиц одного источника/секции в крупные чанки.

    competencies и learning_outcomes убраны из GROUPABLE:
    каждая компетенция/результат обучения остаётся отдельным чанком,
    иначе embedding размывается по нескольким несвязанным компетенциям.
    Только content и assessment допускают объединение строк.
    """
    GROUPABLE = {"content", "assessment"}
    result = []
    i = 0
    while i < len(records):
        r = records[i]
        stype = r.get("section_type")
        wc = r.get("word_count") or len(r["text"].split())  # [U] используем если есть
        if stype in GROUPABLE and wc < 60:
            group_text = r["text"]
            group_wc = wc
            j = i + 1
            while j < len(records):
                nxt = records[j]
                if (nxt.get("section_type") == stype and
                        nxt.get("source") == r.get("source") and
                        nxt.get("section_title") == r.get("section_title")):
                    nxt_wc = nxt.get("word_count") or len(nxt["text"].split())
                    if group_wc + nxt_wc <= max_group_words:
                        group_text += "\n---\n" + nxt["text"]
                        group_wc += nxt_wc
                        j += 1
                        continue
                break
            merged = dict(r)
            merged["text"] = group_text
            merged["word_count"] = group_wc
            result.append(merged)
            i = j
        else:
            result.append(r)
            i += 1
    return result


def main():
    with open(INPUT_FILE, encoding="utf-8") as f:
        raw_records = [json.loads(line) for line in f]

    records = group_short_chunks(raw_records)
    print(f"Записей после группировки: {len(records)} (было {len(raw_records)})")

    chunks_out:      list[dict] = []
    global_chunk_id: int        = 0
    seen_hashes:     set        = set()
    stats_source:    dict       = {}
    dup_count = 0

    for record in records:
        text          = record["text"]
        source        = record["source"]
        section_title = record.get("section_title")
        section_level = record.get("section_level")
        block_stype   = record.get("section_type")
        doc_id        = generate_doc_id(source)

        stats_source.setdefault(source, {
            "records": 0, "chunks": 0, "dups": 0, "by_stype": {}
        })
        stats_source[source]["records"] += 1

        if section_title and section_title.strip() in NOISE_TITLES:
            continue

        stype_for_limit = (
            block_stype if block_stype and block_stype != "other"
            else classify_section(section_title)
        )

        stype_count = stats_source[source]["by_stype"].get(stype_for_limit, 0)
        if stype_count >= MAX_CHUNKS_PER_SECTION_TYPE:
            continue

        # [H] doc_position: счётчик чанков внутри данного источника
        doc_pos_start = stats_source[source]["chunks"]

        for idx, chunk in enumerate(smart_split(text, MAX_WORDS, OVERLAP)):
            if len(chunk.split()) < MIN_WORDS:
                continue

            h = text_hash(chunk, source)
            if h in seen_hashes:
                dup_count += 1
                stats_source[source]["dups"] += 1
                continue
            seen_hashes.add(h)

            clean_chunk = filter_noise_lines(chunk)
            if len(clean_chunk.split()) < MIN_WORDS:
                continue

            chunks_out.append({
                "id":           global_chunk_id,
                "doc_id":       doc_id,
                "chunk_index":  idx,
                "doc_position": doc_pos_start + idx,  # [H] позиция внутри документа
                "source":       source,
                "section_title": section_title,
                "section_level": section_level,
                "text":         clean_chunk,
                "metadata":     extract_metadata(clean_chunk, section_title, block_stype),
            })
            global_chunk_id += 1
            stats_source[source]["chunks"] += 1
            stype_count += 1
            stats_source[source]["by_stype"][stype_for_limit] = stype_count

            if stype_count >= MAX_CHUNKS_PER_SECTION_TYPE:
                break

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for c in chunks_out:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")

    print(f"Создано уникальных чанков: {len(chunks_out)} (дублей пропущено: {dup_count})")

    print(f"\n{'Источник':<40} {'Зап.':>5} {'Чанков':>7} {'Дублей':>7}")
    print("-" * 62)
    for src, s in sorted(stats_source.items()):
        print(f"{src:<40} {s['records']:>5} {s['chunks']:>7} {s['dups']:>7}")

    type_counts = Counter(c["metadata"]["section_type"] for c in chunks_out)
    print(f"\nПо типу раздела:")
    for t, n in type_counts.most_common():
        print(f"  {t:<20}: {n}")


if __name__ == "__main__":
    main()
