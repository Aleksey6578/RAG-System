"""
chunking.py — нарезка очищенных текстов РПД на чанки.

Исправления v2:
  - БАГ: smart_split → flush(): current_size пересчитывался через
    len(last.split()), но last — это текст, склеенный через "\n\n",
    и split() по пробелам давал заниженный wc, нарушая overlap.
    Исправлено: word count хранится отдельно в current_words list.
  - БАГ: sliding window для длинных параграфов генерировал чанки без
    проверки MIN_WORDS внутри smart_split — теперь фильтрация добавлена.
  - Дедупликация чанков по SHA-256.
  - classify_section расширен: ловит реальные заголовки РПД.
  - Статистика по типам разделов в итоговом выводе.
"""

import json
import hashlib
import re
from collections import Counter

INPUT_FILE  = "data_clean.jsonl"
OUTPUT_FILE = "chunks.jsonl"
MAX_TOKENS  = 300
OVERLAP     = 50
MIN_WORDS   = 30
# Замечание №8: лимит чанков на один источник — предотвращает доминирование rpd_1
MAX_CHUNKS_PER_SOURCE = 40

# Замечание №10: шумовые заголовки — только явный мусор, СВЕДЕНИЯ убираем из блока
NOISE_TITLES = {
    "УТВЕРЖДАЮ", "СОГЛАСОВАНО",
    "РАБОЧАЯ ПРОГРАММА ДИСЦИПЛИНЫ",
    "рабочая программа дисциплины",
}


def classify_section(title: str) -> str:
    if not title:
        return "other"
    t = title.lower()
    if re.search(r"цел[ьи]|задач[аи]", t):                                     return "goals"
    if re.search(r"компетенц", t):                                               return "competencies"
    # ОВЗ/доступность — РАНЬШЕ learning_outcomes:
    # «обучения лиц с ограниченными возможностями здоровья» иначе ловится
    # по слову «обучен» и ошибочно уходит в learning_outcomes.
    if re.search(r"доступн|инвалид|огранич.{0,15}возможн|здоровь|овз", t):     return "accessibility"
    # learning_outcomes: требуем «результат» + «обучен» рядом, либо «индикатор»
    if re.search(r"результат.{0,10}обучен|индикатор", t):                       return "learning_outcomes"
    if re.search(r"содержан|лекц|лаборатор|практич|тем[аы]", t):               return "content"
    if re.search(r"фос|фонд оценочн|оценочн|аттестац|контрол", t):             return "assessment"
    if re.search(r"литература|библиограф|учебно.метод", t):                     return "bibliography"
    if re.search(r"методическ", t):                                             return "methodical"
    if re.search(r"место.{0,15}дисципл|структур.{0,10}опоп", t):               return "place"
    if re.search(r"матери.{0,10}техн|аудитор|оборудован", t):                  return "infrastructure"
    if re.search(r"час[ыа]|трудоёмк|трудоем|семестр", t):                      return "hours"
    return "other"


def extract_metadata(text: str, section_title: str, block_stype: str = None) -> dict:
    # Вычисляем classify_section всегда — он содержит актуальные паттерны.
    title_stype = classify_section(section_title)
    if block_stype and block_stype != "other":
        # Доверяем block_stype из конвертера, но ПЕРЕЗАПИСЫВАЕМ если title_stype
        # даёт более специфичный результат и они несовместимы.
        # Пример: block_stype="learning_outcomes", title_stype="accessibility"
        # → ОВЗ-секция ошибочно классифицирована старым конвертером.
        INCOMPATIBLE = {
            ("learning_outcomes", "accessibility"),
            ("learning_outcomes", "hours"),
            ("learning_outcomes", "assessment"),
            ("competencies",      "accessibility"),
        }
        if (block_stype, title_stype) in INCOMPATIBLE:
            stype = title_stype   # title_stype точнее
        else:
            stype = block_stype   # доверяем конвертеру
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


def text_hash(text: str) -> str:
    return hashlib.sha256(text.strip().encode("utf-8")).hexdigest()


def smart_split(text: str, max_tokens: int = MAX_TOKENS, overlap: int = OVERLAP) -> list[str]:
    """
    Разбивает текст на чанки по параграфам.

    ИСПРАВЛЕНИЕ 1: word-count для overlap теперь хранится в current_words
    (список отдельных wc для каждого параграфа), а не пересчитывается через
    split() от склеенной строки — что давало неверный результат при наличии \n\n.

    ИСПРАВЛЕНИЕ 2: чанки из sliding window длинных параграфов проверяются
    на MIN_WORDS до добавления в результат.
    """
    paragraphs = text.split("\n\n")
    chunks:       list[str] = []
    current:      list[str] = []   # параграфы текущего чанка
    current_wcs:  list[int] = []   # wc каждого параграфа в current (для overlap)
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
            # ИСПРАВЛЕНИЕ: current_size берём из сохранённого wc, а не split()
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

        if wc > max_tokens:
            if current:
                flush(keep_last=False)
            start = 0
            while start < len(words):
                end   = start + max_tokens
                chunk = " ".join(words[start:end])
                # ИСПРАВЛЕНИЕ: фильтрация коротких чанков из sliding window
                if len(chunk.split()) >= MIN_WORDS:
                    chunks.append(chunk)
                start += max_tokens - overlap
            continue

        if current_size + wc > max_tokens and current:
            flush(keep_last=True)

        current.append(para)
        current_wcs.append(wc)
        current_size += wc

    if current:
        chunks.append("\n\n".join(current))

    return chunks


def group_short_chunks(records: list, max_group_words: int = 150) -> list:
    """
    Замечание №9: строки таблиц (30-40 слов) неинформативны по одной.
    Группируем соседние строки одного источника и section_title в более крупные чанки.
    Только для записей типа competencies, learning_outcomes, content, assessment.
    """
    GROUPABLE = {"competencies", "learning_outcomes", "content", "assessment"}
    result = []
    i = 0
    while i < len(records):
        r = records[i]
        stype = r.get("section_type")
        wc = len(r["text"].split())

        # Группируем только короткие записи из группируемых типов
        if stype in GROUPABLE and wc < 60:
            group_text = r["text"]
            group_wc = wc
            j = i + 1
            while j < len(records):
                nxt = records[j]
                # Группируем только записи из того же источника и секции
                if (nxt.get("section_type") == stype and
                        nxt.get("source") == r.get("source") and
                        nxt.get("section_title") == r.get("section_title")):
                    nxt_wc = len(nxt["text"].split())
                    if group_wc + nxt_wc <= max_group_words:
                        group_text += "\n---\n" + nxt["text"]
                        group_wc += nxt_wc
                        j += 1
                        continue
                break
            # Создаём объединённую запись
            merged = dict(r)
            merged["text"] = group_text
            result.append(merged)
            i = j
        else:
            result.append(r)
            i += 1
    return result


def generate_doc_id(source: str) -> str:
    return hashlib.md5(source.encode()).hexdigest()


def main():
    with open(INPUT_FILE, encoding="utf-8") as f:
        raw_records = [json.loads(line) for line in f]

    # Замечание №9: объединяем короткие строки таблиц в более крупные чанки
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
        block_stype   = record.get("section_type")  # из конвертера
        doc_id        = generate_doc_id(source)

        stats_source.setdefault(source, {"records": 0, "chunks": 0, "dups": 0})
        stats_source[source]["records"] += 1

        # Пропускаем шумовые разделы
        if section_title and section_title.strip() in NOISE_TITLES:
            continue

        # Замечание №8: лимит чанков на источник
        if stats_source[source]["chunks"] >= MAX_CHUNKS_PER_SOURCE:
            continue

        for idx, chunk in enumerate(smart_split(text, MAX_TOKENS, OVERLAP)):
            if len(chunk.split()) < MIN_WORDS:
                continue

            h = text_hash(chunk)
            if h in seen_hashes:
                dup_count += 1
                stats_source[source]["dups"] += 1
                continue
            seen_hashes.add(h)

            chunks_out.append({
                "id":            global_chunk_id,
                "doc_id":        doc_id,
                "chunk_index":   idx,
                "source":        source,
                "section_title": section_title,
                "section_level": section_level,
                "text":          chunk,
                "metadata":      extract_metadata(chunk, section_title, block_stype),
            })
            global_chunk_id                += 1
            stats_source[source]["chunks"] += 1

            # Замечание №8: проверяем лимит после добавления
            if stats_source[source]["chunks"] >= MAX_CHUNKS_PER_SOURCE:
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
