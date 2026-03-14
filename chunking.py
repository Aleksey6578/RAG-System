"""
chunking.py — нарезка очищенных текстов РПД на чанки.

Исправления v3: [1] overlap wc, [2] sliding window MIN_WORDS, [3] f.write,
  [G] MAX_WORDS→токены, [H] doc_position, GROUPABLE, per-section limit,
  дедупликация с source, расширенный classify_section, NOISE_LINE_PATTERNS.

Исправления v3.1:
  - [K] Подсчёт размера чанка через tiktoken (с fallback на слова×1.5).

Исправления v3.2:
  - [L] ИСПРАВЛЕНО: overlap-механизм в smart_split.
  - [M] ИСПРАВЛЕНО: разделение metadata на section_metadata и chunk_metadata.

Исправления v3.3:
  - [N] ИСПРАВЛЕНО: direction/level/department никогда не записывались в чанки.
    load_qdrant.py ожидает ch.get("direction") и т.д., но chunking.py никогда
    не передавал эти поля — в Qdrant всегда хранилось "". Фильтр в
    rpd_generate.py добавлял условие direction = "09.03.01...", которое
    не совпадало ни с одним чанком → доменная фильтрация [B] всегда молча
    падала в fallback без фильтра, хотя в логе выглядела рабочей.
    Исправление: читаем direction/level/department из записи prepare_texts
    (они там будут, если upstream добавит их в corpus_meta.json или вручную),
    и всегда пишем в chunk output. Пустая строка — допустимое значение,
    load_qdrant.py и rpd_generate.py это обрабатывают корректно.

  - [O] ИСПРАВЛЕНО: group_short_chunks обновлял word_count после слияния,
    но не обновлял token_count_est (поле добавлено в prepare_texts v3.2).
    Несоответствие приводило к тому, что merged-запись могла иметь
    token_count_est от одного чанка, а word_count — от объединённого текста.
"""

import json
import hashlib
import re
from collections import Counter

INPUT_FILE  = "data_clean.jsonl"
OUTPUT_FILE = "chunks.jsonl"

# ---------------------------------------------------------------------------
# [K] Токенизатор — tiktoken с graceful fallback
# ---------------------------------------------------------------------------

try:
    import tiktoken
    _enc = tiktoken.get_encoding("cl100k_base")

    def count_tokens(text: str) -> int:
        return len(_enc.encode(text))

    _COUNT_MODE  = "токены (tiktoken cl100k_base)"
    MAX_TOKENS   = 400
    OVERLAP_TOKENS = 60
    MIN_TOKENS   = 40

except ImportError:
    _WORD_TO_TOKEN = 1.5

    def count_tokens(text: str) -> int:
        return int(len(text.split()) * _WORD_TO_TOKEN)

    _COUNT_MODE  = f"слова×{_WORD_TO_TOKEN} (tiktoken не установлен)"
    MAX_TOKENS   = 450
    OVERLAP_TOKENS = 75
    MIN_TOKENS   = 45

MAX_WORDS = MAX_TOKENS
OVERLAP   = OVERLAP_TOKENS
MIN_WORDS = MIN_TOKENS

MAX_CHUNKS_PER_SECTION_TYPE = 25  # [БАГ 9 ИСПРАВЛЕНО]: было 15 — крупные РПД теряли контент без предупреждения

NOISE_TITLES = {
    "УТВЕРЖДАЮ", "СОГЛАСОВАНО", "СВЕДЕНИЯ",
    "РАБОЧАЯ ПРОГРАММА ДИСЦИПЛИНЫ",
    "рабочая программа дисциплины",
}
# [БАГ 3 ИСПРАВЛЕНО]: нормализованный набор для регистронезависимого сравнения.
# Раньше section_title.strip() in NOISE_TITLES — точное совпадение регистра.
# "Рабочая программа дисциплины" (Title Case) не совпадало ни с UPPER ни с lower.
NOISE_TITLES_LOWER = {t.lower() for t in NOISE_TITLES}


def classify_section(title: str) -> str:
    if not title:
        return "other"
    t = title.lower()
    if re.search(r"цел[ьи]|задач[аи]", t):                                     return "goals"
    if re.search(r"компетенц", t):                                               return "competencies"
    if re.search(r"доступн|инвалид|огранич.{0,15}возможн|здоровь|овз", t):     return "accessibility"
    if re.search(r"результат.{0,10}обучен|индикатор", t):                       return "learning_outcomes"
    if re.search(r"содержан|лекц|лаборатор|практич|тем[аы]", t):               return "content"
    if re.search(r"фос|фонд оценочн|оценочн|аттестац|контрол|виды\s+сро|самостоятельн", t): return "assessment"
    if re.search(r"литература|библиограф|учебно.метод", t):                     return "bibliography"
    if re.search(r"методическ", t):                                             return "methodical"
    if re.search(r"место.{0,15}дисципл|структур.{0,10}опоп", t):               return "place"
    if re.search(r"матери.{0,10}техн|аудитор|оборудован", t):                  return "infrastructure"
    if re.search(r"час[ыа]|трудоёмк|трудоем|семестр", t):                      return "hours"
    return "other"


def build_metadata(text: str, section_title: str, source: str,
                   block_stype: str = None) -> tuple[dict, dict]:
    """
    [M] Возвращает (section_metadata, chunk_metadata).

    section_metadata — признаки уровня раздела:
      section_type, source, section_title

    chunk_metadata — признаки конкретного текстового фрагмента:
      has_competencies, has_learning_outcomes, has_list,
      word_count, token_count, is_substantive
    """
    # Определяем section_type
    title_stype = classify_section(section_title)
    if block_stype and block_stype != "other":
        INCOMPATIBLE = {
            ("learning_outcomes", "accessibility"),
            ("learning_outcomes", "hours"),
            ("learning_outcomes", "assessment"),
            ("competencies",      "accessibility"),
        }
        stype = title_stype if (block_stype, title_stype) in INCOMPATIBLE else block_stype
    else:
        stype = title_stype

    tc = count_tokens(text)

    section_metadata = {
        "section_type":  stype,
        "source":        source,
        "section_title": section_title or "",
    }

    chunk_metadata = {
        "has_competencies":      bool(re.search(r"УК-\d+|ОПК-\d+|ПК-\d+", text)),
        "has_learning_outcomes": bool(re.search(r"\b(знать|уметь|владеть)\b", text.lower())),
        "has_list":              bool(re.search(r"(^\d+\.|^•|^-)", text, re.MULTILINE)),
        "word_count":            len(text.split()),
        "token_count":           tc,
        "is_substantive":        tc > MIN_TOKENS,
    }

    return section_metadata, chunk_metadata


def extract_metadata(text: str, section_title: str, block_stype: str = None) -> dict:
    """
    Обратная совместимость: возвращает объединённую metadata
    (используется load_qdrant.py через поле "metadata" в чанке).
    """
    sec_meta, chunk_meta = build_metadata(text, section_title, "", block_stype)
    return {**chunk_meta, "section_type": sec_meta["section_type"]}


def text_hash(text: str, source: str = "") -> str:
    return hashlib.sha256(f"{source}\x00{text.strip()}".encode("utf-8")).hexdigest()


NOISE_LINE_PATTERNS = re.compile(
    r"^(продолжение\s+таблицы|таблица\s+\d+|окончание\s+таблицы|примечание[\s:—]|"
    r"рисунок\s+\d+|рис\.\s+\d+|источник:|составлено\s+автором)",
    re.IGNORECASE
)


def filter_noise_lines(text: str) -> str:
    lines = [l for l in text.split("\n")
             if not NOISE_LINE_PATTERNS.match(l.strip())]
    return "\n".join(lines).strip()


def smart_split(text: str,
                max_tokens: int = MAX_TOKENS,
                overlap: int = OVERLAP_TOKENS) -> list[str]:
    """
    Разбивает текст на чанки по параграфам с token-based overlap.

    [L] ИСПРАВЛЕН overlap-механизм:
    Вместо сохранения только последнего параграфа (что давало overlap ≈ 20 слов
    если параграф был коротким), flush() теперь накапливает параграфы с конца,
    пока суммарное число токенов не достигнет целевого OVERLAP_TOKENS.

    Гарантия: overlap всегда >= min(OVERLAP_TOKENS, размер_последнего_чанка).
    """
    paragraphs = text.split("\n\n")
    chunks:      list[str] = []
    current:     list[str] = []
    current_tcs: list[int] = []
    current_size: int       = 0

    def flush(keep_overlap: bool = True):
        """
        [L] При keep_overlap=True накапливаем параграфы с конца
        до набора >= overlap токенов (но не больше max_tokens).
        """
        nonlocal current, current_tcs, current_size
        if current:
            chunks.append("\n\n".join(current))

        if keep_overlap and current:
            # Идём с конца, набираем overlap
            overlap_paras: list[str] = []
            overlap_tcs:   list[int] = []
            overlap_total: int       = 0

            for para, tc in zip(reversed(current), reversed(current_tcs)):
                overlap_paras.insert(0, para)
                overlap_tcs.insert(0, tc)
                overlap_total += tc
                if overlap_total >= overlap:
                    break  # набрали достаточно

            current      = overlap_paras
            current_tcs  = overlap_tcs
            current_size = overlap_total
        else:
            current      = []
            current_tcs  = []
            current_size = 0

    for para in paragraphs:
        words = para.split()
        if not words:
            continue

        tc = count_tokens(para)

        if tc > max_tokens:
            if current:
                flush(keep_overlap=False)
            # Sliding window по словам
            start = 0
            while start < len(words):
                # Берём слова пока не наберём max_tokens
                end = start + 1
                while end <= len(words) and count_tokens(" ".join(words[start:end])) < max_tokens:
                    end += 1
                chunk_text = " ".join(words[start:end - 1]) if end > start + 1 else " ".join(words[start:end])
                if count_tokens(chunk_text) >= MIN_TOKENS:
                    chunks.append(chunk_text)
                # Сдвигаем на (max_tokens - overlap) токенов вперёд.
                # [БАГ 4 ИСПРАВЛЕНО]: overlap_words считал слово, вызвавшее break,
                # как НЕ посчитанное (overlap_words += 1 стоит ПОСЛЕ break).
                # Теперь инкрементируем ДО проверки условия — overlap точный.
                overlap_words = 0
                overlap_tc = 0
                for w in reversed(words[start: end]):
                    overlap_words += 1
                    overlap_tc += count_tokens(w)
                    if overlap_tc >= overlap:
                        break
                start = max(start + 1, end - 1 - overlap_words)
            # [БАГ 10 ИСПРАВЛЕНО]: после sliding window выполнялся continue —
            # параграф не попадал в current, и следующий нормальный параграф
            # начинался без overlap, разрывая контекстную связность.
            # Теперь сохраняем хвост последнего чанка как seed для overlap.
            if chunks:
                tail_words = chunks[-1].split()[-overlap * 2:]  # берём с запасом
                tail_text = " ".join(tail_words)
                if count_tokens(tail_text) >= MIN_TOKENS:
                    current.append(tail_text)
                    current_tcs.append(count_tokens(tail_text))
                    current_size = current_tcs[-1]
            continue

        if current_size + tc > max_tokens and current:
            flush(keep_overlap=True)

        current.append(para)
        current_tcs.append(tc)
        current_size += tc

    if current:
        chunks.append("\n\n".join(current))

    return chunks


def generate_doc_id(source: str) -> str:
    return hashlib.md5(source.encode()).hexdigest()


def group_short_chunks(records: list, max_group_tokens: int = 200) -> list:
    """Группировка коротких строк. GROUPABLE без competencies/learning_outcomes."""
    GROUPABLE = {"content", "assessment"}
    result = []
    i = 0
    while i < len(records):
        r = records[i]
        stype = r.get("section_type")
        tc = count_tokens(r["text"])
        if stype in GROUPABLE and tc < 90:
            group_text = r["text"]
            group_tc   = tc
            j = i + 1
            while j < len(records):
                nxt = records[j]
                if (nxt.get("section_type") == stype and
                        nxt.get("source") == r.get("source") and
                        nxt.get("section_title") == r.get("section_title")):
                    nxt_tc = count_tokens(nxt["text"])
                    if group_tc + nxt_tc <= max_group_tokens:
                        group_text += "\n---\n" + nxt["text"]
                        group_tc   += nxt_tc
                        j += 1
                        continue
                break
            merged = dict(r)
            merged["text"]            = group_text
            merged["word_count"]      = len(group_text.split())
            # [O] Обновляем token_count_est после слияния.
            # Раньше merged["token_count_est"] оставался от первого чанка r,
            # тогда как word_count уже отражал объединённый текст → несоответствие.
            merged["token_count_est"] = round(len(group_text.split()) * 1.5)
            result.append(merged)
            i = j
        else:
            result.append(r)
            i += 1
    return result


def main():
    print(f"Режим подсчёта: {_COUNT_MODE}")
    print(f"MAX_TOKENS={MAX_TOKENS}, OVERLAP={OVERLAP_TOKENS}, MIN_TOKENS={MIN_TOKENS}\n")

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
        section_level = record.get("section_level", 0)
        block_stype   = record.get("section_type")
        doc_id        = record.get("document_id") or generate_doc_id(source)

        # [N] Доменные поля для фильтрации в Qdrant.
        # Читаем из записи prepare_texts (могут быть заданы вручную через
        # corpus_meta.json или выставлены upstream). Пустая строка — норма:
        # load_qdrant.py и rpd_generate.py обрабатывают пустые значения корректно.
        direction  = record.get("direction",  "")
        level      = record.get("level",      "")
        department = record.get("department", "")

        stats_source.setdefault(source, {
            "records": 0, "chunks": 0, "dups": 0, "by_stype": {}
        })
        stats_source[source]["records"] += 1

        if section_title and section_title.strip().lower() in NOISE_TITLES_LOWER:
            continue

        stype_for_limit = (
            block_stype if block_stype and block_stype != "other"
            else classify_section(section_title)
        )

        stype_count   = stats_source[source]["by_stype"].get(stype_for_limit, 0)
        if stype_count >= MAX_CHUNKS_PER_SECTION_TYPE:
            # [БАГ 9 ИСПРАВЛЕНО]: предупреждение при срабатывании лимита
            print(
                f"  ⚠️  [{source}] лимит {MAX_CHUNKS_PER_SECTION_TYPE} чанков "
                f"для типа '{stype_for_limit}' достигнут — блок пропущен: "
                f"{section_title!r:.60}"
            )
            continue

        doc_pos_start = stats_source[source]["chunks"]

        for idx, chunk in enumerate(smart_split(text, MAX_TOKENS, OVERLAP_TOKENS)):
            if count_tokens(chunk) < MIN_TOKENS:
                continue

            h = text_hash(chunk, source)
            if h in seen_hashes:
                dup_count += 1
                stats_source[source]["dups"] += 1
                continue
            seen_hashes.add(h)

            clean_chunk = filter_noise_lines(chunk)
            if count_tokens(clean_chunk) < MIN_TOKENS:
                continue

            # [M] Разделяем на section_metadata и chunk_metadata
            sec_meta, chunk_meta = build_metadata(
                clean_chunk, section_title, source, block_stype
            )

            chunks_out.append({
                "id":              global_chunk_id,
                "doc_id":          doc_id,
                "chunk_index":     idx,
                "doc_position":    doc_pos_start + idx,
                "source":          source,
                "section_title":   section_title,
                "section_level":   section_level,
                "text":            clean_chunk,
                # [N] Доменные поля — пробрасываем из записи prepare_texts,
                # чтобы load_qdrant.py мог записать их в Qdrant payload, а
                # rpd_generate.py — фильтровать по ним. Без этих полей в чанке
                # Qdrant всегда хранил "" и фильтр [B] никогда не срабатывал.
                "direction":       direction,
                "level":           level,
                "department":      department,
                # [M] Разделённые metadata
                "section_metadata": sec_meta,
                "chunk_metadata":   chunk_meta,
                # Обратная совместимость с load_qdrant.py
                "metadata": {**chunk_meta, "section_type": sec_meta["section_type"]},
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

    print(f"Создано уникальных чанков: {len(chunks_out)} (дублей: {dup_count})")

    print(f"\n{'Источник':<40} {'Зап.':>5} {'Чанков':>7} {'Дублей':>7}")
    print("-" * 62)
    for src, s in sorted(stats_source.items()):
        print(f"{src:<40} {s['records']:>5} {s['chunks']:>7} {s['dups']:>7}")

    type_counts = Counter(c["metadata"]["section_type"] for c in chunks_out)
    print(f"\nПо типу раздела:")
    for t, n in type_counts.most_common():
        print(f"  {t:<20}: {n}")

    if chunks_out:
        all_tcs = [c["chunk_metadata"]["token_count"] for c in chunks_out]
        print(f"\nСтатистика токенов ({_COUNT_MODE}):")
        print(f"  min={min(all_tcs)}, max={max(all_tcs)}, avg={sum(all_tcs)//len(all_tcs)}")


if __name__ == "__main__":
    main()
