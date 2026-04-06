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

Исправления v3.4:
  - [З-1] ИСПРАВЛЕНО: потеря 75% чанков competencies из-за MIN_TOKENS.
    Строки таблицы компетенций (22–33 слова, tc_est=33–50) обрабатывались
    по одной и большинство отсеивалось в smart_split фильтром MIN_TOKENS=40.
    Из 16 источников только 4 давали чанки competencies (у остальных все
    записи были ниже порога). Исправление: "competencies" добавлен в GROUPABLE
    в group_short_chunks(). После слияния каждый источник даёт один
    объединённый чанк ~100–130 слов, который уверенно проходит MIN_TOKENS.
    Безопасность: группировка ограничена одним (source, section_title),
    поэтому компетенции разных РПД не смешиваются.

  - [З-3] ИСПРАВЛЕНО: MAX_CHUNKS_PER_SECTION_TYPE=25 на пару (источник, тип)
    при корпусе из 16 источников давал cap=400 у типов hours/content/assessment
    и обрезал реальное содержание РПД. Константа теперь читается из config.json
    (ключ "max_chunks_per_section_type"). Дефолт повышен с 25 до 50 —
    при малом корпусе это снимает искусственный потолок. При отсутствии
    config.json поведение прежнее (используется константа).

Исправления v3.5:
  - [З-R4] ИСПРАВЛЕНО: 16 дублей в chunks.jsonl — одинаковые табличные чанки
    типов place/hours из разных РПД не дедуплицировались, так как хеш включал
    source. text_hash() теперь использует source="" для типов из SOURCELESS_TYPES
    (place, hours), схлопывая идентичные шаблонные тексты в один чанк."""

import json
import hashlib
import os
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

    # [З-4] tiktoken cl100k_base — это словарь GPT-4, НЕ bge-m3.
    # bge-m3 использует собственный мультиязычный BPE (BAAI/bge-m3).
    # Для русского текста расхождение ~10–25%: один фрагмент может получить
    # разные значения token_count здесь и при реальной токенизации bge-m3.
    # Это приемлемо как аппроксимация: при MAX_TOKENS=400 и среднем чанке
    # ~134 токена запас достаточен, чтобы ни один чанк не превысил лимит
    # bge-m3 (8192 токена). Точный подсчёт: AutoTokenizer("BAAI/bge-m3").
    _COUNT_MODE  = "токены (tiktoken cl100k_base, аппроксимация для bge-m3)"
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

# [З-3] Повышен с 25→50 ранее; [З-K3] ИСПРАВЛЕНО: поднят до 100 — при 16 документах
# в корпусе лимит 50 срабатывал на каждом файле для типа 'assessment' (35–49 чанков
# после group_short_chunks), отсекая строки ФОС и ПЗ. Переопределяется ключом
# max_chunks_per_section_type в config.json.
MAX_CHUNKS_PER_SECTION_TYPE = 100

# [FIX-TITLE] Лимит «заголовочных» чанков для типа assessment на пару (source, type).
# Заголовочный чанк — запись с section_level > 0 и коротким текстом (< MIN_TOKENS*2),
# т.е. по сути просто строка-заголовок раздела ФОС без содержания.
# При 16+ РПД в корпусе такие чанки давали 62% коллекции (assessment).
# Лимит 5 снижает долю до ~37%, не затрагивая содержательные чанки ФОС.
MAX_TITLE_CHUNKS = 5

NOISE_TITLES = {
    "УТВЕРЖДАЮ", "СОГЛАСОВАНО",
    "РАБОЧАЯ ПРОГРАММА ДИСЦИПЛИНЫ",
    "рабочая программа дисциплины",
}
# [БАГ 3 ИСПРАВЛЕНО]: нормализованный набор для регистронезависимого сравнения.
# "Рабочая программа дисциплины" (Title Case) не совпадало ни с UPPER ни с lower.
# [З-C1] ИСПРАВЛЕНО: "СВЕДЕНИЯ" удалено из NOISE_TITLES.
# Раньше section_title="СВЕДЕНИЯ" (заголовок раздела обеспеченности литературой)
# точно совпадал с "сведения" в NOISE_TITLES_LOWER → все чанки из этого раздела
# пропускались в chunking.py, даже после исправления SECTION_TYPE_MAP в converter.py.
# Удаление из NOISE_TITLES позволяет чанкам с section_type="bibliography" пройти
# в chunks.jsonl и далее в Qdrant.
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
    # [FIX-1б] "самостоятельн" убрано из assessment: заголовки вида
    # «Самостоятельная работа студента» некорректно попадали в assessment
    # вместо content/hours. Теперь assessment ловит только явные ФОС-заголовки.
    if re.search(r"фос|фонд оценочн|оценочн|аттестац|контрол|виды\s+сро", t): return "assessment"
    # [З-C1] ИСПРАВЛЕНО: добавлены ключевые слова для заголовка «СВЕДЕНИЯ об
    # обеспеченности дисциплины учебной литературой» — синхронизировано с
    # SECTION_TYPE_MAP в converter.py.
    if re.search(r"литература|библиограф|учебно.метод|учебной литератур|обеспеченност|^сведени", t):
        return "bibliography"
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
            # [FIX-1в] ("learning_outcomes", "assessment") убрана:
            # block_stype из конвертера надёжнее title_stype. Пара срабатывала
            # когда заголовок содержал "самостоятельн" (широкий regex) →
            # корректные LO-записи хранились как assessment, раздувая его долю.
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


def text_hash(text: str, source: str = "", stype: str = "") -> str:
    """
    [З-R4] Для структурно-шаблонных типов (place, hours) хеш считается
    без учёта source: одинаковый текст из разных РПД схлопывается в один
    чанк и не создаёт шум в retrieval.

    [B-3] ИСПРАВЛЕНО: bibliography добавлен в SOURCELESS_TYPES.
    Строки библиографии шаблонны: одна и та же запись «Асхаков, С. И.»
    присутствует во всех РПД кафедры, отличаясь только номером семестра
    («| 7 |» vs «| 8 |»). Перед хешированием номер семестра нормализуется
    (заменяется на «<SEM>»), что позволяет схлопывать идентичные книги
    из разных РПД в один чанк вместо создания 16 копий в Qdrant.
    """
    SOURCELESS_TYPES = {"place", "hours", "bibliography", "book_content"}
    effective_source = "" if stype in SOURCELESS_TYPES else source

    # [B-3] Для bibliography нормализуем поле семестра — убираем одиночные
    # числа в позиции разделителя «| N |» чтобы различие в номере семестра
    # не мешало дедупликации идентичных книг.
    norm_text = text.strip()
    if stype == "bibliography":
        norm_text = re.sub(r"\|\s*\d{1,2}\s*\|", "| <SEM> |", norm_text)

    return hashlib.sha256(
        f"{effective_source}\x00{norm_text}".encode("utf-8")
    ).hexdigest()


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
            # [З-K4] ИСПРАВЛЕНО: хвост ограничивается overlap токенами (не overlap*2
            # словами), чтобы текущий current не превысил max_tokens при следующем
            # добавлении нормального параграфа без flush().
            if chunks:
                tail_words = chunks[-1].split()
                # Набираем слова с конца до достижения overlap токенов
                tail_buf: list[str] = []
                tail_tc = 0
                for w in reversed(tail_words):
                    tail_buf.insert(0, w)
                    tail_tc += count_tokens(w)
                    if tail_tc >= overlap:
                        break
                tail_text = " ".join(tail_buf)
                if count_tokens(tail_text) >= MIN_TOKENS:
                    current      = [tail_text]
                    current_tcs  = [count_tokens(tail_text)]
                    current_size = current_tcs[0]
                else:
                    current      = []
                    current_tcs  = []
                    current_size = 0
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
    """
    Группировка коротких записей перед нарезкой на чанки.

    [З-1 ИСПРАВЛЕНО]: добавлен тип "competencies" в GROUPABLE.

    [D-1/B-1] ИСПРАВЛЕНО: добавлен тип "learning_outcomes" в GROUPABLE.
    После исправления З-C4 (заголовок таблицы только у первого чанка)
    строки learning_outcomes стали короче: типичная запись 21–30 слов,
    est=32–45 токенов — 69% записей падает ниже MIN_TOKENS=45 и
    отсеивается в smart_split. В отличие от competencies, строки LO несут
    разные индикаторы (З(УК-1), У(УК-1), В(УК-1)) — их объединение
    сохраняет структуру и одновременно гарантирует прохождение порога.
    Группировка ограничена (source, section_title) — строки разных РПД
    не смешиваются. max_group_tokens=200 даёт чанки ~4–6 индикаторов
    (~130–180 слов), что оптимально для retrieval «outcomes».
    """
    GROUPABLE = {"content", "assessment", "competencies", "learning_outcomes"}
    result = []
    i = 0
    while i < len(records):
        r = records[i]
        stype  = r.get("section_type")
        source = r.get("source") or ""   # [З-K2] пустой source не вызывает смешение
        tc = count_tokens(r["text"])
        if stype in GROUPABLE and tc < 90 and source:
            group_text = r["text"]
            group_tc   = tc
            j = i + 1
            while j < len(records):
                nxt = records[j]
                nxt_source = nxt.get("source") or ""
                if (nxt.get("section_type") == stype and
                        nxt_source == source and
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
            # [FIX] Используем count_tokens() вместо word_count * 1.5.
            # После слияния нескольких чанков оценка через * 1.5 давала
            # занижение ~15–25% для русского текста (реальное — ~1.7–1.9 т/слово).
            merged["token_count_est"] = count_tokens(group_text)
            result.append(merged)
            i = j
        else:
            result.append(r)
            i += 1
    return result


def main():
    print(f"Режим подсчёта: {_COUNT_MODE}")
    print(f"MAX_TOKENS={MAX_TOKENS}, OVERLAP={OVERLAP_TOKENS}, MIN_TOKENS={MIN_TOKENS}\n")

    # [З-3] Читаем лимит чанков на (источник, тип) из config.json.
    # Ключ: "max_chunks_per_section_type". Если не задан — используется константа.
    chunks_limit = MAX_CHUNKS_PER_SECTION_TYPE
    # [FIX-1а] Раздельные лимиты по типам через ключ "max_chunks_per_type".
    # Позволяет снизить assessment до 20 не затрагивая content/learning_outcomes.
    type_limits: dict = {}
    if os.path.exists("config.json"):
        try:
            with open("config.json", encoding="utf-8") as _cf:
                _cfg = json.load(_cf)
            _val = _cfg.get("max_chunks_per_section_type")
            if _val is not None:
                chunks_limit = int(_val)
                print(f"config.json: max_chunks_per_section_type={chunks_limit} (переопределено)")
            _tl = _cfg.get("max_chunks_per_type")
            if _tl and isinstance(_tl, dict):
                type_limits = {k: int(v) for k, v in _tl.items()}
                print(f"config.json: max_chunks_per_type={type_limits} (переопределено)")
        except Exception as _e:
            print(f"  ⚠️  config.json не прочитан для chunking: {_e}")
    print(f"Лимит чанков на (источник, тип): {chunks_limit}\n")

    with open(INPUT_FILE, encoding="utf-8") as f:
        raw_records = [json.loads(line) for line in f]

    records = group_short_chunks(raw_records)
    print(f"Записей после группировки: {len(records)} (было {len(raw_records)})")

    chunks_out:      list[dict] = []
    global_chunk_id: int        = 0
    seen_hashes:     set        = set()
    stats_source:    dict       = {}
    dup_count  = 0
    skip_counts: dict = {}  # [FIX-3] (source, stype) → кол-во пропущенных блоков

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
            "records": 0, "chunks": 0, "dups": 0, "by_stype": {}, "title_counts": {}
        })
        stats_source[source]["records"] += 1

        if section_title and section_title.strip().lower() in NOISE_TITLES_LOWER:
            continue

        stype_for_limit = (
            block_stype if block_stype and block_stype != "other"
            else classify_section(section_title)
        )

        stype_count   = stats_source[source]["by_stype"].get(stype_for_limit, 0)
        # [FIX-1а] Эффективный лимит: per-type из config.json или глобальный.
        effective_limit = type_limits.get(stype_for_limit, chunks_limit)
        if stype_count >= effective_limit:
            # [FIX-3] Агрегируем пропуски — выводим сводку в конце, не спамим
            skip_key = (source, stype_for_limit)
            skip_counts[skip_key] = skip_counts.get(skip_key, 0) + 1
            continue

        doc_pos_start = stats_source[source]["chunks"]

        for idx, chunk in enumerate(smart_split(text, MAX_TOKENS, OVERLAP_TOKENS)):
            if count_tokens(chunk) < MIN_TOKENS:
                continue

            h = text_hash(chunk, source, stype=stype_for_limit)
            if h in seen_hashes:
                dup_count += 1
                stats_source[source]["dups"] += 1
                continue
            seen_hashes.add(h)

            clean_chunk = filter_noise_lines(chunk)
            if count_tokens(clean_chunk) < MIN_TOKENS:
                continue

            # [FIX-TITLE] Ограничиваем заголовочные чанки assessment до MAX_TITLE_CHUNKS.
            # Заголовочный чанк: stype == assessment AND section_level > 0 AND
            # текст короткий (< MIN_TOKENS*2) — т.е. только строка-заголовок раздела ФОС.
            _is_title_chunk = (
                stype_for_limit == "assessment"
                and section_level > 0
                and count_tokens(clean_chunk) < MIN_TOKENS * 2
            )
            if _is_title_chunk:
                _title_cnt = stats_source[source]["title_counts"].get(stype_for_limit, 0)
                if _title_cnt >= MAX_TITLE_CHUNKS:
                    continue
                stats_source[source]["title_counts"][stype_for_limit] = _title_cnt + 1

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

            if stype_count >= effective_limit:
                break

    # [FIX-3] Сводка пропущенных блоков — вместо построчного спама
    if skip_counts:
        print("  Пропущено блоков по лимиту (source, тип → кол-во):")
        for (src, stp), cnt in sorted(skip_counts.items()):
            print(f"    [{src}] {stp}: {cnt}")

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
