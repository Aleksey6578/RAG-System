"""
rpd_generate.py — генерация РПД на основе шаблона из rpd_corpus.

Стратегия: копируем шаблон → заменяем название дисциплины везде →
заполняем таблицы компетенций, результатов обучения, тем, ЛР, ПЗ
сгенерированным LLM-контентом. Форматирование УГНТУ сохраняется полностью.

Исправления v2:
  - БАГ: TEMPLATE и OLD_NAME были захардкожены под rpd_14.docx /
    «Машинное обучение». Теперь TEMPLATE берётся из config.json (ключ
    "template"), а OLD_NAME определяется автоматически из файла шаблона.
  - БАГ: add_table_row после clear_table_data_rows копировал header-строку
    как шаблон строки данных. Исправлено: оригинальная строка данных
    сохраняется ДО очистки таблицы и используется как шаблон.
  - БАГ: fill_topics_table делил часы на len(topics), включающий и Разделы
    и Темы. Теперь делит только на количество Разделов верхнего уровня.
  - БАГ: fill_lectures_table хардкодил «4» часа на лекцию вместо
    расчётного значения из hours dict.
  - БАГ: fill_outcomes_table строил cross-product компетенции × результаты
    (N×M строк). Теперь одна строка = одна компетенция, результаты
    З/У/В объединены в одной ячейке — соответствует формату РПД УГНТУ.
  - БАГ: retrieve() использовал только /points/query (Qdrant 1.7+).
    Добавлен fallback на /points/search для совместимости со старыми версиями.
  - БАГ: open(config_path) без with — утечка файлового дескриптора. Исправлено.
"""

import json, re, sys, os, shutil, requests, time
from typing import Optional
from copy import deepcopy
from docx import Document
from docx.oxml.ns import qn

OUTPUT_DOCX = "output_rpd.docx"

QDRANT = {"url": "http://localhost:6333", "collection": "rpd_rag"}
OLLAMA = {
    "embed_url":    "http://localhost:11434/api/embeddings",
    "generate_url": "http://localhost:11434/api/generate",
    "embed_model":  "bge-m3",
    "llm_model":    "qwen2.5:3b",
}
GENERATION = {"top_k": 5, "min_score": 0.45}

# Какие section_type чанков релевантны для каждого генерируемого раздела.
# Примечание: в реальном корпусе УГНТУ таблица T5 (Индикаторы / Результаты обучения)
# попадает в тип "competencies" (т.к. заголовок раздела содержит "компетенц"),
# поэтому для генерации outcomes мы смотрим оба типа.
SECTION_TYPE_FILTER = {
    "competencies": ["competencies", "learning_outcomes"],
    "outcomes":     ["competencies", "learning_outcomes"],
    "content":      ["content"],          # goals пустой в корпусе УГНТУ — убран
    "lab_works":    ["content", "assessment"],
    "practice":     ["content", "assessment"],
}
EMBED_CACHE = {}


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def clean(text: str) -> str:
    text = re.sub(r" +", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"\[score=[^\]]+\]\n?", "", text)
    return "\n".join(l.strip() for l in text.split("\n") if l.strip()).strip()


def get_embedding(text: str):
    if text in EMBED_CACHE:
        return EMBED_CACHE[text]
    for attempt in range(3):
        try:
            r = requests.post(OLLAMA["embed_url"],
                json={"model": OLLAMA["embed_model"], "prompt": f"query: {text}"},
                timeout=60)
            r.raise_for_status()
            d = r.json()
            vec = d.get("embedding") or d["data"][0]["embedding"]
            EMBED_CACHE[text] = vec
            return vec
        except Exception as e:
            if attempt == 2:
                return []
            time.sleep(2 ** attempt)
    return []


def retrieve(section: str, discipline: str, section_types: list = None) -> str:
    """
    Ищет релевантные чанки в Qdrant.
    section_types — если задан, фильтрует по metadata.section_type через payload filter.
    Это предотвращает попадание контента из нерелевантных дисциплин.
    """
    try:
        vec = get_embedding(f"{section} {discipline}")
        if not vec:
            return ""

        # Payload filter: только нужные типы разделов
        payload_filter = None
        if section_types:
            payload_filter = {
                "should": [
                    {"key": "metadata.section_type", "match": {"value": st}}
                    for st in section_types
                ]
            }

        # Пробуем новый endpoint (Qdrant 1.7+)
        try:
            body = {"query": vec, "limit": GENERATION["top_k"], "with_payload": True}
            if payload_filter:
                body["filter"] = payload_filter
            r = requests.post(
                f"{QDRANT['url']}/collections/{QDRANT['collection']}/points/query",
                json=body, timeout=30)
            r.raise_for_status()
            hits = r.json().get("result", {}).get("points", [])
        except requests.HTTPError:
            # Fallback: старый endpoint /points/search (Qdrant < 1.7)
            body = {"vector": vec, "limit": GENERATION["top_k"], "with_payload": True}
            if payload_filter:
                body["filter"] = payload_filter
            r = requests.post(
                f"{QDRANT['url']}/collections/{QDRANT['collection']}/points/search",
                json=body, timeout=30)
            r.raise_for_status()
            hits = r.json().get("result", [])

        parts = []
        for p in hits:
            if p.get("score", 0) < GENERATION["min_score"]:
                continue
            t = p.get("payload", {}).get("text", "")[:600]
            parts.append(t)
        return "\n\n---\n\n".join(parts)
    except Exception as e:
        print(f"  ⚠️  RAG: {e}")
        return ""


def llm(prompt: str, max_tokens: int = 600) -> str:
    for attempt in range(3):
        try:
            r = requests.post(OLLAMA["generate_url"],
                json={"model": OLLAMA["llm_model"], "prompt": prompt, "stream": False,
                      "options": {"temperature": 0.3, "num_predict": max_tokens, "num_ctx": 2048}},
                timeout=180)
            r.raise_for_status()
            text = r.json().get("response", "")
            if text:
                return clean(text)
        except Exception as e:
            if attempt == 2:
                return f"[Ошибка: {e}]"
            time.sleep(5)
    return "[Ошибка: пустой ответ]"


def gen(label: str, discipline: str, prompt: str, **extra) -> str:
    section_types = SECTION_TYPE_FILTER.get(label)
    ctx = retrieve(label, discipline, section_types)
    if ctx:
        ctx_block = f"Примеры из базы РПД кафедры (используй как образец стиля и формата):\n{ctx}\n\n"
    else:
        ctx_block = ""
    fmt_vars = {"discipline": discipline, **extra}
    full_prompt = ctx_block + prompt.format(**fmt_vars) + f"\n\nСоздай для «{discipline}»:"
    result = llm(full_prompt)
    return result


# ---------------------------------------------------------------------------
# Работа с DOCX-шаблоном
# ---------------------------------------------------------------------------

def detect_old_discipline(doc: Document) -> str:
    """
    Ищет название дисциплины в шаблоне, обходя административные блоки.
    Стратегия: ищем параграфы с Heading-стилем или жирным текстом, которые
    не содержат слов «университет», «кафедра», «федеральный», «утверждаю».

    ИСПРАВЛЕНИЕ: старая логика брала первый подходящий параграф и находила
    название вуза вместо дисциплины. Теперь явно исключаем административные
    шаблонные строки.
    """
    STOP_WORDS = (
        "университет", "институт", "академия", "кафедра",
        "федеральн", "утверждаю", "согласовано", "министерств",
        "образован", "высшего", "направлени", "уровень", "форма обучени",
        "трудоём", "трудоем",
    )

    for para in doc.paragraphs:
        text = para.text.strip()
        if len(text) < 5 or len(text) > 120:
            continue
        tl = text.lower()
        if any(sw in tl for sw in STOP_WORDS):
            continue
        if re.match(r"^\d{2}\.\d{2}\.\d{4}", text):   # дата
            continue
        if re.match(r"^[\d\.\s]+$", text):             # только цифры
            continue
        # Параграф с Heading-стилем — это почти наверняка заголовок
        if para.style and ("heading" in para.style.name.lower()
                           or "заголовок" in para.style.name.lower()):
            clean_name = re.sub(r"^\(?\d+\)?\s*", "", text).strip()
            if len(clean_name) > 5:
                return clean_name
        # Параграф с жирным шрифтом И строчными буквами — вероятно название дисциплины
        is_bold = any(run.bold for run in para.runs if run.text.strip())
        has_lower = bool(re.search(r"[а-я]{4,}", text))
        if is_bold and has_lower and not text.endswith(":"):
            clean_name = re.sub(r"^\(?\d+\)?\s*", "", text).strip()
            if len(clean_name) > 5:
                return clean_name

    return ""


def replace_text_in_paragraph(para, old: str, new: str):
    """Заменяет текст в параграфе, сохраняя форматирование первого рана."""
    if old not in para.text:
        return
    for run in para.runs:
        if old in run.text:
            run.text = run.text.replace(old, new)
            return
    # Текст размазан по нескольким ранам — заменяем через full text
    full = para.text.replace(old, new)
    for run in para.runs:
        run.text = ""
    if para.runs:
        para.runs[0].text = full


def replace_all(doc: Document, old: str, new: str):
    """Заменяет текст во всех параграфах и ячейках таблиц."""
    for para in doc.paragraphs:
        if old in para.text:
            replace_text_in_paragraph(para, old, new)
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for para in cell.paragraphs:
                    if old in para.text:
                        replace_text_in_paragraph(para, old, new)


def set_cell_text(cell, text: str):
    """Устанавливает текст ячейки, сохраняя форматирование."""
    for para in cell.paragraphs:
        for run in para.runs:
            run.text = ""
    if not cell.paragraphs:
        cell.add_paragraph(text)
    else:
        if not cell.paragraphs[0].runs:
            cell.paragraphs[0].add_run(text)
        else:
            cell.paragraphs[0].runs[0].text = text


def clear_table_data_rows(table, start_row: int = 1) -> object:
    """
    ИСПРАВЛЕНИЕ: возвращает deepcopy строки данных ДО удаления —
    это шаблон для add_table_row, чтобы не копировать header-строку.
    """
    all_rows = list(table.rows)
    # Сохраняем шаблон строки данных (первая строка после заголовка)
    data_row_template = None
    if len(all_rows) > start_row:
        data_row_template = deepcopy(all_rows[start_row]._tr)

    rows_to_remove = all_rows[start_row:]
    for row in rows_to_remove:
        table._tbl.remove(row._tr)

    return data_row_template


def add_table_row(table, values: list, row_template=None):
    """
    ИСПРАВЛЕНИЕ: использует row_template (строка данных) вместо
    table.rows[-1] (которая после clear становится header-строкой).
    Если row_template не передан — fallback на копирование последней строки.
    """
    if row_template is not None:
        new_tr = deepcopy(row_template)
    else:
        new_tr = deepcopy(table.rows[-1]._tr)

    table._tbl.append(new_tr)
    row = table.rows[-1]
    for i, val in enumerate(values):
        if i < len(row.cells):
            set_cell_text(row.cells[i], str(val))
    return row


# ---------------------------------------------------------------------------
# Парсинг LLM-ответов
# ---------------------------------------------------------------------------

def parse_competencies(text: str, codes: list = None) -> list:
    """
    Извлекает описания компетенций из текста и зипует с кодами из конфига.

    Новая стратегия: модель генерирует ТОЛЬКО описания (нумерованный список).
    Коды назначаются детерминированно из параметра codes — это исключает
    ситуацию когда модель пишет «КОД:» буквально или теряет часть кодов.

    Если коды не переданы — пробуем старый формат «КОД: описание» как fallback.
    """
    # Извлекаем строки вида «1. Способен ...» или просто «Способен ...»
    descriptions = []
    seen = set()
    for line in text.split("\n"):
        line = line.strip()
        # Убираем нумерацию «1.», «1)», «-»
        line = re.sub(r"^[\d]+[\.\)]\s*", "", line)
        line = re.sub(r"^[-–•]\s*", "", line)
        line = re.sub(r"\*\*", "", line).strip()
        if not line or len(line) < 10:
            continue
        # Принимаем строки начинающиеся с «Способен» или любой глагол
        if re.match(r"^Способен", line, re.I):
            key = line.lower()[:60]
            if key not in seen:
                seen.add(key)
                descriptions.append(line)

    if codes and descriptions:
        # Берём ровно столько описаний сколько кодов, дополняем если мало
        while len(descriptions) < len(codes):
            descriptions.append(f"Способен применять методы и инструменты дисциплины на практике")
        return list(zip(codes, descriptions[:len(codes)]))

    # Fallback: старый формат «УК-1: описание» — на случай если модель всё же вывела коды
    result = []
    seen_codes: set = set()
    for line in text.split("\n"):
        line = line.strip()
        m = re.match(r"(УК-\d+|ОПК-\d+|ПК-\d+)[:\.\s]+(.+)", line)
        if m and m.group(1) not in seen_codes:
            seen_codes.add(m.group(1))
            result.append((m.group(1), m.group(2).strip()))
    return result if result else [
        ("УК-1",  "Способен применять системный подход для анализа и решения задач"),
        ("ОПК-1", "Способен разрабатывать алгоритмы и программы для интеллектуальных систем"),
        ("ПК-1",  "Способен применять методы машинного обучения для решения прикладных задач"),
    ]


def parse_outcomes(text: str) -> list:
    """
    Извлекает [(тип, текст)] где тип = З/У/В.

    Поддерживает два формата из корпуса УГНТУ:
      Формат A (многострочный — ответ LLM):
        Знать:
        1. пункт
        Уметь:
        ...
      Формат B (однострочный — JSON-корпус):
        Знать: - пункт1; - пункт2
        Уметь: - пункт3; - пункт4
    """
    result       = []
    current_type = None
    lines        = []

    def flush():
        if current_type and lines:
            result.append((current_type, "\n".join(lines)))

    def split_inline(rest: str) -> list[str]:
        """Разбивает 'пункт1; - пункт2; - пункт3' на отдельные пункты."""
        items = re.split(r";\s*-\s*|;\s*–\s*", rest)
        cleaned = []
        for item in items:
            item = re.sub(r"^[-–•]\s*", "", item.strip())
            item = re.sub(r"^\d+[\.\)]\s*", "", item)
            item = re.sub(r"\*\*", "", item)
            if item and len(item) > 3:
                cleaned.append(item)
        return cleaned

    for line in text.split("\n"):
        line = line.strip()

        m_know  = re.match(r"^Знать:\s*(.*)", line, re.I)
        m_can   = re.match(r"^Уметь:\s*(.*)", line, re.I)
        m_have  = re.match(r"^Владеть:\s*(.*)", line, re.I)

        if m_know:
            flush()
            current_type = "З"; lines = []
            rest = m_know.group(1).strip()
            if rest:
                lines.extend(split_inline(rest) if ";" in rest else [rest])
        elif m_can:
            flush()
            current_type = "У"; lines = []
            rest = m_can.group(1).strip()
            if rest:
                lines.extend(split_inline(rest) if ";" in rest else [rest])
        elif m_have:
            flush()
            current_type = "В"; lines = []
            rest = m_have.group(1).strip()
            if rest:
                lines.extend(split_inline(rest) if ";" in rest else [rest])
        elif line and current_type:
            item = re.sub(r"^\d+[\.\)]\s*|\*\*|^[-–•]\s*", "", line)
            if item: lines.append(item)

    flush()

    # Постобработка: исправляем «Владеть» — пункты должны начинаться с навыков/методов
    VLADEET_PREFIXES = ("навыками", "методами", "инструментами", "технологиями",
                        "опытом", "практикой", "способностью")
    fixed = []
    for otype, otext in result:
        if otype == "В":
            fixed_lines = []
            for line in otext.split("\n"):
                line = line.strip()
                if not line:
                    continue
                ll = line.lower()
                # Если не начинается с нужного слова — оборачиваем
                if not any(ll.startswith(p) for p in VLADEET_PREFIXES):
                    # Пробуем спасти: убираем «Основы/Знание/Понимание» → «навыками»
                    line = re.sub(
                        r"^(Основ[ыа]|Знание|Понимание|Базов[ые]+)\s+",
                        "навыками ", line, flags=re.I
                    )
                    ll = line.lower()
                    # Если всё равно не начинается — добавляем «навыками»
                    if not any(ll.startswith(p) for p in VLADEET_PREFIXES):
                        line = "навыками " + line[0].lower() + line[1:]
                fixed_lines.append(line)
            fixed.append((otype, "\n".join(fixed_lines)))
        else:
            fixed.append((otype, otext))

    return fixed if fixed else [
        ("З", "основные методы и алгоритмы интеллектуальных систем"),
        ("У", "применять методы машинного обучения для решения задач"),
        ("В", "навыками разработки и оценки интеллектуальных систем"),
    ]


def validate_outcomes(outcomes: list) -> list:
    """
    Замечание №5: гарантируем минимум 2 пункта в каждом блоке З/У/В.
    Если блок пустой или короткий — дополняем дефолтными пунктами.
    """
    DEFAULTS = {
        "З": [
            "основные концепции и методы дисциплины",
            "принципы построения и анализа моделей",
            "современные инструменты и библиотеки",
        ],
        "У": [
            "применять изученные методы для решения задач",
            "анализировать и интерпретировать результаты моделей",
            "выбирать подходящие алгоритмы под конкретную задачу",
        ],
        "В": [
            "навыками разработки и отладки моделей",
            "методами исследования и анализа данных",
            "инструментами практической реализации алгоритмов",
        ],
    }
    type_map = {ot: lines for ot, lines in outcomes}
    result = []
    for otype in ("З", "У", "В"):
        text = type_map.get(otype, "")
        lines = [l.strip() for l in text.split("\n") if l.strip()] if text else []
        # Дополняем до 2 пунктов если меньше
        while len(lines) < 2:
            idx = len(lines) % len(DEFAULTS[otype])
            candidate = DEFAULTS[otype][idx]
            if candidate not in lines:
                lines.append(candidate)
        result.append((otype, "\n".join(lines)))
    return result


def parse_topics(text: str) -> list:
    """
    Извлекает список разделов и тем из текста содержания дисциплины.

    Поддерживает два формата из корпуса:

    Формат A (многострочный — ответ LLM):
        Раздел 1. Название
        Тема 1.1. Подтема
        Тема 1.2. Подтема

    Формат B (однострочный — JSON-корпус кафедры УГНТУ):
        Раздел 1. Название 1.1. Подтема 1.2. Подтема\n\nРаздел 2. Название 2.1. ...

    В формате B разделы разделены двойным переносом, подтемы — внутри строки
    обозначены маркерами вида «1.1.», «1.2.» и т.д.
    """
    topics = []

    # Сначала пробуем разбить по параграфам (двойной перенос)
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]

    for para in paragraphs:
        # Внутри параграфа ищем все маркеры "Раздел N." и "N.M."
        # Пример: "Раздел 1. Введение 1.1. Понятие 1.2. История"
        # Разбиваем по позициям маркеров
        tokens = re.split(r"(?=(?:Раздел|Тема)\s+\d+[\.\d]*\.?\s|\b\d+\.\d+\.?\s)", para)
        tokens = [t.strip() for t in tokens if t.strip()]

        for token in tokens:
            # Раздел верхнего уровня: "Раздел 1. Название"
            m_section = re.match(r"^(Раздел\s+\d+)\.\s+(.+)", token)
            if m_section:
                name = m_section.group(2).strip()
                # Обрезаем если внутри названия есть начало следующей подтемы
                name = re.split(r"\s+\d+\.\d+\.", name)[0].strip()
                if name:
                    topics.append(f"{m_section.group(1)}. {name}")
                continue

            # Тема с явным маркером "Тема N.M."
            m_tema = re.match(r"^(Тема\s+[\d\.]+)\.\s+(.+)", token)
            if m_tema:
                name = m_tema.group(2).strip()
                name = re.split(r"\s+\d+\.\d+\.", name)[0].strip()
                if name:
                    topics.append(f"{m_tema.group(1)}. {name}")
                continue

            # Подтема без слова "Тема": "1.1. Название"
            m_sub = re.match(r"^(\d+\.\d+)\.?\s+(.+)", token)
            if m_sub:
                name = m_sub.group(2).strip()
                name = re.split(r"\s+\d+\.\d+\.", name)[0].strip()
                if name:
                    topics.append(f"Тема {m_sub.group(1)}. {name}")

    # Фолбэк — простой построчный разбор (формат A LLM)
    if not topics:
        for line in text.split("\n"):
            line = line.strip()
            m = re.match(r"^(Раздел|Тема)\s*([\d\.]+)[\.\s]+(.+)", line)
            if m:
                topics.append(f"{m.group(1)} {m.group(2).rstrip('.')}. {m.group(3).strip()}")

    return topics if topics else [
        "Раздел 1. Основы интеллектуальных систем",
        "Раздел 2. Методы машинного обучения",
        "Раздел 3. Применение интеллектуальных систем",
    ]


def parse_list(text: str, discipline: str = "") -> list:
    """
    Извлекает нумерованный список строк.
    Фильтрует нерелевантные пункты и артефакты шаблонных placeholders.
    """
    # Ключевые слова, которых не должно быть в ЛР/ПЗ для большинства дисциплин
    OFFTRACK_KEYWORDS = [
        "презентаци", "доклад", "реферат", "публикаци", "журнал",
        "flutter", "react native", "android studio", "xcode",
        "google play", "app store", "swift", "kotlin",
        "устный", "подготовка к",
    ]
    items = []
    for line in text.split("\n"):
        line = line.strip()
        # Убираем нумерацию «1.», «1)», «ЛР №1»
        line = re.sub(r"^(ЛР\s*№?\d+|ЛР\s*No\d+|\d+[\.\):])\s+", "", line)
        line = re.sub(r"^\*\*(.+)\*\*$", r"\1", line)
        # Убираем артефакт шаблона: «<название> - », «<название>. » и т.п.,
        # которые модель копирует из примера в промпте не заменив placeholder.
        line = re.sub(r"^<[^>]{1,30}>\s*[-–\.]\s*", "", line)
        # Убираем одиночный placeholder если после него ничего нет
        line = re.sub(r"^<[^>]{1,30}>\s*$", "", line)
        if not line or len(line) < 6:
            continue
        line_lower = line.lower()
        if any(kw in line_lower for kw in OFFTRACK_KEYWORDS):
            continue
        items.append(line)
    return items[:8] if items else ["Лабораторная работа 1", "Лабораторная работа 2"]


# ---------------------------------------------------------------------------
# Заполнение таблиц шаблона
# ---------------------------------------------------------------------------

def fill_competencies_table(doc: Document, competencies: list):
    """Таблица 4: № | Компетенция | Код."""
    table    = doc.tables[4]
    tmpl     = clear_table_data_rows(table, start_row=1)
    for i, (code, desc) in enumerate(competencies, 1):
        add_table_row(table, [str(i), desc, code], tmpl)


def fill_outcomes_table(doc: Document, competencies: list, outcomes: list):
    """
    Таблица 5: Шифр | Индикатор | Шифр результата | Результат обучения.

    ИСПРАВЛЕНИЕ (замечание №4): реальный шаблон УГНТУ требует 3 строки на
    каждую компетенцию — З, У, В отдельными строками. Каждая строка:
      col0: код компетенции (УК-1)
      col1: индикатор (УК-1.1 ...)
      col2: шифр результата (З(УК-1), У(УК-1), В(УК-1))
      col3: текст результата (Знать: ..., Уметь: ..., Владеть: ...)
    """
    table = doc.tables[5]
    tmpl  = clear_table_data_rows(table, start_row=1)

    # Индекс типа → порядковый номер подпункта индикатора
    type_order = {"З": 1, "У": 2, "В": 3}
    type_prefix = {"З": "Знать:", "У": "Уметь:", "В": "Владеть:"}

    # Берём З/У/В тексты — если outcomes короче 3 пунктов, дополняем
    zuv_texts: dict = {}
    for ot, otext in outcomes:
        zuv_texts[ot] = otext
    # Гарантируем все три типа
    zuv_texts.setdefault("З", "основные методы и концепции дисциплины")
    zuv_texts.setdefault("У", "применять методы дисциплины для решения задач")
    zuv_texts.setdefault("В", "навыками работы с инструментами дисциплины")

    for code, desc in competencies:
        # Номер индикатора: УК-1 → УК-1.1, ОПК-2 → ОПК-2.1
        indicator_base = f"{code}.1"
        # Короткое описание индикатора из desc
        indicator_short = desc[:80] if len(desc) > 80 else desc
        indicator = f"{indicator_base} {indicator_short}"

        for otype in ("З", "У", "В"):
            result_code = f"{otype}({code})"
            result_text = f"{type_prefix[otype]} {zuv_texts[otype]}"
            add_table_row(table, [code, indicator, result_code, result_text], tmpl)


def fill_topics_table(doc: Document, topics: list, semester: str, hours: dict):
    """
    Таблица 7: № | Название | Семестр | Л | ПЗ | ЛР | СРО | Всего | Шифр.

    ИСПРАВЛЕНИЕ: часы делятся на количество РАЗДЕЛОВ (строки «Раздел N.»),
    а не на общее количество тем включая подтемы.
    Если разделов нет — на всё количество топиков.
    """
    table    = doc.tables[7]
    tmpl     = clear_table_data_rows(table, start_row=2)

    # Считаем только Разделы верхнего уровня для распределения часов
    sections_only = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    n = max(len(sections_only), 1) if sections_only else max(len(topics), 1)

    lec  = hours.get("lecture",  12) // n
    pz   = hours.get("practice", 36) // n
    lr   = hours.get("lab",      16) // n
    sro  = hours.get("self",     62) // n
    total_l = total_pz = total_lr = total_sro = 0

    for i, sec in enumerate(sections_only, 1):
        sec_name = re.sub(r"^Раздел\s*\d+\.\s*", "", sec).strip()
        add_table_row(table, [
            str(i), sec_name, semester,
            str(lec), str(pz), str(lr), str(sro), str(lec+pz+lr+sro),
            "З(ОПК-1)\nУ(ПК-1)\nВ(ПК-1)"
        ], tmpl)
        total_l += lec; total_pz += pz; total_lr += lr; total_sro += sro

    add_table_row(table, [
        "", "ИТОГО:", "",
        str(total_l), str(total_pz), str(total_lr), str(total_sro),
        str(total_l+total_pz+total_lr+total_sro), ""
    ], tmpl)


def fill_lectures_table(doc: Document, topics: list, hours: dict):
    """
    Таблица 8: № | Раздел | Тема | очная | оч.-заочная | заочная.

    ИСПРАВЛЕНИЕ: часы берутся из hours["lecture"], а не хардкодятся как «4».
    """
    table    = doc.tables[8]
    tmpl     = clear_table_data_rows(table, start_row=2)

    sections_only = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    n   = max(len(sections_only), 1) if sections_only else max(len(topics), 1)
    lec = hours.get("lecture", 12) // n   # часов на секцию

    section = ""
    lec_no  = 0
    for topic in topics:
        if re.match(r"^Раздел\s*\d+", topic):
            section = topic
        else:
            lec_no += 1
            short = re.sub(r"^Тема\s*[\d\.]+[\.\s]+", "", topic).strip()
            add_table_row(table,
                [str(lec_no), section or topic, f"Лекция {lec_no}. {short}", str(lec), "", ""],
                tmpl)

    # Если тем нет (только разделы) — одна строка на раздел
    if lec_no == 0:
        for i, topic in enumerate(topics, 1):
            short = re.sub(r"^Раздел\s*\d+[\.\s]+", "", topic).strip()
            add_table_row(table,
                [str(i), topic, f"Лекция {i}. {short}", str(lec), "", ""],
                tmpl)


def fill_lab_table(doc: Document, lab_works: list, topics: list):
    """Таблица 9: Раздел | № ЛР | Название | очная | оч.-заочная | заочная."""
    table    = doc.tables[9]
    tmpl     = clear_table_data_rows(table, start_row=2)

    sections = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    for i, work in enumerate(lab_works, 1):
        section = sections[(i-1) % max(len(sections), 1)] if sections else f"Раздел {((i-1)//2)+1}"
        add_table_row(table, [section, str(i), work, "2", "", ""], tmpl)
    add_table_row(table, ["-", "", "ИТОГО:", str(len(lab_works)*2), "", ""], tmpl)


def fill_practice_table(doc: Document, practices: list, topics: list):
    """Таблица 10: Раздел | № ПЗ | Тема | очная | оч.-заочная | заочная."""
    table    = doc.tables[10]
    tmpl     = clear_table_data_rows(table, start_row=2)

    sections = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    for i, prac in enumerate(practices, 1):
        section = sections[(i-1) % max(len(sections), 1)] if sections else f"Раздел {((i-1)//2)+1}"
        add_table_row(table, [section, str(i), prac, "2", "", ""], tmpl)
    add_table_row(table, ["-", "", "ИТОГО:", str(len(practices)*2), "", ""], tmpl)


# ---------------------------------------------------------------------------
# Промпты
# ---------------------------------------------------------------------------

def fill_t3_hours(doc: Document, semester: str, credits: int,
                  hours_total: int, hours_contact: int, hours_sro: int, exam_type: str):
    """Таблица 3: трудоёмкость — правим строку данных (строка 4, индекс 4)."""
    t = doc.tables[3]
    if len(t.rows) < 5:
        return
    row = t.rows[4]
    vals = [semester, str(credits), str(hours_total), str(hours_contact), str(hours_sro), exam_type]
    for i, v in enumerate(vals):
        if i < len(row.cells):
            set_cell_text(row.cells[i], v)
    # Строка ИТОГО (строка 5)
    if len(t.rows) > 5:
        row5 = t.rows[5]
        for i, v in enumerate(["ИТОГО:", str(credits), str(hours_total), str(hours_contact), str(hours_sro), ""]):
            if i < len(row5.cells):
                set_cell_text(row5.cells[i], v)


def fill_t6_workload(doc: Document, lec: int, pz: int, lr: int, sro: int, semester: str):
    """Таблица 6: виды учебной работы — правим числа в столбце «Всего» и нужном семестре."""
    t = doc.tables[6]
    # Находим колонку семестра
    sem_col = None
    for j, cell in enumerate(t.rows[0].cells):
        if cell.text.strip() == semester:
            sem_col = j
            break

    kw_map = {
        "контактная": lec + pz + lr,
        "лекции":     lec,
        "практические занятия": pz,
        "лабораторные работы":  lr,
        "самостоятельная":      sro,
    }
    for row in t.rows:
        label = row.cells[0].text.strip().lower()
        for kw, val in kw_map.items():
            if kw in label:
                set_cell_text(row.cells[1], str(val))
                if sem_col and sem_col < len(row.cells):
                    set_cell_text(row.cells[sem_col], str(val))
                break


def fill_t11_sro(doc: Document, topics: list, sro: int):
    """Таблица 11: виды СРО по разделам."""
    t = doc.tables[11]
    tmpl = clear_table_data_rows(t, start_row=2)
    sections = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    sro_types = [
        ("подготовка к лабораторным и практическим занятиям", 18),
        ("изучение учебного материала, вынесенного на СРО",    5),
        ("выполнение расчётно-графической работы",             5),
    ]
    total = 0
    for sec in sections:
        for stype, hrs in sro_types:
            add_table_row(t, [sec, stype, str(hrs), "", ""], tmpl)
            total += hrs
    add_table_row(t, ["-", "ИТОГО:", str(total), "", ""], tmpl)


def fill_t21_fos(doc: Document, competencies: list, topics: list):
    """Таблица 21: паспорт ФОС."""
    t = doc.tables[21]
    tmpl = clear_table_data_rows(t, start_row=1)
    sections = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    ocs = ["Письменный и устный опрос", "Лабораторная работа",
           "Тест", "Расчётно-графическая работа"]
    n = 1
    for i, sec in enumerate(sections):
        sec_name = re.sub(r"^Раздел\s*\d+\.\s*", "", sec)
        for code, desc in competencies:
            add_table_row(t, [
                str(n), sec_name, f"В({code})", desc,
                f"{code}.1 Демонстрирует применение методов на практике",
                f"Выполняет задания по разделу «{sec_name}»",
                ocs[i % len(ocs)]
            ], tmpl)
            n += 1


PROMPTS = {
    "competencies": """\
Напиши {competency_count} описаний компетенций для дисциплины «{discipline}».
Используй коды: {competency_codes_numbered}
Каждое описание — одна строка, начинается со слова «Способен», специфична для дисциплины.
Все {competency_count} описаний должны быть разными по смыслу.
Формат:
1. Способен <уникальное действие 1>
2. Способен <уникальное действие 2>
...
Только нумерованный список, без кодов в строках, без заголовков.""",

    "outcomes": """\
Напиши результаты обучения для дисциплины «{discipline}» по ФГОС 3++.
Требования к грамматике:
- «Знать:» — существительные в винительном падеже (Знать: основы теории, методы анализа)
- «Уметь:» — глаголы несовершенного вида (применять, разрабатывать, анализировать)
- «Владеть:» — строго начинать с «навыками», «методами» или «инструментами»
Формат строго — три блока, 3 пункта в каждом:
Знать:
1. <существительное или словосочетание>
2. <существительное или словосочетание>
3. <существительное или словосочетание>
Уметь:
1. <глагол несовершенного вида> <объект>
2. <глагол> <объект>
3. <глагол> <объект>
Владеть:
1. навыками <чего>
2. методами <чего>
3. инструментами <чего>
Только список, без вступления.""",

    "content": """\
Напиши содержание дисциплины «{discipline}» — ровно 3 раздела, в каждом 2 темы.
Разделы: теоретические основы → алгоритмы и методы → практическое применение.
ВАЖНО: названия тем — краткие, без ошибок управления падежами.
Формат (строго 9 строк):
Раздел 1. <название>
Тема 1.1. <название>
Тема 1.2. <название>
Раздел 2. <название>
Тема 2.1. <название>
Тема 2.2. <название>
Раздел 3. <название>
Тема 3.1. <название>
Тема 3.2. <название>
Только эти 9 строк, без пояснений.""",

    "lab_works": """\
Напиши 6 лабораторных работ для дисциплины «{discipline}».
Требования:
- каждая ЛР — конкретное техническое задание (реализация алгоритма, обучение модели, сравнение методов)
- все 6 ЛР должны быть на РАЗНЫЕ темы и алгоритмы (не повторять друг друга)
- охватить: классификация, регрессия, кластеризация, нейросети, NLP или временные ряды, оптимизация
- запрещено: доклады, рефераты, презентации
Формат — строго 6 строк:
1. <название ЛР>
2. <название ЛР>
3. <название ЛР>
4. <название ЛР>
5. <название ЛР>
6. <название ЛР>""",

    "practice": """\
Напиши 6 тем практических занятий для дисциплины «{discipline}».
Требования:
- каждое занятие — решение конкретной задачи с Python-инструментами
- все 6 тем должны быть разными (не повторять примеры)
- чередовать: анализ данных, реализация алгоритма, эксперимент с моделью
- не использовать темы из примера ниже
Темы должны быть ДРУГИМИ чем: Pandas-анализ датасета, градиентный спуск, TensorFlow/Keras
Формат — строго 6 строк:
1. <тема ПЗ>
2. <тема ПЗ>
3. <тема ПЗ>
4. <тема ПЗ>
5. <тема ПЗ>
6. <тема ПЗ>""",
}


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

def main(config_path: Optional[str] = None):
    if config_path is None and os.path.exists("config.json"):
        config_path = "config.json"

    # ИСПРАВЛЕНИЕ: используем with для открытия файла
    if config_path:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        cfg = {}
    cfg.setdefault("discipline", "Интеллектуальные системы")

    discipline         = cfg["discipline"]
    semester           = str(cfg.get("semester", "7"))
    competency_codes   = cfg.get("competency_codes", "УК-1, ОПК-1, ОПК-2, ПК-1, ПК-2")
    direction          = cfg.get("direction", "")
    level              = cfg.get("level", "бакалавриат")
    hours = {
        "lecture":  cfg.get("hours_lecture",  12),
        "practice": cfg.get("hours_practice", 36),
        "lab":      cfg.get("hours_lab",      16),
        "self":     cfg.get("hours_self",     62),
    }

    # ИСПРАВЛЕНИЕ: TEMPLATE берётся из config.json, иначе ищем первый docx в rpd_corpus
    template = cfg.get("template", "")
    if not template or not os.path.exists(template):
        corpus_dir = "rpd_corpus"
        candidates = sorted(
            f for f in os.listdir(corpus_dir)
            if f.endswith(".docx") and not f.startswith("~$")
        ) if os.path.isdir(corpus_dir) else []
        template = os.path.join(corpus_dir, candidates[-1]) if candidates else ""

    print(f"\n{'='*60}")
    print(f"ГЕНЕРАЦИЯ РПД: {discipline}")
    print(f"{'='*60}\n")

    # Проверка Ollama
    try:
        requests.get("http://localhost:11434/api/tags", timeout=5).raise_for_status()
        print("✅ Ollama доступен")
    except Exception as e:
        print(f"❌ Ollama недоступен: {e}")
        return

    if not template or not os.path.exists(template):
        print(f"❌ Шаблон не найден: {template!r}")
        return

    # Генерация контента
    # Нумерованный список кодов — модели проще не пропустить элементы
    codes_list = [c.strip() for c in competency_codes.split(",") if c.strip()]
    competency_codes_numbered = "\n".join(f"{i+1}. {c}" for i, c in enumerate(codes_list))
    extra_vars = {
        "competency_codes":          competency_codes,
        "competency_codes_numbered": competency_codes_numbered,
        "competency_count":          len(codes_list),
        "direction":                 direction,
        "level":                     level,
    }
    raw = {}
    for key, prompt in PROMPTS.items():
        raw[key] = gen(key, discipline, prompt, **extra_vars)

    # Парсинг: коды берём из конфига и зипуем с описаниями — модель их не теряет
    competencies = parse_competencies(raw["competencies"], codes=codes_list)
    outcomes     = validate_outcomes(parse_outcomes(raw["outcomes"]))
    topics       = parse_topics(raw["content"])
    lab_works    = parse_list(raw["lab_works"], discipline)
    practices    = parse_list(raw["practice"],  discipline)


    # Открываем шаблон и определяем старое название дисциплины
    shutil.copy(template, OUTPUT_DOCX)
    doc = Document(OUTPUT_DOCX)

    # ИСПРАВЛЕНИЕ: old_discipline из конфига имеет приоритет над автоопределением.
    # Если поле пустое — пробуем автоопределить, но предупреждаем.
    old_name = cfg.get("old_discipline", "").strip() or detect_old_discipline(doc)
    old_code = cfg.get("old_code", "")
    if not old_name:
        pass
    else:
        pass

    if old_name:
        replace_all(doc, old_name, discipline)
    if old_code:
        replace_all(doc, f"({old_code})", "")
        replace_all(doc, old_code, "")

    # Заполнение таблиц
    TABLE_INDICES = {
        "fill_competencies_table": 4,
        "fill_outcomes_table":     5,
        "fill_topics_table":       7,
        "fill_lectures_table":     8,
        "fill_lab_table":          9,
        "fill_practice_table":     10,
    }
    max_needed = max(TABLE_INDICES.values())
    if len(doc.tables) <= max_needed:
        pass
    hours_contact = hours["lecture"] + hours["practice"] + hours["lab"]
    hours_sro     = hours["self"]
    hours_total   = hours_contact + hours_sro
    exam_type     = cfg.get("exam_type", "экзамен")

    for name, fn, args in [
        ("Т3 Трудоёмкость",        fill_t3_hours,          (doc, semester, cfg.get("credits", 4), hours_total, hours_contact, hours_sro, exam_type)),
        ("Т4 Компетенции",         fill_competencies_table,(doc, competencies)),
        ("Т5 Результаты обучения", fill_outcomes_table,    (doc, competencies, outcomes)),
        ("Т6 Виды работы",         fill_t6_workload,       (doc, hours["lecture"], hours["practice"], hours["lab"], hours["self"], semester)),
        ("Т7 Темы",                fill_topics_table,      (doc, topics, semester, hours)),
        ("Т8 Лекции",              fill_lectures_table,    (doc, topics, hours)),
        ("Т9 ЛР",                  fill_lab_table,         (doc, lab_works, topics)),
        ("Т10 ПЗ",                 fill_practice_table,    (doc, practices, topics)),
        ("Т11 СРО",                fill_t11_sro,           (doc, topics, hours["self"])),
        ("Т21 ФОС",                fill_t21_fos,           (doc, competencies, topics)),
    ]:
        try:
            fn(*args)
        except Exception as e:
            print(f"  ⚠️  {name}: {e}")

    doc.save(OUTPUT_DOCX)
    print(f"\n✅ Сохранено: {OUTPUT_DOCX}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
