"""
rpd_generate.py — генерация РПД на основе шаблона из rpd_corpus.

Стратегия: копируем шаблон → заменяем название дисциплины везде →
заполняем таблицы компетенций, результатов обучения, тем, ЛР, ПЗ
сгенерированным LLM-контентом. Форматирование УГНТУ сохраняется полностью.

Исправления v3:
  - [A] JSON-режим вывода LLM: промпты требуют строгий JSON-ответ.
    При JSONDecodeError — до 2 перегенераций, затем regex-fallback.
    Устраняет «тихие» ошибки формата (парсер молча возвращал дефолт).
  - [B] Доменная фильтрация retrieval: payload_filter расширен условиями
    must по полям direction/level из config.json. Предотвращает подмешивание
    чанков из чужих направлений подготовки при росте корпуса.
  - [C] generation_log.json: сохраняется рядом с output_rpd.docx.
    Содержит: промпт, retrieved chunks (id/source/score/preview),
    ответ LLM, метку времени — полная трассируемость генерации для аудита.
  - [D] validate_generation(): бизнес-валидация после заполнения таблиц —
    сумма часов, количество ЛР/ПЗ, наличие компетенций из config.
  - [R] Явный fallback при пустом retrieval: вместо молчаливой генерации
    без контекста (→ галлюцинации) выводится предупреждение и в промпт
    добавляется инструкция работать без примеров из корпуса.
  - [S] Фильтр retrieval переведён с "metadata.section_type" на "section_type"
    (верхний уровень payload) — согласовано с load_qdrant.py v3.
    Устраняет тихое несрабатывание фильтра при вложенном пути.
  - [K] Multi-query retrieval: каждая секция запрашивается несколькими
    формулировками, результаты дедуплицируются по id — расширяет семантический
    охват при коротких первичных запросах.
  - [L] num_ctx увеличен с 2048 до 4096 токенов: предотвращает обрезание
    промпта при большом retrieval-контексте.
  - [competencies_summary] добавлен в промпты content/lab_works/practice
    и в extra_vars первого прохода (пустая строка) — KeyError устранён.
  - TEMPLATE берётся из config.json.
  - Retry для Ollama, кэш embedding и retrieval.
  - Fallback Qdrant endpoint (query → search).
"""

import json
import re
import sys
import os
import shutil
import time
import requests
from copy import deepcopy
from typing import Optional
from docx import Document
from docx.oxml.ns import qn

OUTPUT_DOCX     = "output_rpd.docx"
GENERATION_LOG  = "generation_log.json"

QDRANT = {"url": "http://localhost:6333", "collection": "rpd_rag"}
OLLAMA = {
    "embed_url":    "http://localhost:11434/api/embeddings",
    "generate_url": "http://localhost:11434/api/generate",
    "embed_model":  "bge-m3",
    "llm_model":    "qwen2.5:3b",
}
GENERATION = {"top_k": 5, "min_score": 0.45}

# Фильтрация чанков по section_type для каждого генерируемого раздела.
SECTION_TYPE_FILTER = {
    "competencies": ["competencies", "learning_outcomes"],
    "outcomes":     ["competencies", "learning_outcomes"],
    "content":      ["content"],
    "lab_works":    ["content", "assessment"],
    "practice":     ["content", "assessment"],
}

EMBED_CACHE    = {}
RETRIEVE_CACHE = {}

# [K] Multi-query: несколько формулировок запроса на секцию.
# Расширяет семантический охват — разные формулировки дают разные чанки.
SECTION_QUERIES = {
    "competencies": [
        "{discipline} компетенции способен применять знания умения",
        "{discipline} УК ОПК ПК формируемые компетенции ФГОС",
    ],
    "outcomes": [
        "{discipline} результаты обучения знать уметь владеть индикаторы",
        "{discipline} learning outcomes знания навыки компетенции",
    ],
    "content": [
        "{discipline} содержание дисциплины разделы темы лекции учебный план",
        "{discipline} тематический план программа курса разделы",
    ],
    "lab_works": [
        "{discipline} лабораторные работы задания практика реализация алгоритма",
        "{discipline} практические задания эксперименты программирование",
    ],
    "practice": [
        "{discipline} практические занятия задачи Python анализ данных",
        "{discipline} семинары решение задач методы вычислительный эксперимент",
    ],
}

# Глобальный лог генерации — [C]
_generation_log: dict = {}


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


def _search_qdrant(vec: list, payload_filter: dict | None, top_k: int) -> list:
    """Поиск в Qdrant с fallback query → search."""
    try:
        body = {"query": vec, "limit": top_k, "with_payload": True}
        if payload_filter:
            body["filter"] = payload_filter
        r = requests.post(
            f"{QDRANT['url']}/collections/{QDRANT['collection']}/points/query",
            json=body, timeout=30)
        r.raise_for_status()
        return r.json().get("result", {}).get("points", [])
    except requests.HTTPError:
        body = {"vector": vec, "limit": top_k, "with_payload": True}
        if payload_filter:
            body["filter"] = payload_filter
        r = requests.post(
            f"{QDRANT['url']}/collections/{QDRANT['collection']}/points/search",
            json=body, timeout=30)
        r.raise_for_status()
        return r.json().get("result", [])


def retrieve(section: str, discipline: str, section_types: list = None,
             direction: str = "", level: str = "") -> tuple[str, list]:
    """
    Ищет релевантные чанки в Qdrant.

    [K] Multi-query: объединяем результаты по нескольким формулировкам.
    [B] Доменная фильтрация по direction/level.
    [S] Фильтр использует "section_type" (верхний уровень payload).
    [R] При пустом результате возвращает пустую строку с флагом для caller.

    Возвращает: (ctx_string, hits_list) для логирования [C].
    """
    cache_key = f"{section}|{discipline}|{','.join(section_types or [])}|{direction}|{level}"
    if cache_key in RETRIEVE_CACHE:
        return RETRIEVE_CACHE[cache_key]

    try:
        # [B] Строим фильтр с доменными полями
        must_conditions: list = []
        if section_types:
            must_conditions.append({
                "should": [
                    # [S] Используем "section_type" на верхнем уровне payload
                    {"key": "section_type", "match": {"value": st}}
                    for st in section_types
                ]
            })
        if direction:
            must_conditions.append({"key": "direction", "match": {"value": direction}})
        if level:
            must_conditions.append({"key": "level", "match": {"value": level}})

        payload_filter = {"must": must_conditions} if must_conditions else None

        # [K] Multi-query: собираем чанки по нескольким запросам
        queries = SECTION_QUERIES.get(section, [f"{discipline} {section}"])
        queries = [q.format(discipline=discipline) for q in queries]

        all_hits: dict[int, dict] = {}  # id → hit (дедупликация)
        for query_text in queries:
            vec = get_embedding(query_text)
            if not vec:
                continue
            hits = _search_qdrant(vec, payload_filter, GENERATION["top_k"])
            for h in hits:
                hit_id = h.get("id")
                if hit_id not in all_hits or h.get("score", 0) > all_hits[hit_id].get("score", 0):
                    all_hits[hit_id] = h

        # Фильтруем по score
        good_hits = sorted(
            [h for h in all_hits.values() if h.get("score", 0) >= GENERATION["min_score"]],
            key=lambda h: h.get("score", 0),
            reverse=True
        )[:GENERATION["top_k"]]

        # [R] Fallback при пустом retrieval — снижаем порог и убираем фильтр
        if not good_hits:
            print(f"    ⚠️  RAG [{section}]: нет чанков выше {GENERATION['min_score']}, "
                  f"пробую без доменного фильтра...")
            vec = get_embedding(queries[0])
            if vec:
                hits = _search_qdrant(vec, None, GENERATION["top_k"])
                good_hits = sorted(
                    [h for h in hits if h.get("score", 0) >= GENERATION["min_score"] * 0.7],
                    key=lambda h: h.get("score", 0), reverse=True
                )[:GENERATION["top_k"]]

        print(f"    🔍 RAG [{section}]: найдено {len(good_hits)} чанков "
              f"(scores: {[round(h.get('score', 0), 3) for h in good_hits]})")

        # Сборка контекста с метаданными источника
        seen_texts: set = set()
        parts: list[str] = []
        for h in good_hits:
            payload = h.get("payload", {})
            text    = payload.get("text", "")[:1200]
            if not text:
                continue
            dedup_key = text[:100]
            if dedup_key in seen_texts:
                continue
            seen_texts.add(dedup_key)
            source        = payload.get("source", "")
            section_title = payload.get("section_title", "")
            prefix = ""
            if source:
                prefix += f"[{source}]"
            if section_title:
                prefix += f" [{section_title}]"
            parts.append(f"{prefix}\n{text}" if prefix else text)

        ctx = "\n\n---\n\n".join(parts)
        RETRIEVE_CACHE[cache_key] = (ctx, good_hits)
        return ctx, good_hits

    except Exception as e:
        print(f"  ⚠️  RAG [{section}]: {e}")
        return "", []


def llm(prompt: str, max_tokens: int = 600) -> str:
    for attempt in range(3):
        try:
            r = requests.post(OLLAMA["generate_url"],
                json={
                    "model": OLLAMA["llm_model"],
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.3,
                        "num_predict": max_tokens,
                        "num_ctx": 4096,  # [L] увеличено с 2048
                    }
                },
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


def gen(label: str, discipline: str, prompt: str,
        direction: str = "", level: str = "", **extra) -> str:
    """
    Генерация секции с RAG-контекстом.

    [R] При пустом retrieval добавляет явную инструкцию в промпт.
    [C] Сохраняет данные в _generation_log для последующей записи в JSON.
    """
    section_types = SECTION_TYPE_FILTER.get(label)
    ctx, hits = retrieve(label, discipline, section_types, direction, level)

    if ctx:
        ctx_block = (
            "Примеры из базы РПД кафедры (используй как образец стиля и формата):\n"
            f"{ctx}\n\n"
        )
    else:
        # [R] Явное предупреждение и инструкция при отсутствии контекста
        print(f"  ⚠️  RAG [{label}]: контекст пуст — генерация без примеров из корпуса")
        ctx_block = (
            "Примеры из базы РПД недоступны. "
            "Сгенерируй содержимое самостоятельно строго по указанному формату "
            "без копирования примеров из промпта.\n\n"
        )

    fmt_vars = {"discipline": discipline, **extra}
    full_prompt = ctx_block + prompt.format(**fmt_vars) + f"\n\nСоздай для «{discipline}»:"
    result = llm(full_prompt)

    # [C] Логируем для generation_log.json
    _generation_log[label] = {
        "prompt_preview":   full_prompt[:600],
        "retrieved_chunks": [
            {
                "id":           h.get("id"),
                "source":       h.get("payload", {}).get("source", ""),
                "score":        round(h.get("score", 0), 4),
                "text_preview": h.get("payload", {}).get("text", "")[:120],
            }
            for h in hits
        ],
        "llm_response":     result,
        "timestamp":        time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    return result


# ---------------------------------------------------------------------------
# Работа с DOCX-шаблоном
# ---------------------------------------------------------------------------

def detect_old_discipline(doc: Document) -> str:
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
        if re.match(r"^\d{2}\.\d{2}\.\d{4}", text):
            continue
        if re.match(r"^[\d\.\s]+$", text):
            continue
        if para.style and ("heading" in para.style.name.lower()
                           or "заголовок" in para.style.name.lower()):
            clean_name = re.sub(r"^\(?\d+\)?\s*", "", text).strip()
            if len(clean_name) > 5:
                return clean_name
        is_bold = any(run.bold for run in para.runs if run.text.strip())
        has_lower = bool(re.search(r"[а-я]{4,}", text))
        if is_bold and has_lower and not text.endswith(":"):
            clean_name = re.sub(r"^\(?\d+\)?\s*", "", text).strip()
            if len(clean_name) > 5:
                return clean_name
    return ""


def replace_text_in_paragraph(para, old: str, new: str):
    if old not in para.text:
        return
    for run in para.runs:
        if old in run.text:
            run.text = run.text.replace(old, new)
            return
    full = para.text.replace(old, new)
    for run in para.runs:
        run.text = ""
    if para.runs:
        para.runs[0].text = full


def replace_all(doc: Document, old: str, new: str):
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


def clear_table_data_rows(table, start_row: int = 1):
    all_rows = list(table.rows)
    data_row_template = None
    if len(all_rows) > start_row:
        data_row_template = deepcopy(all_rows[start_row]._tr)
    rows_to_remove = all_rows[start_row:]
    for row in rows_to_remove:
        table._tbl.remove(row._tr)
    return data_row_template


def add_table_row(table, values: list, row_template=None):
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
# [A] JSON-парсеры с fallback на regex
# ---------------------------------------------------------------------------

def parse_competencies_json(text: str) -> list | None:
    """
    [A] Пытается разобрать JSON-ответ LLM для компетенций.
    Ожидаемый формат: [{"code": "УК-1", "desc": "Способен..."}]
    """
    m = re.search(r"\[.*?\]", text, re.S)
    if not m:
        return None
    try:
        data = json.loads(m.group())
        if not isinstance(data, list):
            return None
        result = [
            (str(d.get("code", "")), str(d.get("desc", "")))
            for d in data
            if isinstance(d, dict) and d.get("code") and d.get("desc")
        ]
        return result if result else None
    except (json.JSONDecodeError, TypeError):
        return None


def parse_competencies(text: str, codes: list = None) -> list:
    """
    [A] Парсит компетенции: JSON-режим → regex-fallback.
    """
    # Попытка JSON-разбора
    json_result = parse_competencies_json(text)
    if json_result:
        return json_result

    # Regex-fallback: нумерованные строки «Способен...»
    descriptions = []
    seen = set()
    for line in text.split("\n"):
        line = re.sub(r"^\d+[\.)]\s*", "", line.strip())
        line = re.sub(r"^[-–•]\s*", "", line)
        line = re.sub(r"\*\*", "", line).strip()
        if not line or len(line) < 10:
            continue
        if re.match(r"^Способен", line, re.I):
            key = line.lower()[:60]
            if key not in seen:
                seen.add(key)
                descriptions.append(line)

    if codes and descriptions:
        while len(descriptions) < len(codes):
            descriptions.append("Способен применять методы и инструменты дисциплины на практике")
        return list(zip(codes, descriptions[:len(codes)]))

    # Последний fallback: старый формат «УК-1: описание»
    result = []
    seen_codes: set = set()
    for line in text.split("\n"):
        m = re.match(r"(УК-\d+|ОПК-\d+|ПК-\d+)[:\.\s]+(.+)", line.strip())
        if m and m.group(1) not in seen_codes:
            seen_codes.add(m.group(1))
            result.append((m.group(1), m.group(2).strip()))
    return result if result else [
        ("УК-1",  "Способен применять системный подход для анализа и решения задач"),
        ("ОПК-1", "Способен разрабатывать алгоритмы и программы для интеллектуальных систем"),
        ("ПК-1",  "Способен применять методы машинного обучения для решения прикладных задач"),
    ]


def parse_outcomes_json(text: str) -> list | None:
    """
    [A] Пытается разобрать JSON-ответ LLM для результатов обучения.
    Ожидаемый формат: [{"type": "З", "text": "..."}, ...]
    """
    m = re.search(r"\[.*?\]", text, re.S)
    if not m:
        return None
    try:
        data = json.loads(m.group())
        if not isinstance(data, list):
            return None
        result = [
            (str(d.get("type", "")), str(d.get("text", "")))
            for d in data
            if isinstance(d, dict) and d.get("type") in ("З", "У", "В") and d.get("text")
        ]
        return result if len(result) >= 3 else None
    except (json.JSONDecodeError, TypeError):
        return None


def parse_outcomes(text: str) -> list:
    """
    [A] Парсит результаты обучения: JSON-режим → regex-fallback.
    """
    json_result = parse_outcomes_json(text)
    if json_result:
        return json_result

    # Regex-fallback: многострочный или однострочный формат Знать/Уметь/Владеть
    result = []
    current_type = None
    lines = []

    def flush():
        if current_type and lines:
            result.append((current_type, "\n".join(lines)))

    def split_inline(rest: str) -> list[str]:
        items = re.split(r";\s*-\s*|;\s*–\s*", rest)
        cleaned = []
        for item in items:
            item = re.sub(r"^[-–•]\s*", "", item.strip())
            item = re.sub(r"^\d+[\.)]\s*", "", item)
            item = re.sub(r"\*\*", "", item)
            if item and len(item) > 3:
                cleaned.append(item)
        return cleaned

    for line in text.split("\n"):
        line = line.strip()
        m_know = re.match(r"^Знать:\s*(.*)", line, re.I)
        m_can  = re.match(r"^Уметь:\s*(.*)", line, re.I)
        m_have = re.match(r"^Владеть:\s*(.*)", line, re.I)
        if m_know:
            flush(); current_type = "З"; lines = []
            rest = m_know.group(1).strip()
            if rest:
                lines.extend(split_inline(rest) if ";" in rest else [rest])
        elif m_can:
            flush(); current_type = "У"; lines = []
            rest = m_can.group(1).strip()
            if rest:
                lines.extend(split_inline(rest) if ";" in rest else [rest])
        elif m_have:
            flush(); current_type = "В"; lines = []
            rest = m_have.group(1).strip()
            if rest:
                lines.extend(split_inline(rest) if ";" in rest else [rest])
        elif line and current_type:
            item = re.sub(r"^\d+[\.)]\s*|\*\*|^[-–•]\s*", "", line)
            if item:
                lines.append(item)

    flush()

    VLADEET_PREFIXES = ("навыками", "методами", "инструментами", "технологиями",
                        "опытом", "практикой", "способностью")
    fixed = []
    for otype, otext in result:
        if otype == "В":
            fixed_lines = []
            for ln in otext.split("\n"):
                ln = ln.strip()
                if not ln:
                    continue
                ll = ln.lower()
                if not any(ll.startswith(p) for p in VLADEET_PREFIXES):
                    ln = re.sub(r"^(Основ[ыа]|Знание|Понимание|Базов[ые]+)\s+",
                                "навыками ", ln, flags=re.I)
                    ll = ln.lower()
                    if not any(ll.startswith(p) for p in VLADEET_PREFIXES):
                        ln = "навыками " + ln[0].lower() + ln[1:]
                fixed_lines.append(ln)
            fixed.append((otype, "\n".join(fixed_lines)))
        else:
            fixed.append((otype, otext))

    return fixed if fixed else [
        ("З", "Основные методы и алгоритмы интеллектуальных систем"),
        ("У", "Применять методы машинного обучения для решения задач"),
        ("В", "Навыками разработки и оценки интеллектуальных систем"),
    ]


def parse_topics_json(text: str) -> list | None:
    """
    [A] Пытается разобрать JSON-ответ LLM для тематического плана.
    Ожидаемый формат: [{"type": "section"|"topic", "label": "Раздел 1", "name": "..."}]
    """
    m = re.search(r"\[.*?\]", text, re.S)
    if not m:
        return None
    try:
        data = json.loads(m.group())
        if not isinstance(data, list):
            return None
        topics = []
        for d in data:
            if not isinstance(d, dict):
                continue
            label = str(d.get("label", "")).strip()
            name  = str(d.get("name", "")).strip()
            if label and name:
                topics.append(f"{label}. {name}")
        return topics if topics else None
    except (json.JSONDecodeError, TypeError):
        return None


def parse_topics(text: str) -> list:
    """[A] Парсит содержание дисциплины: JSON-режим → regex-fallback."""
    json_result = parse_topics_json(text)
    if json_result:
        return json_result

    # Regex-fallback
    topics = []
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    for para in paragraphs:
        tokens = re.split(
            r"(?=(?:Раздел|Тема)\s+\d+[\.\d]*\.?\s|\b\d+\.\d+\.?\s)", para
        )
        tokens = [t.strip() for t in tokens if t.strip()]
        for token in tokens:
            m_sec = re.match(r"^(Раздел\s+\d+)\.\s+(.+)", token)
            if m_sec:
                name = re.split(r"\s+\d+\.\d+\.", m_sec.group(2))[0].strip()
                if name:
                    topics.append(f"{m_sec.group(1)}. {name}")
                continue
            m_tema = re.match(r"^(Тема\s+[\d\.]+)\.\s+(.+)", token)
            if m_tema:
                name = re.split(r"\s+\d+\.\d+\.", m_tema.group(2))[0].strip()
                if name:
                    topics.append(f"{m_tema.group(1)}. {name}")
                continue
            m_sub = re.match(r"^(\d+\.\d+)\.?\s+(.+)", token)
            if m_sub:
                name = re.split(r"\s+\d+\.\d+\.", m_sub.group(2))[0].strip()
                if name:
                    topics.append(f"Тема {m_sub.group(1)}. {name}")

    if not topics:
        for line in text.split("\n"):
            m = re.match(r"^(Раздел|Тема)\s*([\d\.]+)[\.\ ]+(.+)", line.strip())
            if m:
                topics.append(
                    f"{m.group(1)} {m.group(2).rstrip('.')}. {m.group(3).strip()}"
                )

    return topics if topics else [
        "Раздел 1. Основы интеллектуальных систем",
        "Раздел 2. Методы машинного обучения",
        "Раздел 3. Применение интеллектуальных систем",
    ]


def parse_list_json(text: str) -> list | None:
    """
    [A] Пытается разобрать JSON-ответ LLM для списка ЛР/ПЗ.
    Ожидаемый формат: [{"title": "Реализация алгоритма..."}, ...]
    """
    m = re.search(r"\[.*?\]", text, re.S)
    if not m:
        return None
    try:
        data = json.loads(m.group())
        if not isinstance(data, list):
            return None
        result = [str(d.get("title", "")).strip() for d in data
                  if isinstance(d, dict) and d.get("title")]
        return result if len(result) >= 3 else None
    except (json.JSONDecodeError, TypeError):
        return None


def parse_list(text: str, discipline: str = "") -> list:
    """[A] Парсит список ЛР/ПЗ: JSON-режим → regex-fallback."""
    json_result = parse_list_json(text)
    if json_result:
        return json_result[:8]

    OFFTRACK_KEYWORDS = [
        "презентаци", "доклад", "реферат", "публикаци", "журнал",
        "flutter", "react native", "android studio", "xcode",
        "google play", "app store", "swift", "kotlin",
        "устный", "подготовка к",
    ]
    items = []
    for line in text.split("\n"):
        line = line.strip()
        line = re.sub(r"^(ЛР\s*№?\d+|ЛР\s*No\d+|\d+[\.):])\s+", "", line)
        line = re.sub(r"^\*\*(.+)\*\*$", r"\1", line)
        line = re.sub(r"^<[^>]{1,30}>\s*[-–\.\:]?\s*", "", line)
        line = re.sub(r"^<[^>]{1,30}>\s*$", "", line)
        if not line or len(line) < 6:
            continue
        if any(kw in line.lower() for kw in OFFTRACK_KEYWORDS):
            continue
        items.append(line)
    return items[:8] if items else ["Лабораторная работа 1", "Лабораторная работа 2"]


# ---------------------------------------------------------------------------
# Заполнение таблиц шаблона
# ---------------------------------------------------------------------------

def fill_competencies_table(doc: Document, competencies: list):
    table = doc.tables[4]
    tmpl  = clear_table_data_rows(table, start_row=1)
    for i, (code, desc) in enumerate(competencies, 1):
        add_table_row(table, [str(i), desc, code], tmpl)


def fill_outcomes_table(doc: Document, competencies: list, outcomes: list):
    table = doc.tables[5]
    tmpl  = clear_table_data_rows(table, start_row=1)

    type_map: dict = {}
    for ot, otext in outcomes:
        type_map[ot] = otext
    type_map.setdefault("З", "основные концепции и методы дисциплины")
    type_map.setdefault("У", "применять методы дисциплины для решения задач")
    type_map.setdefault("В", "навыками работы с инструментами дисциплины")

    def split_items(text: str) -> list:
        lines = []
        for ln in text.split("\n"):
            ln = re.sub(r"^\d+[\.)]\s*", "", ln.strip())
            if ln and len(ln) > 4:
                lines.append(ln)
        return lines if lines else [text.strip()]

    z_items = split_items(type_map["З"])
    u_items = split_items(type_map["У"])
    v_items = split_items(type_map["В"])
    type_prefix = {"З": "Знать:", "У": "Уметь:", "В": "Владеть:"}

    for idx, (code, desc) in enumerate(competencies):
        indicator = f"{code}.1 {desc[:100]}"
        for otype, items in [("З", z_items), ("У", u_items), ("В", v_items)]:
            result_code = f"{otype}({code})"
            rotated = items[idx % len(items):] + items[:idx % len(items)]
            result_text = f"{type_prefix[otype]} {rotated[0]}"
            if len(rotated) > 1:
                result_text += f"\n{rotated[1]}"
            add_table_row(table, [code, indicator, result_code, result_text], tmpl)


def fill_topics_table(doc: Document, topics: list, semester: str, hours: dict,
                      codes_list: list = None):
    table = doc.tables[7]
    tmpl  = clear_table_data_rows(table, start_row=2)

    sections_only = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    n = max(len(sections_only), 1) if sections_only else max(len(topics), 1)

    lec  = hours.get("lecture",  12) // n
    pz   = hours.get("practice", 36) // n
    lr   = hours.get("lab",      16) // n
    sro  = hours.get("self",     62) // n
    total_l = total_pz = total_lr = total_sro = 0

    codes = codes_list or ["ОПК-1", "ПК-1"]
    for i, sec in enumerate(sections_only, 1):
        sec_name = re.sub(r"^Раздел\s*\d+\.\s*", "", sec).strip()
        c1 = codes[(i - 1) % len(codes)]
        c2 = codes[i % len(codes)]
        shifer = f"З({c1})\nУ({c1})\nВ({c2})"
        add_table_row(table, [
            str(i), sec_name, semester,
            str(lec), str(pz), str(lr), str(sro), str(lec + pz + lr + sro),
            shifer
        ], tmpl)
        total_l += lec; total_pz += pz; total_lr += lr; total_sro += sro

    add_table_row(table, [
        "", "ИТОГО:", "",
        str(total_l), str(total_pz), str(total_lr), str(total_sro),
        str(total_l + total_pz + total_lr + total_sro), ""
    ], tmpl)


def fill_lectures_table(doc: Document, topics: list, hours: dict):
    table = doc.tables[8]
    tmpl  = clear_table_data_rows(table, start_row=2)

    themes_only   = [t for t in topics if not re.match(r"^Раздел\s*\d+", t)]
    sections_only = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]

    n_topics = max(len(themes_only), 1) if themes_only else max(len(sections_only), 1)
    lec = max(hours.get("lecture", 12) // n_topics, 1)

    section = ""
    lec_no  = 0
    for topic in topics:
        if re.match(r"^Раздел\s*\d+", topic):
            section = topic
        else:
            lec_no += 1
            short = re.sub(r"^Тема\s*[\d\.]+[\.\ ]+", "", topic).strip()
            add_table_row(table,
                [str(lec_no), section or topic, f"Лекция {lec_no}. {short}", str(lec), "", ""],
                tmpl)

    if lec_no == 0:
        lec = max(hours.get("lecture", 12) // max(len(sections_only), 1), 1)
        for i, topic in enumerate(sections_only, 1):
            short = re.sub(r"^Раздел\s*\d+[\.\ ]+", "", topic).strip()
            add_table_row(table,
                [str(i), topic, f"Лекция {i}. {short}", str(lec), "", ""],
                tmpl)


def fill_lab_table(doc: Document, lab_works: list, topics: list, hours_lab: int = 18):
    table = doc.tables[9]
    tmpl  = clear_table_data_rows(table, start_row=2)

    if len(lab_works) < 6:
        print(f"  ⚠️  Т9: получено {len(lab_works)} ЛР — дополняю до 6")
        for j in range(len(lab_works), 6):
            lab_works.append(f"Лабораторная работа {j + 1}")

    sections = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    hrs_each = max(hours_lab // len(lab_works), 1)

    for i, work in enumerate(lab_works, 1):
        section = sections[(i - 1) % max(len(sections), 1)] if sections else f"Раздел {((i - 1) // 2) + 1}"
        add_table_row(table, [section, str(i), work, str(hrs_each), "", ""], tmpl)
    add_table_row(table, ["-", "", "ИТОГО:", str(hrs_each * len(lab_works)), "", ""], tmpl)


def fill_practice_table(doc: Document, practices: list, topics: list,
                        hours_practice: int = 36):
    table = doc.tables[10]
    tmpl  = clear_table_data_rows(table, start_row=2)

    if len(practices) < 6:
        print(f"  ⚠️  Т10: получено {len(practices)} ПЗ — дополняю до 6")
        for j in range(len(practices), 6):
            practices.append(f"Практическое занятие {j + 1}")

    sections = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    hrs_each = max(hours_practice // len(practices), 1)

    for i, prac in enumerate(practices, 1):
        section = sections[(i - 1) % max(len(sections), 1)] if sections else f"Раздел {((i - 1) // 2) + 1}"
        add_table_row(table, [section, str(i), prac, str(hrs_each), "", ""], tmpl)
    add_table_row(table, ["-", "", "ИТОГО:", str(hrs_each * len(practices)), "", ""], tmpl)


def fill_t3_hours(doc: Document, semester: str, credits: int,
                  hours_total: int, hours_contact: int, hours_sro: int,
                  exam_type: str):
    t = doc.tables[3]
    if len(t.rows) < 5:
        return
    row = t.rows[4]
    vals = [semester, str(credits), str(hours_total), str(hours_contact),
            str(hours_sro), exam_type]
    for i, v in enumerate(vals):
        if i < len(row.cells):
            set_cell_text(row.cells[i], v)
    if len(t.rows) > 5:
        row5 = t.rows[5]
        for i, v in enumerate(["ИТОГО:", str(credits), str(hours_total),
                                str(hours_contact), str(hours_sro), ""]):
            if i < len(row5.cells):
                set_cell_text(row5.cells[i], v)


def fill_t6_workload(doc: Document, lec: int, pz: int, lr: int, sro: int,
                     semester: str):
    t = doc.tables[6]
    sem_col = None
    for j, cell in enumerate(t.rows[0].cells):
        if cell.text.strip() == semester:
            sem_col = j
            break
    kw_map = {
        "контактная":            lec + pz + lr,
        "лекции":                lec,
        "практические занятия":  pz,
        "лабораторные работы":   lr,
        "самостоятельная":       sro,
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
    t    = doc.tables[11]
    tmpl = clear_table_data_rows(t, start_row=2)
    sections = [tp for tp in topics if re.match(r"^Раздел\s*\d+", tp)]
    n = max(len(sections), 1)

    hrs_study = round(sro * 0.20)
    hrs_rgr   = round(sro * 0.20)
    hrs_prep  = sro - hrs_study - hrs_rgr

    sro_types = [
        ("подготовка к лабораторным и практическим занятиям", hrs_prep),
        ("изучение учебного материала, вынесенного на СРО",   hrs_study),
        ("выполнение расчётно-графической работы",            hrs_rgr),
    ]
    for sec in sections:
        for stype, total_hrs in sro_types:
            hrs_per_sec = round(total_hrs / n)
            add_table_row(t, [sec, stype, str(hrs_per_sec), "", ""], tmpl)
    add_table_row(t, ["-", "ИТОГО:", str(sro), "", ""], tmpl)


def fill_t21_fos(doc: Document, competencies: list, topics: list):
    t    = doc.tables[21]
    tmpl = clear_table_data_rows(t, start_row=1)
    sections = [tp for tp in topics if re.match(r"^Раздел\s*\d+", tp)]
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


# ---------------------------------------------------------------------------
# [D] Валидация бизнес-правил
# ---------------------------------------------------------------------------

def validate_generation(cfg: dict, hours: dict, competencies: list,
                        topics: list, lab_works: list, practices: list) -> list[str]:
    """
    [D] Проверяет корректность сгенерированного содержимого.
    Возвращает список предупреждений (пустой = всё ОК).
    """
    warnings: list[str] = []

    # Проверка суммы часов
    expected_total = cfg.get("hours", 144)
    actual_total   = sum(hours.values())
    if actual_total != expected_total:
        warnings.append(
            f"⚠️  Сумма часов {actual_total} ≠ {expected_total} из config.json"
        )

    # Наличие разделов
    sections = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    if not sections:
        warnings.append("⚠️  Разделы дисциплины не сгенерированы (topics пуст)")

    # Количество ЛР и ПЗ
    if len(lab_works) < 6:
        warnings.append(f"⚠️  ЛР: сгенерировано {len(lab_works)} < 6 минимальных")
    if len(practices) < 6:
        warnings.append(f"⚠️  ПЗ: сгенерировано {len(practices)} < 6 минимальных")

    # Соответствие компетенций конфигу
    codes_from_cfg = {c.strip() for c in cfg.get("competency_codes", "").split(",") if c.strip()}
    generated_codes = {code for code, _ in competencies}
    missing = codes_from_cfg - generated_codes
    if missing:
        warnings.append(f"⚠️  Компетенции не сгенерированы: {', '.join(sorted(missing))}")

    return warnings


# ---------------------------------------------------------------------------
# [A] Промпты — JSON-режим для всех генерируемых разделов
# ---------------------------------------------------------------------------

PROMPTS = {
    "competencies": """\
Сгенерируй {competency_count} описаний компетенций для дисциплины «{discipline}».
Коды: {competency_codes_numbered}
Требования: каждое описание начинается со слова «Способен», специфично для «{discipline}».
ВЕРНИ ТОЛЬКО JSON-массив (без пояснений, без markdown):
[
  {{"code": "УК-1", "desc": "Способен <уникальное действие 1>"}},
  {{"code": "ОПК-1", "desc": "Способен <уникальное действие 2>"}},
  ...
]
Ровно {competency_count} объектов.""",

    "outcomes": """\
Напиши результаты обучения для дисциплины «{discipline}» по ФГОС 3++.
ВЕРНИ ТОЛЬКО JSON-массив (без пояснений, без markdown):
[
  {{"type": "З", "text": "знание 1"}},
  {{"type": "З", "text": "знание 2"}},
  {{"type": "З", "text": "знание 3"}},
  {{"type": "У", "text": "умение 1"}},
  {{"type": "У", "text": "умение 2"}},
  {{"type": "У", "text": "умение 3"}},
  {{"type": "В", "text": "навыками <чего>"}},
  {{"type": "В", "text": "методами <чего>"}},
  {{"type": "В", "text": "инструментами <чего>"}}
]
Ровно 9 объектов (3 З, 3 У, 3 В).""",

    "content": """\
Напиши содержание дисциплины «{discipline}» — ровно 3 раздела, в каждом 2 темы.
Компетенции дисциплины: {competencies_summary}
ВЕРНИ ТОЛЬКО JSON-массив (без пояснений, без markdown):
[
  {{"type": "section", "label": "Раздел 1", "name": "<тематический блок 1>"}},
  {{"type": "topic",   "label": "Тема 1.1", "name": "<конкретная тема>"}},
  {{"type": "topic",   "label": "Тема 1.2", "name": "<конкретная тема>"}},
  {{"type": "section", "label": "Раздел 2", "name": "<тематический блок 2>"}},
  {{"type": "topic",   "label": "Тема 2.1", "name": "<конкретная тема>"}},
  {{"type": "topic",   "label": "Тема 2.2", "name": "<конкретная тема>"}},
  {{"type": "section", "label": "Раздел 3", "name": "<тематический блок 3>"}},
  {{"type": "topic",   "label": "Тема 3.1", "name": "<конкретная тема>"}},
  {{"type": "topic",   "label": "Тема 3.2", "name": "<конкретная тема>"}}
]
Ровно 9 объектов.""",

    "lab_works": """\
Напиши 6 лабораторных работ для дисциплины «{discipline}».
Компетенции, которые должны формироваться: {competencies_summary}
Требования:
- каждая ЛР — конкретное техническое задание (реализация алгоритма, обучение модели)
- все 6 ЛР на РАЗНЫЕ темы, нет повторений
- охватить: классификация, регрессия, кластеризация, нейросети, NLP/временные ряды, оптимизация
ВЕРНИ ТОЛЬКО JSON-массив (без пояснений, без markdown):
[
  {{"title": "Реализация алгоритма классификации методом SVM"}},
  {{"title": "..."}},
  ...
]
Ровно 6 объектов.""",

    "practice": """\
Напиши 6 тем практических занятий для дисциплины «{discipline}».
Компетенции, которые должны формироваться: {competencies_summary}
Требования:
- каждое занятие — решение конкретной задачи с Python-инструментами
- все 6 тем разные, чередовать: анализ данных, алгоритм, эксперимент с моделью
ВЕРНИ ТОЛЬКО JSON-массив (без пояснений, без markdown):
[
  {{"title": "Исследование данных и построение признакового пространства"}},
  {{"title": "..."}},
  ...
]
Ровно 6 объектов.""",
}


# ---------------------------------------------------------------------------
# [A] Обёртка генерации с JSON-retry
# ---------------------------------------------------------------------------

def gen_with_json_retry(label: str, discipline: str, prompt: str,
                        parser_json, parser_fallback, max_retries: int = 2,
                        direction: str = "", level: str = "", **extra):
    """
    [A] Генерирует секцию с JSON-валидацией и retry.

    1. Вызывает gen() → LLM-ответ
    2. Пробует parser_json — если успех, возвращает (raw_text, parsed)
    3. При неудаче: до max_retries перегенераций
    4. Если JSON так и не распарсился — regex-fallback через parser_fallback
    """
    raw = gen(label, discipline, prompt, direction=direction, level=level, **extra)
    result = parser_json(raw)
    if result is not None:
        return raw, result

    for attempt in range(max_retries):
        print(f"  🔄 [{label}] JSON не распарсился (попытка {attempt + 1}/{max_retries}), "
              f"перегенерация...")
        raw = gen(label, discipline, prompt, direction=direction, level=level, **extra)
        result = parser_json(raw)
        if result is not None:
            return raw, result

    print(f"  ⚠️  [{label}] JSON недоступен после {max_retries} попыток — regex-fallback")
    return raw, parser_fallback(raw)


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

def main(config_path: Optional[str] = None):
    if config_path is None and os.path.exists("config.json"):
        config_path = "config.json"

    if config_path:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        cfg = {}
    cfg.setdefault("discipline", "Интеллектуальные системы")

    discipline       = cfg["discipline"]
    semester         = str(cfg.get("semester", "7"))
    competency_codes = cfg.get("competency_codes", "УК-1, ОПК-1, ОПК-2, ПК-1, ПК-2")
    direction        = cfg.get("direction", "")
    level            = cfg.get("level", "бакалавриат")
    hours = {
        "lecture":  cfg.get("hours_lecture",  12),
        "practice": cfg.get("hours_practice", 36),
        "lab":      cfg.get("hours_lab",      16),
        "self":     cfg.get("hours_self",     62),
    }

    template = cfg.get("template", "")
    if not template or not os.path.exists(template):
        corpus_dir = "rpd_corpus"
        candidates = sorted(
            f for f in os.listdir(corpus_dir)
            if f.endswith(".docx") and not f.startswith("~$")
        ) if os.path.isdir(corpus_dir) else []
        template = os.path.join(corpus_dir, candidates[-1]) if candidates else ""

    print(f"\n{'=' * 60}")
    print(f"ГЕНЕРАЦИЯ РПД: {discipline}")
    print(f"Направление: {direction}  Уровень: {level}")
    print(f"{'=' * 60}\n")

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

    codes_list = [c.strip() for c in competency_codes.split(",") if c.strip()]
    competency_codes_numbered = "\n".join(f"{i + 1}. {c}" for i, c in enumerate(codes_list))

    # Базовые переменные (competencies_summary пустой для первого прохода)
    base_vars = {
        "competency_codes":          competency_codes,
        "competency_codes_numbered": competency_codes_numbered,
        "competency_count":          len(codes_list),
        "direction":                 direction,
        "level":                     level,
        "competencies_summary":      "",  # заполняется после парсинга компетенций
    }

    raw: dict = {}

    # --- Шаг 1: компетенции и результаты обучения ---
    raw["competencies"], competencies = gen_with_json_retry(
        "competencies", discipline, PROMPTS["competencies"],
        parser_json=lambda t: parse_competencies_json(t),
        parser_fallback=lambda t: parse_competencies(t, codes=codes_list),
        direction=direction, level=level, **base_vars
    )

    raw["outcomes"], outcomes = gen_with_json_retry(
        "outcomes", discipline, PROMPTS["outcomes"],
        parser_json=parse_outcomes_json,
        parser_fallback=parse_outcomes,
        direction=direction, level=level, **base_vars
    )

    # --- Шаг 2: обновляем competencies_summary и перегенерируем разделы ---
    comp_summary = "; ".join(f"{c[0]}: {c[1][:60]}" for c in competencies[:5])
    content_vars = {**base_vars, "competencies_summary": comp_summary}

    raw["content"], topics = gen_with_json_retry(
        "content", discipline, PROMPTS["content"],
        parser_json=parse_topics_json,
        parser_fallback=parse_topics,
        direction=direction, level=level, **content_vars
    )

    raw["lab_works"], lab_works = gen_with_json_retry(
        "lab_works", discipline, PROMPTS["lab_works"],
        parser_json=parse_list_json,
        parser_fallback=lambda t: parse_list(t, discipline),
        direction=direction, level=level, **content_vars
    )

    raw["practice"], practices = gen_with_json_retry(
        "practice", discipline, PROMPTS["practice"],
        parser_json=parse_list_json,
        parser_fallback=lambda t: parse_list(t, discipline),
        direction=direction, level=level, **content_vars
    )

    # --- [D] Валидация ---
    validation_warnings = validate_generation(
        cfg, hours, competencies, topics, lab_works, practices
    )
    if validation_warnings:
        print("\n🔎 Результаты валидации:")
        for w in validation_warnings:
            print(f"  {w}")
        _generation_log["validation_warnings"] = validation_warnings
    else:
        print("\n✅ Валидация пройдена — все бизнес-правила соблюдены")

    # --- Заполнение шаблона ---
    shutil.copy(template, OUTPUT_DOCX)
    doc = Document(OUTPUT_DOCX)

    old_name = cfg.get("old_discipline", "").strip() or detect_old_discipline(doc)
    old_code = cfg.get("old_code", "")
    new_code = cfg.get("new_code", "")

    if old_name:
        replace_all(doc, old_name, discipline)
    if old_code:
        replacement_code = f"({new_code})" if new_code else ""
        replace_all(doc, f"({old_code})", replacement_code)
        replace_all(doc, old_code, new_code if new_code else "")

    hours_contact = hours["lecture"] + hours["practice"] + hours["lab"]
    hours_sro     = hours["self"]
    hours_total   = hours_contact + hours_sro
    exam_type     = cfg.get("exam_type", "экзамен")

    for name, fn, args in [
        ("Т3 Трудоёмкость",        fill_t3_hours,          (doc, semester, cfg.get("credits", 4), hours_total, hours_contact, hours_sro, exam_type)),
        ("Т4 Компетенции",         fill_competencies_table, (doc, competencies)),
        ("Т5 Результаты обучения", fill_outcomes_table,     (doc, competencies, outcomes)),
        ("Т6 Виды работы",         fill_t6_workload,        (doc, hours["lecture"], hours["practice"], hours["lab"], hours["self"], semester)),
        ("Т7 Темы",                fill_topics_table,       (doc, topics, semester, hours, codes_list)),
        ("Т8 Лекции",              fill_lectures_table,     (doc, topics, hours)),
        ("Т9 ЛР",                  fill_lab_table,          (doc, lab_works, topics, hours["lab"])),
        ("Т10 ПЗ",                 fill_practice_table,     (doc, practices, topics, hours["practice"])),
        ("Т11 СРО",                fill_t11_sro,            (doc, topics, hours["self"])),
        ("Т21 ФОС",                fill_t21_fos,            (doc, competencies, topics)),
    ]:
        try:
            fn(*args)
            print(f"  ✅ {name}")
        except Exception as e:
            print(f"  ⚠️  {name}: {e}")

    doc.save(OUTPUT_DOCX)
    print(f"\n✅ Сохранено: {OUTPUT_DOCX}")

    # [C] Сохраняем лог генерации
    try:
        with open(GENERATION_LOG, "w", encoding="utf-8") as f:
            json.dump(_generation_log, f, ensure_ascii=False, indent=2)
        print(f"📋 Лог генерации: {GENERATION_LOG}")
    except Exception as e:
        print(f"  ⚠️  Не удалось сохранить лог: {e}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
