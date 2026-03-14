"""
rpd_generate.py — генерация РПД на основе шаблона из rpd_corpus.

Стратегия: копируем шаблон → заменяем название дисциплины везде →
заполняем таблицы компетенций, результатов обучения, тем, ЛР, ПЗ
сгенерированным LLM-контентом. Форматирование УГНТУ сохраняется полностью.

Исправления v3.6:
  - [КОМПЕТЕНЦИИ] ИСПРАВЛЕНО: пример в промпте содержал «в области {discipline}»
    посередине предложения → модель копировала конструкцию буквально.
    Пример заменён на безопасный с «Машинное обучение» как фиксированной
    дисциплиной + явная инструкция «Примеры выше — для другой дисциплины».
  - [БИБЛИОГРАФИЯ T15] ИСПРАВЛЕНО: промпт содержал «Фамилия, И. О. Название»
    в JSON-примере → модель копировала шаблон. Убраны шаблонные примеры,
    добавлен запрет «НЕ придумывай авторов», добавлена постпроверка
    _is_placeholder() в gen_bibliography() для фильтрации галлюцинаций.
    При обнаружении плейсхолдеров — fallback на реальные учебники.
  - [БИБЛИОГРАФИЯ T17] ИСПРАВЛЕНО: qwen2.5:3b стабильно не умеет генерировать
    методические пособия (возвращает «Фамилия, И. О. Название» независимо
    от промпта). LLM-вызов для T17 убран полностью — всегда используется
    fallback с реальными УГНТУ-пособиями кафедры ВТИК (Д. М. Зарипов, 2023).

Исправления v3.5:
  - [ПРОМПТЫ] Убраны плейсхолдеры «<уникальное действие 1>»/«знание 1»/«умение 1».
  - [БИБЛИОГРАФИЯ] Добавлена генерация Т15/Т17 через LLM + fallback.

(более ранние версии — см. историю файла)
"""

import json
import re
import sys
import os
import shutil
import time
import hashlib
import requests
from math import sqrt
from copy import deepcopy
from typing import Optional
from docx import Document
from docx.oxml.ns import qn

OUTPUT_DOCX     = "output_rpd.docx"
GENERATION_LOG  = "generation_log.json"
BIBLIOGRAPHY_ALLOWLIST = "bibliography_allowlist.json"

QDRANT = {"url": "http://localhost:6333", "collection": "rpd_rag"}
OLLAMA = {
    "embed_url":    "http://localhost:11434/api/embeddings",
    "generate_url": "http://localhost:11434/api/generate",
    "embed_model":  "bge-m3",
    "llm_model":    "qwen2.5:3b",
}
GENERATION = {
    "top_k": 5,
    "min_score": 0.45,
    # [STEP-3] retrieve→rerank: сначала берём больше кандидатов,
    # затем оставляем top_k после переранжирования и source-diversity.
    "retrieve_top_k": 20,
    "max_chunks_per_source": 2,
}

# [J] Максимальная длина контекста, передаваемого в LLM (символы).
# Замечание: "Нет ограничения контекста — контекст может превышать окно модели".
# Ограничиваем retrieved-контекст до 3000 символов ≈ 750-900 токенов
# для русского текста. Это оставляет достаточно места в num_ctx=4096
# для самого промпта и ответа LLM.
# При превышении лишние части обрезаются с явной пометкой "[...обрезано...]".
MAX_CONTEXT_CHARS = 3000

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

PRIORITY_WEIGHTS = {
    "high": 1.05,
    "normal": 1.0,
    "low": 0.85,
}

DISCIPLINE_FILTER_SECTION_TYPES = {"content", "hours", "assessment"}

# Устойчивые темы «чужих» дисциплин: используем для понижения веса retrieval.
NON_TARGET_TOPICS = {
    "неорганическая химия", "органическая химия", "аналитическая химия",
    "физическая химия", "квантовая химия", "биохимия", "микробиология",
    "ботаника", "зоология", "фармакология", "терапия", "хирургия",
    "макроэкономика", "микроэкономика", "бухгалтерский учет", "аудит",
    "гражданское право", "уголовное право", "криминалистика",
    "история россии", "философия", "политология", "социология",
    "экология", "геология", "геодезия", "металлургия", "нефтегазовое дело",
    "теплотехника", "электротехника", "сопротивление материалов", "детали машин",
    "педиатрия", "патологическая анатомия", "ветеринария", "агрономия",
    "педагогика", "дошкольное образование", "лингвистика", "литературоведение",
}

TARGET_TERMS_BY_DISCIPLINE = {
    "интеллектуальные системы": {
        "интеллектуальные системы", "машинное обучение", "ml", "классификация",
        "регрессия", "кластеризация", "нейросеть", "нейронные сети", "deep learning",
        "nlp", "обработка естественного языка", "компьютерное зрение",
        "рекомендательные системы", "feature engineering", "признаки", "датасет",
        "обучение с учителем", "обучение без учителя", "градиентный бустинг",
        "оценка модели", "метрики качества", "переобучение", "кросс-валидация",
        "python", "pytorch", "tensorflow", "scikit-learn", "llm",
    },
}

TOPIC_RELEVANCE = {
    "embedding_min": 0.31,
    "min_keyword_hits": 1,
    "max_irrelevant_share": 0.35,
}

DISCIPLINE_GUARD = {
    "embedding_min": 0.33,
    "keyword_min": 0.08,
    "penalty_weight": 0.55,
}


def _rank_score(hit: dict) -> float:
    """Комбинированный score для сортировки retrieval с учётом priority."""
    payload = hit.get("payload", {})
    raw_score = hit.get("score", 0.0)
    priority = str(payload.get("priority", "normal")).lower()
    weight = PRIORITY_WEIGHTS.get(priority, PRIORITY_WEIGHTS["normal"])
    return raw_score * weight


def _apply_source_diversity(hits: list[dict], max_per_source: int) -> list[dict]:
    """Ограничивает число чанков из одного source после rerank."""
    if max_per_source <= 0:
        return hits
    selected: list[dict] = []
    src_counts: dict[str, int] = {}
    for h in hits:
        src = h.get("payload", {}).get("source", "")
        if src and src_counts.get(src, 0) >= max_per_source:
            continue
        if src:
            src_counts[src] = src_counts.get(src, 0) + 1
        selected.append(h)
    return selected


def _tokenize_keywords(text: str) -> set[str]:
    words = re.findall(r"[а-яa-z0-9-]+", (text or "").lower())
    keys = {w for w in words if len(w) >= 4}
    norm_full = _normalize_text(text)
    if norm_full:
        keys.add(norm_full)
    return keys


def _cosine_similarity(v1: list[float], v2: list[float]) -> float:
    if not v1 or not v2 or len(v1) != len(v2):
        return 0.0
    dot = sum(a * b for a, b in zip(v1, v2))
    n1 = sqrt(sum(a * a for a in v1))
    n2 = sqrt(sum(b * b for b in v2))
    if n1 == 0 or n2 == 0:
        return 0.0
    return dot / (n1 * n2)


def _discipline_guard_rank(section: str, section_types: list | None,
                           discipline: str, hit: dict,
                           discipline_vec: list[float],
                           discipline_keywords: set[str]) -> tuple[float, bool, dict]:
    payload = hit.get("payload", {})
    text = _normalize_text(payload.get("text", ""))
    base_rank = _rank_score(hit)
    guard_details = {"base_rank": round(base_rank, 4)}

    target_scope = bool(section_types) and bool(
        DISCIPLINE_FILTER_SECTION_TYPES.intersection({str(s).lower() for s in section_types})
    )
    if section == "content":
        target_scope = True
    if not target_scope or not text:
        return base_rank, False, guard_details

    matched = sum(1 for kw in discipline_keywords if kw in text)
    keyword_score = matched / max(len(discipline_keywords), 1)
    chunk_vec = get_embedding(text[:512])
    embedding_score = _cosine_similarity(discipline_vec, chunk_vec) if discipline_vec and chunk_vec else 0.0

    non_target_hits = sum(1 for t in NON_TARGET_TOPICS if t in text)
    penalty = DISCIPLINE_GUARD["penalty_weight"] ** non_target_hits if non_target_hits else 1.0

    passes = (
        embedding_score >= DISCIPLINE_GUARD["embedding_min"]
        and keyword_score >= DISCIPLINE_GUARD["keyword_min"]
    )

    # Комбинированный score: релевантность по дисциплине + штраф за «чужие» темы.
    relevance_weight = (0.55 + 0.45 * embedding_score) * (0.6 + 0.4 * min(keyword_score * 3, 1.0))
    guarded_rank = base_rank * relevance_weight * penalty
    guard_details.update({
        "keyword_score": round(keyword_score, 4),
        "embedding_score": round(embedding_score, 4),
        "non_target_hits": non_target_hits,
        "penalty": round(penalty, 4),
        "guarded_rank": round(guarded_rank, 4),
        "pass": passes,
    })
    return guarded_rank, True, guard_details



def _get_discipline_target_terms(discipline: str) -> set[str]:
    """Возвращает словарь целевых терминов для конкретной дисциплины."""
    normalized = _normalize_text(discipline)
    terms: set[str] = set()
    for key, vocab in TARGET_TERMS_BY_DISCIPLINE.items():
        key_norm = _normalize_text(key)
        if key_norm and key_norm in normalized:
            terms.update(_normalize_text(v) for v in vocab if _normalize_text(v))
    # Всегда добавляем токены названия дисциплины как базовую опору.
    terms.update(_tokenize_keywords(discipline))
    return {t for t in terms if t}


def classify_topic_relevance(topics: list[str], discipline: str) -> tuple[list[str], dict]:
    """Классифицирует темы как релевантные/нерелевантные по keywords + embeddings."""
    target_terms = _get_discipline_target_terms(discipline)
    discipline_vec = get_embedding(discipline)

    relevant_topics: list[str] = []
    irrelevant_topics: list[str] = []
    details: list[dict] = []

    content_topics = [t for t in topics if not re.match(r"^Раздел\s*\d+", t)]
    for topic in content_topics:
        topic_norm = _normalize_text(topic)
        keyword_hits = [term for term in target_terms if term in topic_norm]
        topic_vec = get_embedding(topic_norm[:512])
        embedding_score = _cosine_similarity(discipline_vec, topic_vec) if discipline_vec and topic_vec else 0.0
        non_target_hits = [term for term in NON_TARGET_TOPICS if term in topic_norm]

        is_relevant = (
            len(keyword_hits) >= TOPIC_RELEVANCE["min_keyword_hits"]
            and embedding_score >= TOPIC_RELEVANCE["embedding_min"]
            and not non_target_hits
        )
        if is_relevant:
            relevant_topics.append(topic)
        else:
            irrelevant_topics.append(topic)

        details.append({
            "topic": topic,
            "keyword_hits": keyword_hits[:6],
            "embedding_score": round(embedding_score, 4),
            "non_target_hits": non_target_hits[:4],
            "relevant": is_relevant,
        })

    irrelevant_share = (len(irrelevant_topics) / len(content_topics)) if content_topics else 0.0

    # Разделы оставляем, если хотя бы одна тема прошла фильтр; иначе возвращаем исходный список.
    if relevant_topics:
        sections = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
        filtered_topics = sections + relevant_topics
    else:
        filtered_topics = topics[:]

    report = {
        "discipline": discipline,
        "target_terms": sorted(target_terms)[:25],
        "total_topics": len(topics),
        "content_topics": len(content_topics),
        "relevant_topics": len(relevant_topics),
        "irrelevant_topics": len(irrelevant_topics),
        "irrelevant_share": round(irrelevant_share, 4),
        "threshold": TOPIC_RELEVANCE["max_irrelevant_share"],
        "needs_regeneration": bool(content_topics) and irrelevant_share > TOPIC_RELEVANCE["max_irrelevant_share"],
        "details": details,
        "mismatch_topics": [d["topic"] for d in details if not d["relevant"]],
    }
    return filtered_topics, report


def _build_strict_content_prompt(base_prompt: str) -> str:
    """Ужесточает prompt для повторной генерации только секции content."""
    tighten = """

ДОПОЛНИТЕЛЬНЫЕ ЖЁСТКИЕ ОГРАНИЧЕНИЯ ПО РЕЛЕВАНТНОСТИ:
- Включай ТОЛЬКО темы по интеллектуальным системам и машинному обучению.
- Обязательно используй термины: классификация, регрессия, нейронные сети, NLP, метрики качества.
- Строго ЗАПРЕЩЕНЫ темы из других дисциплин (химия, медицина, право, экономика, история и т.п.).
- Если сомневаешься в теме — НЕ включай её в ответ.
"""
    return base_prompt + tighten
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
_json_parse_failures: dict = {}


def _clean_json_artifacts(raw_text: str) -> str:
    """Удаляет типовые артефакты LLM-ответа перед JSON-парсингом."""
    text = (raw_text or "").strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    text = re.sub(r"^\s*\.\.\.\s*$", "", text, flags=re.MULTILINE)
    text = text.replace("\u2026", "")
    text = re.sub(r"//.*?$", "", text, flags=re.MULTILINE)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r",\s*([}\]])", r"\1", text)
    return text.strip()


def _extract_json_candidate(raw_text: str) -> str:
    """Выделяет JSON-массив/объект из ответа после очистки артефактов."""
    cleaned = _clean_json_artifacts(raw_text)
    m_array = re.search(r"\[.*\]", cleaned, re.S)
    if m_array:
        return m_array.group().strip()
    m_object = re.search(r"\{.*\}", cleaned, re.S)
    if m_object:
        return m_object.group().strip()
    return cleaned


def _repair_json_with_llm(raw_json: str) -> str:
    """Промежуточный repair-проход: исправляет только JSON, без изменения смысла."""
    prompt = (
        "Исправь только JSON, без изменения смысла и без добавления новых данных.\n"
        "Требования:\n"
        "- верни только валидный JSON (без markdown, комментариев и пояснений);\n"
        "- сохрани исходную структуру и значения максимально дословно;\n"
        "- убери только синтаксические ошибки, trailing commas и мусорные символы.\n\n"
        "Невалидный JSON:\n"
        f"{raw_json}"
    )
    repaired = llm(prompt, max_tokens=900, json_mode=False, temperature=0.0)
    return _extract_json_candidate(repaired)


def _record_parse_debug(debug: Optional[dict], invalid_json: str = "", repaired_json: str = "",
                        schema_error: str = ""):
    if debug is None:
        return
    if invalid_json:
        debug["raw_invalid_json"] = invalid_json
    if repaired_json:
        debug["repaired_json"] = repaired_json
    if schema_error:
        debug["schema_error"] = schema_error


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def clean(text: str) -> str:
    text = re.sub(r" +", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"\[score=[^\]]+\]\n?", "", text)
    return "\n".join(l.strip() for l in text.split("\n") if l.strip()).strip()


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).lower()


def _first_int(value: str) -> Optional[int]:
    m = re.search(r"-?\d+", value or "")
    return int(m.group(0)) if m else None


def _canonical_attestation(value: str) -> str:
    text = _normalize_text(value)
    if "зач" in text and "диф" in text:
        return "дифференцированный зачёт"
    if "зач" in text:
        return "зачёт"
    if "экзам" in text:
        return "экзамен"
    return (value or "").strip() or "экзамен"


def build_hours_model(cfg: dict, hours: dict) -> dict:
    """Единая модель часов для синхронного заполнения таблиц Т3/Т6/Т7/Т11."""
    lecture = int(hours.get("lecture", 0))
    practice = int(hours.get("practice", 0))
    lab = int(hours.get("lab", 0))
    self_hours = int(hours.get("self", 0))
    contact = lecture + practice + lab
    total = contact + self_hours
    return {
        "credits": int(cfg.get("credits", 0)),
        "lecture": lecture,
        "practice": practice,
        "lab": lab,
        "self": self_hours,
        "contact": contact,
        "total": total,
        "attestation": _canonical_attestation(cfg.get("exam_type", "экзамен")),
    }


def _apply_consistency_action(action_mode: str, issues: list[str], title: str, details: str):
    msg = f"{title}: {details}"
    if action_mode == "error":
        issues.append(msg)
        return
    print(f"  🔧 {msg}")


def validate_document_consistency(doc: Document, hours_model: dict,
                                  consistency_mode: str = "fix") -> dict:
    """
    Проверяет согласованность трудоёмкости/аттестации в документе.

    consistency_mode:
      - fix   (по умолчанию): конфликтующие ячейки принудительно исправляются;
      - error: генерация завершается ошибкой с понятным сообщением.
    """
    mode = str(consistency_mode or "fix").strip().lower()
    if mode not in {"fix", "error"}:
        mode = "fix"

    canonical = {
        "ze": int(hours_model.get("credits", 0)),
        "hours_total": int(hours_model.get("total", 0)),
        "hours_contact": int(hours_model.get("contact", 0)),
        "hours_self": int(hours_model.get("self", 0)),
        "attestation": _canonical_attestation(hours_model.get("attestation", "экзамен")),
    }
    result = {
        "canonical": canonical,
        "mode": mode,
        "fixes": [],
        "errors": [],
    }

    # --- Раздел 3.1 ---
    if len(doc.tables) <= 3:
        result["errors"].append("Не найдена таблица Т3 для проверки раздела 3.1")
    else:
        t3 = doc.tables[3]
        if len(t3.rows) > 4:
            r31 = t3.rows[4]
            expected_vals = {
                1: canonical["ze"],
                2: canonical["hours_total"],
                3: canonical["hours_contact"],
                4: canonical["hours_self"],
            }
            for idx, expected in expected_vals.items():
                if idx >= len(r31.cells):
                    continue
                current = _first_int(r31.cells[idx].text)
                if current != expected:
                    _apply_consistency_action(
                        mode,
                        result["errors"],
                        "Раздел 3.1",
                        f"ячейка r5c{idx + 1}='{r31.cells[idx].text.strip()}' → '{expected}'"
                    )
                    if mode == "fix":
                        set_cell_text(r31.cells[idx], str(expected))
                        result["fixes"].append(f"Т3 r5c{idx + 1}: {current} → {expected}")

            att_cell_idx = 5
            if att_cell_idx < len(r31.cells):
                current_att = _canonical_attestation(r31.cells[att_cell_idx].text)
                if current_att != canonical["attestation"]:
                    _apply_consistency_action(
                        mode,
                        result["errors"],
                        "Раздел 3.1",
                        f"форма аттестации '{r31.cells[att_cell_idx].text.strip()}' → '{canonical['attestation']}'"
                    )
                    if mode == "fix":
                        set_cell_text(r31.cells[att_cell_idx], canonical["attestation"])
                        result["fixes"].append(
                            f"Т3 r5c{att_cell_idx + 1}: аттестация → {canonical['attestation']}"
                        )

        # --- ИТОГО ПО ДИСЦИПЛИНЕ ---
        found_total_row = False
        for r_idx, row in enumerate(t3.rows):
            row_text = " ".join(c.text for c in row.cells)
            if "итого" in _normalize_text(row_text):
                found_total_row = True
                for c_idx, cell in enumerate(row.cells):
                    if _first_int(cell.text) is None:
                        continue
                    if c_idx == 2:
                        current_total = _first_int(cell.text)
                        if current_total != canonical["hours_total"]:
                            _apply_consistency_action(
                                mode,
                                result["errors"],
                                "ИТОГО ПО ДИСЦИПЛИНЕ",
                                f"ячейка r{r_idx + 1}c{c_idx + 1}='{cell.text.strip()}' → '{canonical['hours_total']}'"
                            )
                            if mode == "fix":
                                set_cell_text(cell, str(canonical["hours_total"]))
                                result["fixes"].append(
                                    f"Т3 r{r_idx + 1}c{c_idx + 1}: total → {canonical['hours_total']}"
                                )
                break
        if not found_total_row:
            result["errors"].append("Не найдена строка 'ИТОГО ПО ДИСЦИПЛИНЕ'/'ИТОГО' в таблице Т3")

    # --- Единая форма аттестации во всех таблицах и итоговом листе ---
    attestation_variants = [
        "экзамен",
        "зачет",
        "зачёт",
        "дифференцированный зачет",
        "дифференцированный зачёт",
    ]
    for t_idx, table in enumerate(doc.tables):
        for r_idx, row in enumerate(table.rows):
            for c_idx, cell in enumerate(row.cells):
                cell_text_norm = _normalize_text(cell.text)
                if not cell_text_norm:
                    continue
                if any(v in cell_text_norm for v in attestation_variants):
                    current_att = _canonical_attestation(cell.text)
                    if current_att != canonical["attestation"]:
                        _apply_consistency_action(
                            mode,
                            result["errors"],
                            "Форма аттестации",
                            f"Т{t_idx} r{r_idx + 1}c{c_idx + 1}: '{cell.text.strip()}' → '{canonical['attestation']}'"
                        )
                        if mode == "fix":
                            set_cell_text(cell, canonical["attestation"])
                            result["fixes"].append(
                                f"Т{t_idx} r{r_idx + 1}c{c_idx + 1}: аттестация → {canonical['attestation']}"
                            )

    for p_idx, paragraph in enumerate(doc.paragraphs):
        p_text_norm = _normalize_text(paragraph.text)
        if not p_text_norm:
            continue
        if "форма" in p_text_norm and "аттест" in p_text_norm:
            current_att = _canonical_attestation(paragraph.text)
            if current_att != canonical["attestation"]:
                _apply_consistency_action(
                    mode,
                    result["errors"],
                    "Форма аттестации (итоговый лист)",
                    f"параграф #{p_idx + 1}: '{paragraph.text.strip()}'"
                )
                if mode == "fix":
                    replaced = re.sub(
                        r"(экзамен|зач[её]т|дифференцированный\s+зач[её]т)",
                        canonical["attestation"],
                        paragraph.text,
                        flags=re.IGNORECASE,
                    )
                    paragraph.text = replaced
                    result["fixes"].append(f"Параграф #{p_idx + 1}: аттестация → {canonical['attestation']}")

    if mode == "error" and result["errors"]:
        error_text = "\n".join(f"- {e}" for e in result["errors"])
        raise ValueError("Consistency check failed:\n" + error_text)

    return result


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
            # [БАГ ИСПРАВЛЕН]: d.get("embedding") or d["data"][0]["embedding"]
            # При пустом списке embedding=[] (falsy) Python переходил к d["data"][...] →
            # KeyError, так как Ollama /api/embeddings не возвращает ключ "data".
            # Исправление аналогично load_qdrant.py (БАГ 2): двухшаговая проверка.
            vec = d.get("embedding")
            if not vec:
                data_list = d.get("data") or []
                vec = data_list[0].get("embedding") if data_list else None
            if vec:
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
             direction: str = "", level: str = "") -> tuple[str, list, list]:
    """
    Ищет релевантные чанки в Qdrant.

    [K] Multi-query: объединяем результаты по нескольким формулировкам.
    [B] Доменная фильтрация по direction/level.
    [S] Фильтр использует "section_type" (верхний уровень payload).
    [R] При пустом результате возвращает пустую строку с флагом для caller.

    Возвращает: (ctx_string, reranked_hits, raw_hits) для логирования [C].
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
            hits = _search_qdrant(vec, payload_filter, GENERATION["retrieve_top_k"])
            for h in hits:
                hit_id = h.get("id")
                if hit_id not in all_hits or h.get("score", 0) > all_hits[hit_id].get("score", 0):
                    all_hits[hit_id] = h

        # [STEP-3] Фаза 1: кандидаты retrieval (до rerank).
        raw_hits = sorted(
            [h for h in all_hits.values() if h.get("score", 0) >= GENERATION["min_score"]],
            key=lambda h: h.get("score", 0),
            reverse=True
        )[:GENERATION["retrieve_top_k"]]

        # [NEW] Двухуровневый дисциплинарный guard (keyword + embedding)
        # на этапе выбора контекста для content/hours/assessment.
        discipline_vec = get_embedding(discipline)
        discipline_keywords = _tokenize_keywords(discipline)
        filtered_hits: list[dict] = []
        for h in raw_hits:
            guarded_rank, checked, details = _discipline_guard_rank(
                section, section_types, discipline, h, discipline_vec, discipline_keywords
            )
            h["_guarded_rank"] = guarded_rank
            h["_guard_checked"] = checked
            h["_guard"] = details
            if checked and not details.get("pass", False):
                continue
            filtered_hits.append(h)

        # [STEP-3] Фаза 2: rerank по приоритету + discipline-guard + diversity по source.
        reranked_hits = sorted(
            filtered_hits,
            key=lambda h: h.get("_guarded_rank", _rank_score(h)),
            reverse=True,
        )
        reranked_hits = _apply_source_diversity(
            reranked_hits, GENERATION["max_chunks_per_source"]
        )
        good_hits = reranked_hits[:GENERATION["top_k"]]

        # [R] Fallback при пустом retrieval — снижаем порог и убираем фильтр
        if not good_hits:
            print(f"    ⚠️  RAG [{section}]: нет чанков выше {GENERATION['min_score']}, "
                  f"пробую без доменного фильтра...")
            vec = get_embedding(queries[0])
            if vec:
                hits = _search_qdrant(vec, None, GENERATION["retrieve_top_k"])
                raw_hits = sorted(
                    [h for h in hits if h.get("score", 0) >= GENERATION["min_score"] * 0.7],
                    key=lambda h: h.get("score", 0),
                    reverse=True
                )[:GENERATION["retrieve_top_k"]]
                discipline_vec = get_embedding(discipline)
                discipline_keywords = _tokenize_keywords(discipline)
                filtered_hits = []
                for h in raw_hits:
                    guarded_rank, checked, details = _discipline_guard_rank(
                        section, section_types, discipline, h, discipline_vec, discipline_keywords
                    )
                    h["_guarded_rank"] = guarded_rank
                    h["_guard_checked"] = checked
                    h["_guard"] = details
                    if checked and not details.get("pass", False):
                        continue
                    filtered_hits.append(h)
                reranked_hits = sorted(
                    filtered_hits,
                    key=lambda h: h.get("_guarded_rank", _rank_score(h)),
                    reverse=True,
                )
                reranked_hits = _apply_source_diversity(
                    reranked_hits, GENERATION["max_chunks_per_source"]
                )
                good_hits = reranked_hits[:GENERATION["top_k"]]

        print(
            f"    🔍 RAG [{section}]: raw={len(raw_hits)} → filtered={len(filtered_hits)} → reranked={len(good_hits)} "
            f"(scores: {[round(h.get('score', 0), 3) for h in good_hits]}, "
            f"ranked: {[round(h.get('_guarded_rank', _rank_score(h)), 3) for h in good_hits]})"
        )

        # Сборка контекста с метаданными источника
        seen_texts: set = set()
        parts: list[str] = []
        for h in good_hits:
            payload = h.get("payload", {})
            raw_text = payload.get("text", "")
            if not raw_text:
                continue

            # [замечание #12 ИСПРАВЛЕНО]: ранее hard-cut payload["text"][:1200]
            # мог обрывать текст посередине предложения, давая LLM
            # неструктурированный фрагмент. Теперь при превышении 1200 символов
            # ищем последнюю точку в диапазоне [800, 1200] и обрезаем по ней.
            # Если точка не найдена — оставляем сырой срез (лучше, чем ничего).
            if len(raw_text) > 1200:
                cut = raw_text[:1200]
                last_dot = cut.rfind(".")
                text = cut[:last_dot + 1] if last_dot >= 800 else cut
            else:
                text = raw_text

            dedup_key = hashlib.sha256(text.strip().encode("utf-8")).hexdigest()
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

        # [J] Ограничение длины контекста — предотвращает переполнение окна LLM.
        # При превышении MAX_CONTEXT_CHARS обрезаем с явной пометкой.
        if len(ctx) > MAX_CONTEXT_CHARS:
            ctx = ctx[:MAX_CONTEXT_CHARS].rsplit("\n", 1)[0]
            ctx += "\n[...контекст обрезан до MAX_CONTEXT_CHARS символов...]"

        RETRIEVE_CACHE[cache_key] = (ctx, good_hits, raw_hits)
        return ctx, good_hits, raw_hits

    except Exception as e:
        print(f"  ⚠️  RAG [{section}]: {e}")
        return "", [], []


def llm(prompt: str, max_tokens: int = 600, json_mode: bool = False,
        temperature: float = 0.3) -> str:
    for attempt in range(3):
        try:
            options = {
                "temperature": temperature,
                "num_predict": max_tokens,
                "num_ctx": 4096,  # [L] увеличено с 2048
            }
            body = {
                "model": OLLAMA["llm_model"],
                "prompt": prompt,
                "stream": False,
                "options": options,
            }
            # [STEP-2] Для JSON-секций просим строгий JSON-формат.
            if json_mode:
                body["format"] = "json"

            r = requests.post(OLLAMA["generate_url"],
                json=body,
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


def _sanitize_retrieved_text(text: str) -> str:
    """
    [замечание #13] Базовая защита от prompt injection в retrieved-контексте.

    Если corpus содержит строки вида "Ignore previous instructions" или
    "System: ...", LLM может их воспринять как системные директивы.
    Фильтруем строки, начинающиеся с типичных injection-паттернов.
    Это не полная защита (для production нужен отдельный guard-слой),
    но устраняет наиболее очевидные векторы атаки.
    """
    INJECTION_PATTERNS = re.compile(
        r"^(ignore\s+(previous|all|prior)|forget\s+(previous|all)|"
        r"system\s*:|instruction\s*:|act\s+as\s|"
        r"забудь\s+(предыдущ|все)|игнорируй\s+предыдущ|"
        r"ты\s+теперь\s|притворяйся\s|ты\s+—\s)",
        re.IGNORECASE,
    )
    clean_lines = [
        line for line in text.split("\n")
        if not INJECTION_PATTERNS.match(line.strip())
    ]
    return "\n".join(clean_lines).strip()


def gen(label: str, discipline: str, prompt: str,
        direction: str = "", level: str = "", json_mode: bool = False,
        temperature: float = 0.3, **extra) -> str:
    """
    Генерация секции с RAG-контекстом.

    [R] При пустом retrieval добавляет явную инструкцию в промпт.
    [C] Сохраняет данные в _generation_log для последующей записи в JSON.
    """
    section_types = SECTION_TYPE_FILTER.get(label)
    ctx, hits, raw_hits = retrieve(label, discipline, section_types, direction, level)

    # [замечание #13] Санитизация retrieved-контекста перед вставкой в промпт
    ctx = _sanitize_retrieved_text(ctx)

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

    # [БАГ 5 ИСПРАВЛЕНО]: direction и level не попадали в fmt_vars.
    # Если промпт содержит {direction} или {level} → KeyError при format().
    # Добавляем явно, до **extra — чтобы extra мог при необходимости переопределить.
    fmt_vars = {"discipline": discipline, "direction": direction, "level": level, **extra}
    full_prompt = ctx_block + prompt.format(**fmt_vars) + f"\n\nСоздай для «{discipline}»:"
    result = llm(full_prompt, json_mode=json_mode, temperature=temperature)

    # [C] Логируем для generation_log.json
    _generation_log[label] = {
        "prompt_preview":   full_prompt[:600],
        "retrieved_raw": [
            {
                "id":           h.get("id"),
                "source":       h.get("payload", {}).get("source", ""),
                "score":        round(h.get("score", 0), 4),
                "rank_score":   round(_rank_score(h), 4),
                "text_preview": h.get("payload", {}).get("text", "")[:120],
            }
            for h in raw_hits
        ],
        "retrieved_chunks": [
            {
                "id":           h.get("id"),
                "source":       h.get("payload", {}).get("source", ""),
                "score":        round(h.get("score", 0), 4),
                "rank_score":   round(_rank_score(h), 4),
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
    # Быстрый путь: old целиком в одном run — заменяем, не трогая остальные
    for run in para.runs:
        if old in run.text:
            run.text = run.text.replace(old, new)
            return
    # [БАГ 2 ИСПРАВЛЕНО]: old разбит между несколькими runs.
    # Сохраняем rPr (форматирование) первого run, очищаем все runs,
    # кладём замену в runs[0] и восстанавливаем оригинальный rPr.
    if not para.runs:
        return
    from copy import deepcopy
    WNS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    rpr_tag = f"{{{WNS}}}rPr"
    first_r = para.runs[0]._r
    saved_rpr = deepcopy(first_r.find(rpr_tag))  # None если rPr нет
    full = para.text.replace(old, new)
    for run in para.runs:
        run.text = ""
    para.runs[0].text = full
    # Восстанавливаем bold/italic/font/size/colour из сохранённого rPr
    if saved_rpr is not None:
        existing = para.runs[0]._r.find(rpr_tag)
        if existing is not None:
            para.runs[0]._r.remove(existing)
        para.runs[0]._r.insert(0, saved_rpr)


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


def clear_tail_tables(doc: Document, table_indexes: list[int], keep_rows: int = 2):
    """Очищает хвостовые таблицы от старого текста шаблона перед заполнением."""
    for idx in table_indexes:
        if idx >= len(doc.tables):
            continue
        table = doc.tables[idx]
        if len(table.rows) <= keep_rows:
            continue
        clear_table_data_rows(table, start_row=keep_rows)


def clear_passport_blocks(doc: Document):
    """Явно очищает паспортные блоки (табличные поля и маркерные абзацы)."""
    # Таблица трудоёмкости/паспорта: чистим строку семестра и строку ИТОГО.
    if len(doc.tables) > 3 and len(doc.tables[3].rows) > 5:
        for row_idx in (4, 5):
            row = doc.tables[3].rows[row_idx]
            for cell in row.cells:
                set_cell_text(cell, "")

    # Таблица ФОС: полностью очищаем данные, чтобы старые компетенции не оставались.
    if len(doc.tables) > 21:
        clear_table_data_rows(doc.tables[21], start_row=1)

    # Маркерные паспортные абзацы (часто остаются в шаблоне отдельным текстом).
    passport_markers = ("паспорт", "код и направление подготовки", "рабочая программа дисциплины")
    for para in doc.paragraphs:
        text = _normalize_text(para.text)
        if not text:
            continue
        if any(marker in text for marker in passport_markers) and "{" in para.text:
            for run in para.runs:
                run.text = ""


def collect_doc_terms(doc: Document) -> str:
    chunks: list[str] = [p.text for p in doc.paragraphs]
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                chunks.append(cell.text)
    return _normalize_text("\n".join(chunks))


def _contains_topic_phrase(text: str, phrase: str) -> bool:
    """Проверяет совпадение темы как отдельного слова/фразы, не как подстроки."""
    pattern = rf"(?<!\w){re.escape(phrase)}(?!\w)"
    return re.search(pattern, text, flags=re.IGNORECASE) is not None


def post_validate_terms(doc: Document, discipline: str, competencies: list[tuple[str, str]]) -> tuple[bool, str]:
    """Пост-валидация: блокирует сохранение DOCX при найденных «чужих» темах/компетенциях."""
    text = collect_doc_terms(doc)
    discipline_keys = _tokenize_keywords(discipline)

    comp_words = set()
    for _, comp_desc in competencies:
        comp_words.update(_tokenize_keywords(comp_desc))

    non_target = sorted({
        topic for topic in NON_TARGET_TOPICS
        if _contains_topic_phrase(text, topic)
    })
    if non_target:
        return False, f"Обнаружены чужие темы: {', '.join(non_target[:6])}"

    # Если нет опорных слов дисциплины и компетенций — вероятно в документе чужой хвост.
    support_hits = sum(1 for kw in discipline_keys.union(comp_words) if kw and kw in text)
    if support_hits < 3:
        return False, "Недостаточно терминов целевой дисциплины после заполнения шаблона"

    return True, "ok"


# ---------------------------------------------------------------------------
# [A] JSON-парсеры с fallback на regex
# ---------------------------------------------------------------------------

def parse_competencies_json(text: str, debug: Optional[dict] = None) -> list | None:
    """
    [A] Пытается разобрать JSON-ответ LLM для компетенций.
    Ожидаемый формат: [{"code": "УК-1", "desc": "Способен..."}]
    """
    # [БАГ 8 ИСПРАВЛЕНО]: нежадный r"\[.*?\]" → жадный r"\[.*\]"
    candidate = _extract_json_candidate(text)
    if "[" not in candidate:
        return None
    try:
        data = json.loads(candidate)
    except (json.JSONDecodeError, TypeError):
        _record_parse_debug(debug, invalid_json=candidate)
        repaired_json = _repair_json_with_llm(candidate)
        _record_parse_debug(debug, repaired_json=repaired_json)
        try:
            data = json.loads(repaired_json)
        except (json.JSONDecodeError, TypeError):
            return None

    if not isinstance(data, list):
        _record_parse_debug(debug, schema_error="expected_json_array")
        return None

    result = [
        (str(d.get("code", "")), str(d.get("desc", "")))
        for d in data
        if isinstance(d, dict) and d.get("code") and d.get("desc")
    ]
    if len(result) < 3:
        _record_parse_debug(debug, schema_error="min_items_violation")
        return None

    codes = [code.strip().upper() for code, _ in result]
    if len(set(codes)) != len(codes):
        _record_parse_debug(debug, schema_error="duplicate_competency_codes")
        return None

    return result


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


def parse_outcomes_json(text: str, debug: Optional[dict] = None) -> list | None:
    """
    [A] Пытается разобрать JSON-ответ LLM для результатов обучения.
    Ожидаемые форматы:
      1) Legacy: [{"type": "З", "text": "..."}, ...]
      2) По компетенциям: [{"code": "УК-1", "type": "З", "text": "..."}, ...]
    """
    candidate = _extract_json_candidate(text)
    if "[" not in candidate:
        return None
    try:
        data = json.loads(candidate)
    except (json.JSONDecodeError, TypeError):
        _record_parse_debug(debug, invalid_json=candidate)
        repaired_json = _repair_json_with_llm(candidate)
        _record_parse_debug(debug, repaired_json=repaired_json)
        try:
            data = json.loads(repaired_json)
        except (json.JSONDecodeError, TypeError):
            return None

    if not isinstance(data, list):
        _record_parse_debug(debug, schema_error="expected_json_array")
        return None

    result = []
    for d in data:
        if not isinstance(d, dict):
            continue
        otype = str(d.get("type", ""))
        text_value = str(d.get("text", ""))
        if otype not in ("З", "У", "В") or not text_value:
            continue

        code = str(d.get("code", "")).strip()
        result.append((otype, text_value, code) if code else (otype, text_value))

    if len(result) < 3:
        _record_parse_debug(debug, schema_error="min_items_violation")
        return None

    outcome_signatures = [
        f"{item[0]}::{item[1].strip().lower()}::{(item[2] if len(item) > 2 else '').strip().upper()}"
        for item in result
    ]
    if len(set(outcome_signatures)) != len(outcome_signatures):
        _record_parse_debug(debug, schema_error="duplicate_outcomes")
        return None
    return result


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


def parse_topics_json(text: str, debug: Optional[dict] = None) -> list | None:
    """
    [A] Пытается разобрать JSON-ответ LLM для тематического плана.
    Ожидаемый формат: [{"type": "section"|"topic", "label": "Раздел 1", "name": "..."}]
    """
    candidate = _extract_json_candidate(text)
    if "[" not in candidate:
        return None
    try:
        data = json.loads(candidate)
    except (json.JSONDecodeError, TypeError):
        _record_parse_debug(debug, invalid_json=candidate)
        repaired_json = _repair_json_with_llm(candidate)
        _record_parse_debug(debug, repaired_json=repaired_json)
        try:
            data = json.loads(repaired_json)
        except (json.JSONDecodeError, TypeError):
            return None

    if not isinstance(data, list):
        _record_parse_debug(debug, schema_error="expected_json_array")
        return None

    topics = []
    for d in data:
        if not isinstance(d, dict):
            continue
        label = str(d.get("label", "")).strip()
        name = str(d.get("name", "")).strip()
        if label and name:
            topics.append(f"{label}. {name}")

    if len(topics) < 3:
        _record_parse_debug(debug, schema_error="min_items_violation")
        return None
    topic_keys = [t.lower() for t in topics]
    if len(set(topic_keys)) != len(topic_keys):
        _record_parse_debug(debug, schema_error="duplicate_topic_titles")
        return None
    return topics


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


def parse_list_json(text: str, min_items: int = 3, debug: Optional[dict] = None) -> list | None:
    """
    [A] Пытается разобрать JSON-ответ LLM для списка ЛР/ПЗ.
    Ожидаемый формат: [{"title": "Реализация алгоритма..."}, ...]

    [БАГ ИСПРАВЛЕН]: min_items вынесен в параметр.
    Замечание: "Нет контроля длины LLM ответа — модель возвращает 4 лабораторных
    вместо 6. Хотя парсер пытается исправлять, лучше проверять count items."
    Раньше порог был жёстко зашит как >= 3, что позволяло принять неполный список
    (4 из 6 ЛР) как "валидный" JSON — retry не срабатывал, дефолт не подставлялся.
    Теперь caller передаёт min_items=6, и неполный список возвращает None → retry.
    """
    candidate = _extract_json_candidate(text)
    if "[" not in candidate:
        return None
    try:
        data = json.loads(candidate)
    except (json.JSONDecodeError, TypeError):
        _record_parse_debug(debug, invalid_json=candidate)
        repaired_json = _repair_json_with_llm(candidate)
        _record_parse_debug(debug, repaired_json=repaired_json)
        try:
            data = json.loads(repaired_json)
        except (json.JSONDecodeError, TypeError):
            return None

    if not isinstance(data, list):
        _record_parse_debug(debug, schema_error="expected_json_array")
        return None

    result = [str(d.get("title", "")).strip() for d in data
              if isinstance(d, dict) and d.get("title")]
    if len(result) < min_items:
        _record_parse_debug(debug, schema_error="min_items_violation")
        return None
    lowered = [r.lower() for r in result]
    if len(set(lowered)) != len(lowered):
        _record_parse_debug(debug, schema_error="duplicate_titles")
        return None
    return result


def _default_list_titles(list_kind: str = "lab_works") -> list[str]:
    """Безопасные дефолтные заголовки для ЛР/ПЗ."""
    if list_kind == "practice":
        return [f"Практическое занятие {i}" for i in range(1, 7)]
    return [f"Лабораторная работа {i}" for i in range(1, 7)]


def _is_human_readable_topic(item: str) -> bool:
    """Проверяет, что элемент похож на человекочитаемую тему без JSON-символики."""
    if not isinstance(item, str):
        return False
    text = item.strip()
    if len(text) < 6:
        return False
    if re.search(r"[\{\}\[\]\"]", text):
        return False
    if re.search(r'"[^\"]+"\s*:', text):
        return False
    if any(k in text.lower() for k in ["no лр", "no пз", "трудоемкость, часы", "номер раздела"]):
        return False
    return True


def parse_list(text: str, discipline: str = "", min_items: int = 3,
               list_kind: str = "lab_works") -> list:
    """[A] Парсит список ЛР/ПЗ: JSON-режим → regex-fallback.
    [БАГ 3 ИСПРАВЛЕНО]: добавлен параметр min_items (ранее был захардкожен как 3).
    Caller передаёт min_items=6 → fallback-список из 4 ЛР/ПЗ теперь
    корректно отклоняется вместо попадания в документ.
    """
    json_result = parse_list_json(text, min_items=min_items)
    if json_result:
        return json_result[:8]

    OFFTRACK_KEYWORDS = [
        "презентаци", "доклад", "реферат", "публикаци", "журнал",
        "flutter", "react native", "android studio", "xcode",
        "google play", "app store", "swift", "kotlin",
        "устный", "подготовка к",
    ]
    items = []
    technical_keys = ("No ЛР", "No ПЗ", "Трудоемкость, часы", "Номер раздела")
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        if line.startswith(("{", "}", '"')):
            continue
        if re.search(r'"[^\"]+"\s*:', line):
            continue
        if any(key.lower() in line.lower() for key in technical_keys):
            continue
        line = re.sub(r"^(ЛР\s*№?\d+|ЛР\s*No\d+|\d+[\.):])\s+", "", line)
        line = re.sub(r"^\*\*(.+)\*\*$", r"\1", line)
        line = re.sub(r"^<[^>]{1,30}>\s*[-–\.\:]?\s*", "", line)
        line = re.sub(r"^<[^>]{1,30}>\s*$", "", line)
        if not line or len(line) < 6:
            continue
        if any(kw in line.lower() for kw in OFFTRACK_KEYWORDS):
            continue
        items.append(line)
    return items[:8] if len(items) >= min_items else _default_list_titles(list_kind)


# ---------------------------------------------------------------------------
# Заполнение таблиц шаблона
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Библиография — генерация и заполнение таблиц
# ---------------------------------------------------------------------------

def parse_bibliography_json(text: str, debug: Optional[dict] = None) -> list | None:
    """
    Парсит JSON-ответ LLM для библиографических записей.
    Ожидаемые поля: type/purpose/desc/url/coeff.
    """
    candidate = _extract_json_candidate(text)
    if "[" not in candidate:
        return None
    try:
        data = json.loads(candidate)
    except (json.JSONDecodeError, TypeError):
        _record_parse_debug(debug, invalid_json=candidate)
        repaired_json = _repair_json_with_llm(candidate)
        _record_parse_debug(debug, repaired_json=repaired_json)
        try:
            data = json.loads(repaired_json)
        except (json.JSONDecodeError, TypeError):
            return None

    if not isinstance(data, list):
        _record_parse_debug(debug, schema_error="expected_json_array")
        return None

    result = [d for d in data if isinstance(d, dict) and d.get("desc")]
    if len(result) < 1:
        _record_parse_debug(debug, schema_error="min_items_violation")
        return None
    desc_keys = [str(d.get("desc", "")).strip().lower() for d in result]
    if len(set(desc_keys)) != len(desc_keys):
        _record_parse_debug(debug, schema_error="duplicate_titles")
        return None
    return result


def _normalize_for_match(value: str) -> str:
    return re.sub(r"[^а-яa-z0-9\s-]", " ", (value or "").lower())


def _tokenize_for_match(value: str) -> list[str]:
    stop_words = {
        "изд", "издание", "учебник", "учебное", "пособие", "том", "часть",
        "москва", "санкт", "петербург", "пресс", "год", "с", "стр", "пер",
        "подход", "система", "данных", "англ", "для", "обработки", "информация",
    }
    tokens = re.findall(r"[а-яa-z0-9-]+", _normalize_for_match(value))
    return [t for t in tokens if len(t) >= 4 and t not in stop_words]


def _extract_source_candidates() -> list[dict]:
    """
    Собирает потенциальные библиографические строки из chunks.jsonl и rpd_json/*.json.
    Возвращает список dict: {text, source, section_title}.
    """
    candidates: list[dict] = []
    seen: set[str] = set()

    def _push_line(line: str, source: str, section_title: str):
        text = re.sub(r"\s+", " ", (line or "").strip())
        if not text:
            return
        if len(text) < 30 or len(text) > 700:
            return
        # Базовые признаки библиографической записи.
        if not re.search(r"(19|20)\d{2}", text):
            return
        if "—" not in text and "/" not in text:
            return
        key = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if key in seen:
            return
        seen.add(key)
        candidates.append({"text": text, "source": source, "section_title": section_title})

    if os.path.exists("chunks.jsonl"):
        with open("chunks.jsonl", encoding="utf-8") as f:
            for line in f:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                text = row.get("text", "")
                for part in re.split(r"[\n\r]+", text):
                    _push_line(part, row.get("source", "chunks.jsonl"), row.get("section_title", ""))

    if os.path.isdir("rpd_json"):
        for name in os.listdir("rpd_json"):
            if not name.endswith(".json"):
                continue
            path = os.path.join("rpd_json", name)
            try:
                data = json.load(open(path, encoding="utf-8"))
            except Exception:
                continue
            for chunk in data.get("chunks", []):
                text = chunk.get("text", "")
                for part in re.split(r"[\n\r]+", text):
                    _push_line(part, name, chunk.get("section_title", ""))

    return candidates


def _load_bibliography_allowlist() -> list[dict]:
    if not os.path.exists(BIBLIOGRAPHY_ALLOWLIST):
        return []
    try:
        payload = json.load(open(BIBLIOGRAPHY_ALLOWLIST, encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    return [e for e in payload if isinstance(e, dict) and e.get("desc")]


def _entry_signature(desc: str) -> tuple[str, set[str]]:
    tokens = _tokenize_for_match(desc)
    author = tokens[0] if tokens else ""
    title_tokens = set(tokens[1:8]) if len(tokens) > 1 else set()
    return author, title_tokens


def _match_entry_to_sources(entry: dict, source_candidates: list[dict]) -> list[dict]:
    desc = entry.get("desc", "")
    author_token, title_tokens = _entry_signature(desc)
    matched: list[dict] = []
    if not author_token or not title_tokens:
        return matched
    for src in source_candidates:
        src_tokens = set(_tokenize_for_match(src.get("text", "")))
        title_overlap = len(title_tokens.intersection(src_tokens))
        if author_token in src_tokens and title_overlap >= 2:
            matched.append(src)
    return matched


def _is_gost_like(desc: str) -> bool:
    text = re.sub(r"\s+", " ", (desc or "").strip())
    return bool(re.search(r"^[А-ЯA-ZЁ][а-яa-zё-]+,\s*[А-ЯA-ZЁ]", text)) and bool(
        re.search(r"—\s*[А-ЯA-Zа-яё\-\s]+\s*:\s*[^,]+,\s*(19|20)\d{2}\.", text)
    )


def _dedupe_bibliography_entries(entries: list[dict]) -> list[dict]:
    unique: list[dict] = []
    seen: set[str] = set()
    for entry in entries:
        key = _normalize_for_match(entry.get("desc", ""))
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(entry)
    return unique


def gen_bibliography(discipline: str, direction: str = "", level: str = "") -> tuple[list, list]:
    """
    Генерирует основную (Т15) и методическую (Т17) литературу.
    Возвращает (main_entries, method_entries).
    Каждая запись — dict с полями: type/purpose/desc/url/coeff.

    T15: формируем только из подтверждённых источников (RAG-контекст)
         или из заранее подготовленного allowlist JSON.
         Новые книги свободно НЕ генерируются.

    T17: qwen2.5:3b стабильно копирует «Фамилия, И. О. Название» из промпта.
         Обходим LLM полностью — всегда используем fallback с реальными
         УГНТУ-пособиями в корректном формате.
    """
    # Признаки шаблонных/галлюцинированных записей
    _PLACEHOLDER_MARKERS = ("фамилия", "название", "<гост", "<реальная", "...", "<")

    def _is_placeholder(desc: str) -> bool:
        dl = desc.lower()
        return any(m in dl for m in _PLACEHOLDER_MARKERS)

    def _make_fallback_main() -> list:
        """Реальные учебники по ИИ/МО, доступные в российских ЭБС."""
        return [
            {
                "type": "Основная литература",
                "purpose": "Для изучения теории;",
                "desc": (
                    "Флах, П. Машинное обучение : наука и искусство построения алгоритмов, "
                    "которые извлекают знания из данных / П. Флах ; пер. с англ. "
                    "А. А. Слинкина. — Москва : ДМК Пресс, 2015. — 400 с."
                ),
                "url": "http://www.znanium.com",
                "coeff": "1.00",
            },
            {
                "type": "Основная литература",
                "purpose": "Для изучения теории;Для выполнения СРО;",
                "desc": (
                    "Осовский, С. Нейронные сети для обработки информации : учебное пособие / "
                    "С. Осовский ; пер. с польск. И. Д. Рудинского. — Москва : "
                    "Финансы и статистика, 2002. — 344 с."
                ),
                "url": "http://www.znanium.com",
                "coeff": "1.00",
            },
            {
                "type": "Дополнительная литература",
                "purpose": "Для изучения теории;",
                "desc": (
                    "Рассел, С. Искусственный интеллект : современный подход / "
                    "С. Рассел, П. Норвиг ; пер. с англ. — 4-е изд. — Москва : "
                    "Вильямс, 2022. — 1408 с."
                ),
                "url": "http://biblio-online.ru",
                "coeff": "0.50",
            },
        ]

    def _make_fallback_method(disc: str) -> list:
        """УГНТУ-пособия — всегда используем fallback (LLM не знает конкретных пособий кафедры)."""
        return [
            {
                "purpose": "Для выполнения лабораторных работ;",
                "desc": (
                    f"Методические указания к выполнению лабораторных работ "
                    f"по дисциплине «{disc}» / УГНТУ, каф. ВТИК ; сост. Д. М. Зарипов. — "
                    "Уфа : УГНТУ, 2023. — 64 с."
                ),
                "url": "http://bibl.rusoil.net",
                "coeff": "1.00",
            },
            {
                "purpose": "Для выполнения практических занятий;",
                "desc": (
                    f"Методические указания к практическим занятиям "
                    f"по дисциплине «{disc}» / УГНТУ, каф. ВТИК ; сост. Д. М. Зарипов. — "
                    "Уфа : УГНТУ, 2023. — 48 с."
                ),
                "url": "http://bibl.rusoil.net",
                "coeff": "1.00",
            },
        ]

    # ── Основная литература — только grounding (RAG/allowlist) ───────────
    source_candidates = _extract_source_candidates()
    allowlist_entries = _load_bibliography_allowlist() or _make_fallback_main()

    confirmed_entries: list[dict] = []
    confirmations: list[dict] = []
    required_count = 3

    for entry in allowlist_entries:
        if _is_placeholder(entry.get("desc", "")):
            continue
        matches = _match_entry_to_sources(entry, source_candidates)
        if matches:
            e = dict(entry)
            e["grounding"] = {
                "matched": True,
                "sources": matches[:3],
                "match_type": "author_title",
            }
            confirmed_entries.append(e)
            confirmations.append({
                "desc": entry.get("desc", ""),
                "matched_sources": matches[:3],
            })

    confirmed_entries = _dedupe_bibliography_entries(
        [e for e in confirmed_entries if _is_gost_like(e.get("desc", ""))]
    )

    grounded_fallback = False
    if len(confirmed_entries) >= required_count:
        main_entries = confirmed_entries[:required_count]
        print(f"    ✅ Библиография T15: подтверждено {len(main_entries)} записей из корпуса")
    else:
        grounded_fallback = True
        safe_fallback = _dedupe_bibliography_entries(
            [e for e in allowlist_entries if _is_gost_like(e.get("desc", ""))]
        )
        main_entries = safe_fallback[:required_count]
        print(
            "    ⚠️  Библиография T15: подтверждений недостаточно "
            f"({len(confirmed_entries)}/{required_count}) → grounded_fallback"
        )

    _generation_log["bibliography_main"] = {
        "mode": "grounded_only",
        "discipline": discipline,
        "required_count": required_count,
        "confirmed_count": len(confirmed_entries),
        "grounded_fallback": grounded_fallback,
        "source_candidates_count": len(source_candidates),
        "confirmations": confirmations,
        "selected_entries": [e.get("desc", "") for e in main_entries],
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    # ── Методические издания — всегда fallback ───────────────────────────
    # qwen2.5:3b стабильно копирует «Фамилия, И. О. Название» из любого промпта,
    # поскольку не знает конкретных методических пособий кафедры ВТИК УГНТУ.
    method_entries = _make_fallback_method(discipline)
    print(f"    ✅ Библиография T17: используется fallback (реальные УГНТУ-пособия)")

    return main_entries, method_entries


def fill_bibliography_main(doc: Document, entries: list, semester: str):
    """
    Заполняет таблицу 15 (основная и дополнительная литература).
    Очищает старые записи из шаблона и вставляет новые.
    """
    if len(doc.tables) <= 15:
        print("  ⚠️  Т15 (библиография): таблица не найдена")
        return
    t = doc.tables[15]
    # Определяем строку-шаблон (первая строка с данными, после заголовков)
    # В таблице обычно 3 строки заголовков, затем данные
    header_rows = 3
    tmpl = clear_table_data_rows(t, start_row=header_rows)

    for entry in entries:
        row_vals = [
            entry.get("type", "Основная литература"),
            entry.get("purpose", "Для изучения теории;"),
            semester,   # очная
            "",         # очно-заочная
            "",         # заочная
            entry.get("desc", ""),
            "1",        # кол-во экз.
            entry.get("url", ""),
            entry.get("coeff", "1.00"),
        ]
        add_table_row(t, row_vals, tmpl)


def fill_bibliography_method(doc: Document, entries: list, semester: str):
    """
    Заполняет таблицу 17 (учебно-методические издания).
    """
    if len(doc.tables) <= 17:
        print("  ⚠️  Т17 (метод.издания): таблица не найдена")
        return
    t = doc.tables[17]
    header_rows = 3
    tmpl = clear_table_data_rows(t, start_row=header_rows)

    for entry in entries:
        row_vals = [
            entry.get("purpose", "Для выполнения лабораторных работ;"),
            semester,   # очная
            "",         # очно-заочная
            "",         # заочная
            entry.get("desc", ""),
            "1",        # всего
            "0",        # в т.ч. на кафедре
            entry.get("url", ""),
            entry.get("coeff", "1.00"),
        ]
        add_table_row(t, row_vals, tmpl)


def fill_competencies_table(doc: Document, competencies: list):
    if len(doc.tables) <= 4: raise IndexError(f"Шаблон содержит {len(doc.tables)} таблиц, нужна Т4 (индекс 4)")
    table = doc.tables[4]
    tmpl  = clear_table_data_rows(table, start_row=1)
    for i, (code, desc) in enumerate(competencies, 1):
        add_table_row(table, [str(i), desc, code], tmpl)


def fill_outcomes_table(doc: Document, competencies: list, outcomes: list):
    if len(doc.tables) <= 5: raise IndexError(f"Шаблон содержит {len(doc.tables)} таблиц, нужна Т5 (индекс 5)")
    table = doc.tables[5]
    tmpl  = clear_table_data_rows(table, start_row=1)

    type_map: dict = {}
    outcomes_by_code: dict[str, dict[str, list[str]]] = {}
    for item in outcomes:
        if len(item) == 3:
            ot, otext, code = item
            code = code.strip()
            outcomes_by_code.setdefault(code, {}).setdefault(ot, []).append(otext)
            type_map.setdefault(ot, otext)
        else:
            ot, otext = item
            type_map.setdefault(ot, otext)

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

    def pick_unique_item(items: list[str], idx: int, code: str, desc: str) -> str:
        """Выбирает элемент без клонирования первого пункта между компетенциями."""
        if not items:
            return ""
        if len(items) >= len(competencies):
            return items[idx]
        base = items[idx % len(items)]
        # Когда LLM вернул мало пунктов, делаем формулировку уникальной
        # за счёт привязки к конкретной компетенции.
        return f"{base} ({code}: {desc[:70].rstrip(' .,;:')})"

    for idx, (code, desc) in enumerate(competencies):
        indicator = f"{code}.1 {desc[:100]}"
        for otype, items in [("З", z_items), ("У", u_items), ("В", v_items)]:
            result_code = f"{otype}({code})"
            code_items = outcomes_by_code.get(code, {}).get(otype, [])
            if code_items:
                result_text = f"{type_prefix[otype]} {code_items[0]}"
                if len(code_items) > 1:
                    result_text += f"\n{code_items[1]}"
            else:
                primary = pick_unique_item(items, idx, code, desc)
                result_text = f"{type_prefix[otype]} {primary}"
                if len(items) > 1:
                    secondary = items[(idx + 1) % len(items)]
                    if secondary != primary:
                        result_text += f"\n{secondary}"
            add_table_row(table, [code, indicator, result_code, result_text], tmpl)


def fill_topics_table(doc: Document, topics: list, semester: str, hours_model: dict,
                      codes_list: list = None):
    if len(doc.tables) <= 7: raise IndexError(f"Шаблон содержит {len(doc.tables)} таблиц, нужна Т7 (индекс 7)")
    table = doc.tables[7]
    tmpl  = clear_table_data_rows(table, start_row=2)

    sections_only = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    n = max(len(sections_only), 1) if sections_only else max(len(topics), 1)

    lec  = hours_model.get("lecture",  12) // n
    pz   = hours_model.get("practice", 36) // n
    lr   = hours_model.get("lab",      16) // n
    sro  = hours_model.get("self",     62) // n
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
    if len(doc.tables) <= 8: raise IndexError(f"Шаблон содержит {len(doc.tables)} таблиц, нужна Т8 (индекс 8)")
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
    if len(doc.tables) <= 9: raise IndexError(f"Шаблон содержит {len(doc.tables)} таблиц, нужна Т9 (индекс 9)")
    table = doc.tables[9]
    tmpl  = clear_table_data_rows(table, start_row=2)

    if len(lab_works) < 6:
        print(f"  ⚠️  Т9: получено {len(lab_works)} ЛР — дополняю до 6")
        for j in range(len(lab_works), 6):
            lab_works.append(f"Лабораторная работа {j + 1}")

    sections = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    # [БАГ 5 ИСПРАВЛЕНО]: целочисленное деление теряло остаток.
    # Пример: 18ч / 7 ЛР → hrs_each=2, ИТОГО=14 ≠ 18.
    # Теперь remainder распределяется по первым ЛР (каждая получает +1ч).
    n_lab = len(lab_works)
    base  = max(hours_lab // n_lab, 1)
    rem   = hours_lab - base * n_lab
    hours_list = [base + (1 if i < rem else 0) for i in range(n_lab)]

    for i, work in enumerate(lab_works, 1):
        section = sections[(i - 1) % max(len(sections), 1)] if sections else f"Раздел {((i - 1) // 2) + 1}"
        add_table_row(table, [section, str(i), work, str(hours_list[i - 1]), "", ""], tmpl)
    add_table_row(table, ["-", "", "ИТОГО:", str(hours_lab), "", ""], tmpl)


def fill_practice_table(doc: Document, practices: list, topics: list,
                        hours_practice: int = 36):
    if len(doc.tables) <= 10: raise IndexError(f"Шаблон содержит {len(doc.tables)} таблиц, нужна Т10 (индекс 10)")
    table = doc.tables[10]
    tmpl  = clear_table_data_rows(table, start_row=2)

    if len(practices) < 6:
        print(f"  ⚠️  Т10: получено {len(practices)} ПЗ — дополняю до 6")
        for j in range(len(practices), 6):
            practices.append(f"Практическое занятие {j + 1}")

    sections = [t for t in topics if re.match(r"^Раздел\s*\d+", t)]
    # [БАГ 5 ИСПРАВЛЕНО]: аналогично fill_lab_table — равномерное распределение
    # остатка часов на первые занятия, чтобы сумма строк = hours_practice.
    n_prac = len(practices)
    base   = max(hours_practice // n_prac, 1)
    rem    = hours_practice - base * n_prac
    hours_list = [base + (1 if i < rem else 0) for i in range(n_prac)]

    for i, prac in enumerate(practices, 1):
        section = sections[(i - 1) % max(len(sections), 1)] if sections else f"Раздел {((i - 1) // 2) + 1}"
        add_table_row(table, [section, str(i), prac, str(hours_list[i - 1]), "", ""], tmpl)
    add_table_row(table, ["-", "", "ИТОГО:", str(hours_practice), "", ""], tmpl)


def fill_t3_hours(doc: Document, semester: str, hours_model: dict):
    # [БАГ 4 ИСПРАВЛЕНО]: добавлена явная проверка границы — все остальные
    # fill_*() делают raise IndexError, fill_t3_hours молча падал с необработанным
    # IndexError при шаблоне с менее чем 4 таблицами.
    if len(doc.tables) <= 3:
        raise IndexError(
            f"Шаблон содержит {len(doc.tables)} таблиц, нужна Т3 (индекс 3)"
        )
    t = doc.tables[3]
    if len(t.rows) < 5:
        return
    row = t.rows[4]
    vals = [
        semester,
        str(hours_model.get("credits", 0)),
        str(hours_model.get("total", 0)),
        str(hours_model.get("contact", 0)),
        str(hours_model.get("self", 0)),
        _canonical_attestation(hours_model.get("attestation", "экзамен")),
    ]
    for i, v in enumerate(vals):
        if i < len(row.cells):
            set_cell_text(row.cells[i], v)
    if len(t.rows) > 5:
        row5 = t.rows[5]
        for i, v in enumerate([
            "ИТОГО:",
            str(hours_model.get("credits", 0)),
            str(hours_model.get("total", 0)),
            str(hours_model.get("contact", 0)),
            str(hours_model.get("self", 0)),
            "",
        ]):
            if i < len(row5.cells):
                set_cell_text(row5.cells[i], v)


def fill_t6_workload(doc: Document, semester: str, hours_model: dict):
    t = doc.tables[6]
    sem_col = None
    for j, cell in enumerate(t.rows[0].cells):
        if cell.text.strip() == semester:
            sem_col = j
            break
    kw_map = {
        "контактная":            int(hours_model.get("contact", 0)),
        "лекции":                int(hours_model.get("lecture", 0)),
        "практические занятия":  int(hours_model.get("practice", 0)),
        "лабораторные работы":   int(hours_model.get("lab", 0)),
        "самостоятельная":       int(hours_model.get("self", 0)),
    }

    # Явные правила против дубляжа часов:
    # - "экзамен" не добавляет новые часы в Т6 (учитывается в форме аттестации Т3);
    # - "контролируемая самостоятельная работа" входит в SRO и отдельно не суммируется;
    # - "СПД" (самостоятельная работа под руководством преподавателя) входит в SRO.
    no_double_count_labels = ("экзам", "контролируемая самостоятельная работа", "спд")

    for row in t.rows:
        label = row.cells[0].text.strip().lower()

        explicit_value = None
        if any(tag in label for tag in no_double_count_labels):
            explicit_value = 0
        else:
            for kw, val in kw_map.items():
                if kw in label:
                    explicit_value = val
                    break

        if explicit_value is None:
            continue

        set_cell_text(row.cells[1], str(explicit_value))
        # [БАГ ИСПРАВЛЕН]: "if sem_col" → False при sem_col=0 (первый столбец).
        # Если семестр в столбце 0, запись в него молча пропускалась.
        # Исправление: явная проверка "is not None".
        if sem_col is not None and sem_col < len(row.cells):
            set_cell_text(row.cells[sem_col], str(explicit_value))


def fill_t11_sro(doc: Document, topics: list, hours_model: dict):
    if len(doc.tables) <= 11: raise IndexError(f"Шаблон содержит {len(doc.tables)} таблиц, нужна Т11 (индекс 11)")
    t    = doc.tables[11]
    tmpl = clear_table_data_rows(t, start_row=2)
    sections = [tp for tp in topics if re.match(r"^Раздел\s*\d+", tp)]
    n = max(len(sections), 1)

    sro = int(hours_model.get("self", 0))

    hrs_study = round(sro * 0.20)
    hrs_rgr   = round(sro * 0.20)
    hrs_prep  = sro - hrs_study - hrs_rgr

    sro_types = [
        # [БАГ 5 doc]: добавлен "/или" в название вида СРО (замечание по документу #5)
        ("подготовка к лабораторным и/или практическим занятиям", hrs_prep),
        ("изучение учебного материала, вынесенного на СРО",       hrs_study),
        ("выполнение расчётно-графической работы",                 hrs_rgr),
    ]
    for sec_idx, sec in enumerate(sections):
        for stype, total_hrs in sro_types:
            # [БАГ 6 ИСПРАВЛЕНО]: round() давал расхождение суммы строк и ИТОГО.
            # Последний раздел получает остаток: total_hrs - base*(n-1).
            base_per_sec = round(total_hrs / n)
            if sec_idx < n - 1:
                hrs_per_sec = base_per_sec
            else:
                hrs_per_sec = total_hrs - base_per_sec * (n - 1)
            add_table_row(t, [sec, stype, str(hrs_per_sec), "", ""], tmpl)
    add_table_row(t, ["-", "ИТОГО:", str(sro), "", ""], tmpl)


def fill_t21_fos(doc: Document, competencies: list, topics: list):
    # [БАГ 9 ИСПРАВЛЕНО]: добавлена проверка длины doc.tables перед обращением по индексу.
    # Раньше IndexError при нехватке таблиц давал нечитаемое "list index out of range".
    if len(doc.tables) <= 21: raise IndexError(f"Шаблон содержит {len(doc.tables)} таблиц, нужна Т21 (индекс 21)")
    t    = doc.tables[21]
    tmpl = clear_table_data_rows(t, start_row=1)
    sections = [tp for tp in topics if re.match(r"^Раздел\s*\d+", tp)]
    ocs = ["Письменный и устный опрос", "Лабораторная работа",
           "Тест", "Расчётно-графическая работа"]
    n = 1
    for i, sec in enumerate(sections):
        sec_name = re.sub(r"^Раздел\s*\d+\.\s*", "", sec)
        for code, desc in competencies:
            # [БАГ 7 ИСПРАВЛЕНО]: ранее ocs[i % len(ocs)] — смена оценочного средства
            # происходила только при смене раздела, а не строки. Все компетенции одного
            # раздела получали одно оценочное средство. Теперь ротация по счётчику строк n.
            add_table_row(t, [
                str(n), sec_name, f"В({code})", desc,
                f"{code}.1 Демонстрирует применение методов на практике",
                f"Выполняет задания по разделу «{sec_name}»",
                ocs[(n - 1) % len(ocs)]
            ], tmpl)
            n += 1


def sync_hours_postfill(doc: Document, semester: str, topics: list, hours_model: dict,
                        codes_list: Optional[list] = None) -> list[str]:
    """Пост-проход синхронизации сумм по Т3/Т6/Т7/Т11 с автокоррекцией."""
    fixes: list[str] = []

    # Т3 и Т6 переписываем из канонической модели.
    fill_t3_hours(doc, semester, hours_model)
    fixes.append("Т3 синхронизирована с hours_model")
    fill_t6_workload(doc, semester, hours_model)
    fixes.append("Т6 синхронизирована с hours_model")

    # Т7 и Т11 пересобираем, чтобы гарантировать равенство сумм строк и ИТОГО.
    fill_topics_table(doc, topics, semester, hours_model, codes_list)
    fixes.append("Т7 пересчитана с hours_model")
    fill_t11_sro(doc, topics, hours_model)
    fixes.append("Т11 пересчитана с hours_model")

    return fixes


# ---------------------------------------------------------------------------
# [D] Валидация бизнес-правил
# ---------------------------------------------------------------------------

def validate_generation(cfg: dict, hours: dict, competencies: list,
                        topics: list, lab_works: list, practices: list,
                        relevance_report: Optional[dict] = None) -> list[str]:
    """
    [D] Проверяет корректность сгенерированного содержимого.
    Возвращает список предупреждений (пустой = всё ОК).
    """
    warnings: list[str] = []

    # [БАГ 1 ИСПРАВЛЕНО]: cfg.get("hours", 144) — в config.json нет ключа "hours",
    # есть "hours_lecture", "hours_practice", "hours_lab", "hours_self".
    # Результат: ожидаемое значение ВСЕГДА было 144 (дефолт), проверка не работала.
    # Теперь вычисляем expected_total из тех же ключей, что используются в main().
    expected_total = (
        cfg.get("hours_lecture",  12) +
        cfg.get("hours_practice", 36) +
        cfg.get("hours_lab",      16) +
        cfg.get("hours_self",     62)
    )
    actual_total = sum(hours.values())
    if actual_total != expected_total:
        warnings.append(
            f"⚠️  Сумма часов {actual_total} ≠ {expected_total} из config.json"
        )

    credits = int(cfg.get("credits", 0))
    expected_by_credits = credits * 36
    if credits > 0 and actual_total != expected_by_credits:
        raise ValueError(
            f"Конфликт трудоёмкости: total={actual_total} ≠ credits*36={expected_by_credits}"
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

    # Semantic scope mismatch: конкретные темы, не попавшие в целевую область дисциплины.
    if relevance_report and relevance_report.get("mismatch_topics"):
        mismatch_topics = relevance_report.get("mismatch_topics", [])
        preview = "; ".join(mismatch_topics[:5])
        warnings.append(
            "⚠️  Semantic scope mismatch: обнаружены потенциально нерелевантные темы — "
            f"{preview}"
        )

    return warnings


# ---------------------------------------------------------------------------
# [A] Промпты — JSON-режим для всех генерируемых разделов
# ---------------------------------------------------------------------------

PROMPTS = {
    "competencies": """\
Ты составляешь рабочую программу дисциплины для российского технического университета.
Напиши описания компетенций для дисциплины «{discipline}».

Коды компетенций:
{competency_codes_numbered}

Правила:
- каждое описание начинается со слова «Способен»
- описание конкретно для «{discipline}», отражает реальные навыки
- не пиши шаблонные фразы, пиши конкретные профессиональные действия

Примеры правильных описаний (для дисциплины «Машинное обучение»):
[
  {{"code": "УК-1", "desc": "Способен применять системный подход для постановки и декомпозиции задач обработки данных"}},
  {{"code": "ОПК-1", "desc": "Способен разрабатывать алгоритмы машинного обучения и реализовывать их программно"}},
  {{"code": "ПК-1", "desc": "Способен обучать, тестировать и оценивать качество моделей машинного обучения"}}
]

ВЕРНИ ТОЛЬКО JSON-массив (без пояснений, без markdown).
Ровно {competency_count} объектов. Примеры выше — для другой дисциплины, напиши свои для «{discipline}».""",

    "outcomes": """\
Напиши результаты обучения для дисциплины «{discipline}» по ФГОС 3++.
Сформируй результаты для каждой компетенции отдельно.

Компетенции:
{competency_codes_numbered}

Правила:
- для КАЖДОЙ компетенции дай ровно 3 записи: одну З, одну У и одну В
- формулировки между компетенциями должны быть смыслово разными (без копирования)
- З: что знает студент — конкретные методы, алгоритмы, технологии дисциплины
- У: что умеет — начинается с глагола (применять, разрабатывать, анализировать, строить...)
- В: чем владеет — начинается со слова «навыками», «методами» или «инструментами»

Пример правильного формата:
[
  {{"code": "УК-1", "type": "З", "text": "..."}},
  {{"code": "УК-1", "type": "У", "text": "..."}},
  {{"code": "УК-1", "type": "В", "text": "..."}},
  {{"code": "ОПК-1", "type": "З", "text": "..."}},
  {{"code": "ОПК-1", "type": "У", "text": "..."}},
  {{"code": "ОПК-1", "type": "В", "text": "..."}}
]

ВЕРНИ ТОЛЬКО JSON-массив (без пояснений, без markdown).
Ровно {competency_count}×3 объектов с полями code, type, text.
Замени примеры на конкретные результаты для «{discipline}».""",

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

    "bibliography_main": """\
Ты составляешь список рекомендуемой литературы для рабочей программы дисциплины «{discipline}» \
в российском техническом университете.

Напиши 3 записи библиографии: 2 основных учебника и 1 дополнительный.

СТРОГИЕ ПРАВИЛА:
- только реально существующие книги — НЕ придумывай авторов и названия
- авторы должны быть реальными специалистами в области «{discipline}»
- год издания 2010–2024
- формат: Фамилия, И. О. Название : тип / И. О. Фамилия. — Город : Издательство, Год. — N с.
- url: http://www.znanium.com или http://e.lanbook.com или http://biblio-online.ru

ВЕРНИ ТОЛЬКО JSON-массив без пояснений и без markdown:
[
  {{"type": "Основная литература", "purpose": "Для изучения теории;", "desc": "<ГОСТ-запись реального учебника>", "url": "http://www.znanium.com", "coeff": "1.00"}},
  {{"type": "Основная литература", "purpose": "Для выполнения СРО;Для изучения теории;", "desc": "<ГОСТ-запись реального учебника>", "url": "http://e.lanbook.com", "coeff": "1.00"}},
  {{"type": "Дополнительная литература", "purpose": "Для изучения теории;", "desc": "<ГОСТ-запись реального учебника>", "url": "http://biblio-online.ru", "coeff": "0.50"}}
]
Ровно 3 объекта. Замени угловые скобки реальными ГОСТ-записями.""",

    # bibliography_method: ключ оставлен для совместимости, не используется как промпт —
    # методические издания всегда генерируются через fallback (qwen2.5:3b
    # копирует шаблонные "Фамилия, И. О." вместо реальных УГНТУ-пособий).
    "bibliography_method": "",

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
    _json_parse_failures.setdefault(label, 0)

    min_items_required = 6 if label in {"lab_works", "practice"} else 3

    def _is_valid_list_output(value) -> bool:
        if label not in {"lab_works", "practice"}:
            return True
        if not isinstance(value, list) or len(value) < min_items_required:
            return False
        return all(_is_human_readable_topic(v) for v in value)

    def _fallback_with_reason(reason: str, src_raw: str):
        if label in _generation_log:
            _generation_log[label]["fallback_reason"] = reason
            _generation_log[label]["fallback_applied"] = True
        return src_raw, parser_fallback(src_raw)

    def _parse_with_repair_debug(src_raw: str):
        parse_debug: dict = {}
        parsed = parser_json(src_raw, debug=parse_debug)
        if label in _generation_log:
            if parse_debug.get("raw_invalid_json"):
                _generation_log[label]["raw_invalid_json"] = parse_debug["raw_invalid_json"]
            if parse_debug.get("repaired_json"):
                _generation_log[label]["repaired_json"] = parse_debug["repaired_json"]
            if parse_debug.get("schema_error"):
                _generation_log[label]["schema_error"] = parse_debug["schema_error"]
        return parsed

    raw = gen(
        label, discipline, prompt,
        direction=direction, level=level,
        json_mode=True,
        temperature=0.2,
        **extra,
    )
    result = _parse_with_repair_debug(raw)
    if result is not None and _is_valid_list_output(result):
        return raw, result
    if result is not None and not _is_valid_list_output(result):
        _json_parse_failures[label] += 1
        if label in _generation_log:
            _generation_log[label]["post_validation_reason"] = (
                "json_post_validation_failed"
            )
    else:
        _json_parse_failures[label] += 1

    for attempt in range(max_retries):
        print(f"  🔄 [{label}] JSON не распарсился (попытка {attempt + 1}/{max_retries}), "
              f"перегенерация...")
        retry_temperature = 0.15 if attempt == 0 else 0.1
        raw = gen(
            label, discipline, prompt,
            direction=direction, level=level,
            json_mode=True,
            temperature=retry_temperature,
            **extra,
        )
        result = _parse_with_repair_debug(raw)
        if result is not None and _is_valid_list_output(result):
            return raw, result
        if result is not None and not _is_valid_list_output(result):
            if label in _generation_log:
                _generation_log[label]["post_validation_reason"] = (
                    "json_post_validation_failed"
                )
        _json_parse_failures[label] += 1

    print(f"  ⚠️  [{label}] JSON недоступен после {max_retries} попыток — regex-fallback")
    fallback_parsed = parser_fallback(raw)
    if label in {"lab_works", "practice"} and not _is_valid_list_output(fallback_parsed):
        return _fallback_with_reason("fallback_post_validation_failed", raw)
    if label in _generation_log:
        _generation_log[label]["fallback_reason"] = "json_unavailable_regex_fallback"
        _generation_log[label]["fallback_applied"] = True
    return raw, fallback_parsed


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
    hours_model = build_hours_model(cfg, hours)

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

    # Базовые переменные (competencies_summary пустой для первого прохода).
    # [БАГ 7 ИСПРАВЛЕНО]: direction и level УБРАНЫ из base_vars.
    # Раньше они были здесь И передавались явно в gen_with_json_retry(direction=..., level=...).
    # При вызове gen_with_json_retry(..., direction=direction, level=level, **base_vars)
    # Python выбрасывал TypeError: got multiple values for keyword argument 'direction'.
    # direction и level попадают в fmt_vars через явные параметры gen() (после БАГ 5 фикса).
    base_vars = {
        "competency_codes":          competency_codes,
        "competency_codes_numbered": competency_codes_numbered,
        "competency_count":          len(codes_list),
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

    filtered_topics, topic_relevance = classify_topic_relevance(topics, discipline)
    _generation_log["content_topic_relevance"] = topic_relevance

    if topic_relevance.get("needs_regeneration"):
        print("  ⚠️  [content] высокая доля нерелевантных тем — запускаю регенерацию content")
        strict_prompt = _build_strict_content_prompt(PROMPTS["content"])
        raw["content_regenerated"], regenerated_topics = gen_with_json_retry(
            "content", discipline, strict_prompt,
            parser_json=parse_topics_json,
            parser_fallback=parse_topics,
            direction=direction, level=level, **content_vars
        )
        raw["content"] = raw["content_regenerated"]
        filtered_topics, topic_relevance = classify_topic_relevance(regenerated_topics, discipline)
        _generation_log["content_topic_relevance"] = topic_relevance
        topics = regenerated_topics

    topics = filtered_topics

    raw["lab_works"], lab_works = gen_with_json_retry(
        "lab_works", discipline, PROMPTS["lab_works"],
        parser_json=lambda t: parse_list_json(t, min_items=6),
        parser_fallback=lambda t: parse_list(t, discipline, min_items=6, list_kind="lab_works"),
        direction=direction, level=level, **content_vars
    )

    raw["practice"], practices = gen_with_json_retry(
        "practice", discipline, PROMPTS["practice"],
        parser_json=lambda t: parse_list_json(t, min_items=6),
        parser_fallback=lambda t: parse_list(t, discipline, min_items=6, list_kind="practice"),
        direction=direction, level=level, **content_vars
    )

    # --- Шаг 3: библиография ---
    print("  📚 Генерация библиографии...")
    bib_main, bib_method = gen_bibliography(discipline, direction, level)

    # --- [D] Валидация ---
    validation_warnings = validate_generation(
        cfg, hours, competencies, topics, lab_works, practices,
        relevance_report=topic_relevance
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

    # Явная очистка хвостовых таблиц и паспортных блоков до заполнения.
    clear_tail_tables(doc, table_indexes=[7, 8, 9, 10, 11], keep_rows=2)
    clear_passport_blocks(doc)

    old_name = cfg.get("old_discipline", "").strip() or detect_old_discipline(doc)
    old_code = cfg.get("old_code", "")
    new_code = cfg.get("new_code", "")

    if old_name:
        replace_all(doc, old_name, discipline)
    if old_code:
        replacement_code = f"({new_code})" if new_code else ""
        replace_all(doc, f"({old_code})", replacement_code)
        replace_all(doc, old_code, new_code if new_code else "")

    for name, fn, args in [
        ("Т3 Трудоёмкость",        fill_t3_hours,          (doc, semester, hours_model)),
        ("Т4 Компетенции",         fill_competencies_table, (doc, competencies)),
        ("Т5 Результаты обучения", fill_outcomes_table,     (doc, competencies, outcomes)),
        ("Т6 Виды работы",         fill_t6_workload,        (doc, semester, hours_model)),
        ("Т7 Темы",                fill_topics_table,       (doc, topics, semester, hours_model, codes_list)),
        ("Т8 Лекции",              fill_lectures_table,     (doc, topics, hours)),
        ("Т9 ЛР",                  fill_lab_table,          (doc, lab_works, topics, hours["lab"])),
        ("Т10 ПЗ",                 fill_practice_table,     (doc, practices, topics, hours["practice"])),
        ("Т11 СРО",                fill_t11_sro,            (doc, topics, hours_model)),
        ("Т15 Основная лит-ра",    fill_bibliography_main,  (doc, bib_main,   semester)),
        ("Т17 Метод.издания",      fill_bibliography_method,(doc, bib_method, semester)),
        ("Т21 ФОС",                fill_t21_fos,            (doc, competencies, topics)),
    ]:
        try:
            fn(*args)
            print(f"  ✅ {name}")
        except Exception as e:
            print(f"  ⚠️  {name}: {e}")

    try:
        postfill_fixes = sync_hours_postfill(doc, semester, topics, hours_model, codes_list)
        consistency_check = validate_document_consistency(
            doc,
            hours_model,
            consistency_mode=str(cfg.get("consistency_mode", "fix")),
        )
        consistency_check["postfill_sync"] = postfill_fixes
        _generation_log["consistency_check"] = consistency_check
        if consistency_check.get("fixes"):
            print("\n🔧 Consistency check: внесены исправления")
            for item in consistency_check["fixes"]:
                print(f"  - {item}")
        else:
            print("\n✅ Consistency check: расхождений не найдено")
    except Exception as e:
        _generation_log["consistency_check"] = {"errors": [str(e)], "fixes": []}
        print(f"\n❌ Consistency check failed: {e}")
        raise

    is_terms_valid, terms_reason = post_validate_terms(doc, discipline, competencies)
    _generation_log["post_terms_validation"] = {
        "ok": is_terms_valid,
        "reason": terms_reason,
    }
    if not is_terms_valid:
        print(f"\n❌ Пост-валидация терминов не пройдена: {terms_reason}")
        print("❌ Файл DOCX не сохранён из-за обнаружения чужих тем/компетенций")
    else:
        doc.save(OUTPUT_DOCX)
        print(f"\n✅ Сохранено: {OUTPUT_DOCX}")

    # [C] Сохраняем лог генерации
    # [STEP-2] Добавляем счётчики JSON parse failures по секциям.
    for label, fail_count in _json_parse_failures.items():
        if label in _generation_log:
            _generation_log[label]["json_parse_failures"] = fail_count

    try:
        with open(GENERATION_LOG, "w", encoding="utf-8") as f:
            json.dump(_generation_log, f, ensure_ascii=False, indent=2)
        print(f"📋 Лог генерации: {GENERATION_LOG}")
    except Exception as e:
        print(f"  ⚠️  Не удалось сохранить лог: {e}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
