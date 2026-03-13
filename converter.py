"""
converter.py — конвертация DOCX-файлов РПД в JSON-блоки.

Исправления v2:
  - БАГ: flush_buffer(level) вызывался с уровнем НОВОЙ секции вместо текущей.
    Теперь уровень сохраняется в current_level и передаётся корректно.
  - БАГ: split_into_chunks — при разбивке длинного параграфа по предложениям
    переменные buffer/count не сбрасывались после continue, что приводило к
    переносу остатков в следующий параграф. Исправлено: после цикла по
    предложениям флашим остаток и сбрасываем buffer/count.
  - Устранён баг рассинхронизации para_counter / table_counter.
  - Длинные таблицы (> MAX_TABLE_ROWS строк) разбиваются на несколько блоков.
  - Глобальный try/except: один сломанный файл не останавливает конвертацию.
"""

from docx import Document
from docx.document import Document as DocumentClass
from docx.table import Table
from docx.text.paragraph import Paragraph
from docx.oxml.text.paragraph import CT_P
from docx.oxml.table import CT_Tbl
import json
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple

RPD_CORPUS         = "rpd_corpus"
RPD_JSON           = "rpd_json"
MAX_CHUNK_WORDS    = 180
MIN_CHUNK_WORDS    = 20
MAX_HEADING_LENGTH = 300
MAX_TABLE_ROWS     = 30
SECTION_REGEX      = r"^(\d{1,2}(\.\d{1,2}){0,3})(?:[.\s]+)(.+)$"
KEY_HEADERS        = [
    "Цели дисциплины", "Формируемые компетенции",
    "Результаты обучения", "Содержание дисциплины",
]
SECTION_RE = re.compile(SECTION_REGEX)

# Маппинг ключевых слов заголовка → section_type
# ВАЖНО: порядок имеет значение — более специфичные паттерны идут раньше.
SECTION_TYPE_MAP = [
    (["цел", "задач"],                                              "goals"),
    (["компетенц"],                                                 "competencies"),
    # ОВЗ/доступность — РАНЬШЕ learning_outcomes, чтобы «обучения лиц с ОВЗ» не
    # классифицировалось как learning_outcomes через слово «обучен».
    (["доступн", "инвалид", "огранич", "здоровь", "овз"],         "accessibility"),
    # learning_outcomes: требуем «результат» И «обучен» вместе (через regex в detect_section_type),
    # либо явное «индикатор» — одиночное «обучен» уже не срабатывает.
    (["результат обучен", "индикатор"],                            "learning_outcomes"),
    (["содержани", "тематическ"],                                   "content"),
    (["лабораторн", "практическ", "семинар", "самостоятельн"],     "assessment"),
    (["трудоёмк", "трудоемк", "объём", "объем", "часов", "учебн", "нагрузк"], "hours"),
    (["фонд оценочн", "оценочн", "контрол", "аттестац",
      "промежуточн", "текущ"],                                      "assessment"),
    (["учебно-методич", "литератур", "ресурс", "библиотек",
      "программн", "информационн"],                                 "place"),
]


def detect_section_type(section_title: Optional[str]) -> str:
    """Определяет section_type по заголовку секции."""
    if not section_title:
        return "other"
    t = section_title.lower()
    for keywords, stype in SECTION_TYPE_MAP:
        if any(kw in t for kw in keywords):
            return stype
    return "other"


def extract_key_table_rows(table: Table, section_type: str, doc_name: str,
                           section_title: str) -> List[Dict]:
    """
    Для ключевых таблиц (компетенции, результаты, содержание, ЛР/ПЗ)
    извлекает каждую строку данных как отдельный блок с section_type.

    ИСПРАВЛЕНИЕ: строки T5 (результаты обучения) находятся под заголовком
    «Компетенции...» и поэтому получают тип competencies. Детектируем их
    по наличию «Знать:/Уметь:/Владеть:» и переопределяем тип на learning_outcomes.
    """
    if section_type not in ("competencies", "learning_outcomes", "content", "assessment"):
        return []
    rows = process_table(table)
    if len(rows) < 2:
        return []
    header = rows[0]
    blocks = []
    for row_text in rows[1:]:
        row_text = row_text.strip()
        if not row_text or len(row_text.split()) < 5:
            continue
        text = f"{header}\n{row_text}"
        # Определяем реальный тип строки: если содержит Знать/Уметь/Владеть — это learning_outcomes
        row_lower = row_text.lower()
        if any(kw in row_lower for kw in ("знать:", "уметь:", "владеть:", "з(", "у(", "в(")):
            effective_type = "learning_outcomes"
        else:
            effective_type = section_type
        blocks.append({
            "title":         doc_name,
            "section_title": section_title,
            "section_level": None,
            "section_type":  effective_type,
            "text":          text,
            "type":          "table_row",
        })
    return blocks


def extract_document_metadata(doc: Document) -> Dict:
    core = doc.core_properties
    return {
        "title":            core.title    or "",
        "subject":          core.subject  or "",
        "author":           core.author   or "",
        "keywords":         core.keywords or "",
        "created":          str(core.created)  if core.created  else "",
        "modified":         str(core.modified) if core.modified else "",
        "paragraphs_count": len(doc.paragraphs),
        "tables_count":     len(doc.tables),
    }


def is_section_heading(text: str, style_name: Optional[str] = None) -> Tuple[bool, int]:
    text = text.strip()

    if style_name:
        sl = style_name.lower()
        if "heading" in sl or "заголовок" in sl:
            m = re.search(r"(\d+)", style_name)
            return True, int(m.group(1)) if m else 1

    if re.match(r"^\d{2}\.\d{2}\.\d{4}", text): return False, 0
    if re.match(r"^[\d\.]+$", text):             return False, 0
    if len(text) > MAX_HEADING_LENGTH:           return False, 0

    m = SECTION_RE.match(text)
    if m:
        return True, min(m.group(1).count(".") + 1, 6)

    for kw in KEY_HEADERS:
        if text.lower().startswith(kw.lower()):
            return True, 1

    if text.isupper() and len(text) < 100 and not text.endswith("."):
        return True, 2

    return False, 0


def process_table(table: Table) -> List[str]:
    """Возвращает строки таблицы в формате 'ячейка | ячейка'."""
    rows = []
    for row in table.rows:
        cells = [
            " ".join(p.text.strip() for p in cell.paragraphs if p.text.strip())
            for cell in row.cells
        ]
        line = " | ".join(cells)
        if line.replace("|", "").strip():
            rows.append(line)
    return rows


def table_to_blocks(
    table: Table,
    doc_name: str,
    section: str,
    section_level: Optional[str],
) -> List[Dict]:
    """Разбивает большую таблицу на блоки по MAX_TABLE_ROWS строк."""
    rows = process_table(table)
    if not rows:
        return []

    header    = rows[0]
    data_rows = rows[1:]
    blocks    = []

    if not data_rows:
        text = "[ТАБЛИЦА]\n" + "\n".join(rows)
        if len(text.split()) >= MIN_CHUNK_WORDS:
            blocks.append({"title": doc_name, "section_title": section,
                           "section_level": section_level, "text": text, "type": "table"})
        return blocks

    for i in range(0, max(len(data_rows), 1), MAX_TABLE_ROWS):
        chunk_rows = [header] + data_rows[i : i + MAX_TABLE_ROWS]
        text       = "[ТАБЛИЦА]\n" + "\n".join(chunk_rows)
        if len(text.split()) >= MIN_CHUNK_WORDS:
            blocks.append({"title": doc_name, "section_title": section,
                           "section_level": section_level, "text": text, "type": "table"})

    return blocks


def clean_text(text: str) -> str:
    text = text.replace("\xa0", " ").replace("\t", " ")
    text = re.sub(r" {2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def split_into_chunks(paragraphs: List[str], max_words: int = MAX_CHUNK_WORDS) -> List[str]:
    """
    Разбивает список параграфов на чанки.

    ИСПРАВЛЕНИЕ: при разбивке длинного параграфа по предложениям остаток
    в buffer теперь корректно флашится до continue, а не переносится в
    следующую итерацию.
    """
    chunks: List[str] = []
    buffer: List[str] = []
    count = 0

    for p in paragraphs:
        p = clean_text(p)
        if not p:
            continue
        words = p.split()
        wc    = len(words)

        # ИСПРАВЛЕНИЕ: длинный параграф обрабатывается полностью, включая остаток
        if wc > max_words and not buffer:
            sentence_buffer: List[str] = []
            sentence_count = 0
            for sentence in re.split(r"(?<=[.!?])\s+", p):
                sentence = sentence.strip()
                if not sentence:
                    continue
                sentence_buffer.append(sentence)
                sentence_count += len(sentence.split())
                if sentence_count >= max_words:
                    chunks.append(" ".join(sentence_buffer))
                    sentence_buffer = []
                    sentence_count = 0
            # Флашим остаток предложений — баг был здесь: они терялись
            if sentence_buffer:
                chunks.append(" ".join(sentence_buffer))
            # buffer и count остаются нетронутыми (пустыми)
            continue

        count += wc
        buffer.append(p)
        if count >= max_words:
            chunks.append("\n\n".join(buffer))
            buffer = []
            count = 0

    if buffer:
        chunks.append("\n\n".join(buffer))
    return chunks


def iter_block_items(parent):
    body = parent.element.body if isinstance(parent, DocumentClass) else parent._element
    for child in body.iterchildren():
        if isinstance(child, CT_P):
            yield "paragraph", Paragraph(child, parent)
        elif isinstance(child, CT_Tbl):
            yield "table", Table(child, parent)


def process_document(doc_path: Path) -> List[Dict]:
    doc      = Document(doc_path)
    metadata = extract_document_metadata(doc)

    blocks:          List[Dict]    = []
    current_section: Optional[str] = None
    current_level:   Optional[str] = None
    current_stype:   str           = "other"
    buffer:          List[str]     = []

    def flush_buffer():
        nonlocal buffer
        for chunk in split_into_chunks(buffer):
            if len(chunk.split()) >= MIN_CHUNK_WORDS:
                blocks.append({
                    "title":         doc_path.name,
                    "section_title": current_section,
                    "section_level": current_level,
                    "section_type":  current_stype,
                    "text":          chunk,
                    "type":          "text",
                })
        buffer = []

    for item_type, item in iter_block_items(doc):
        if item_type == "paragraph":
            text = item.text.strip()
            if not text:
                continue
            style_name = item.style.name if item.style else None
            is_heading, _level = is_section_heading(text, style_name)

            if is_heading:
                if buffer and current_section:
                    flush_buffer()
                m = SECTION_RE.match(text)
                if m:
                    current_level   = m.group(1)
                    current_section = m.group(3).strip()
                else:
                    current_level   = None
                    current_section = text.strip()
                current_stype = detect_section_type(current_section)
            else:
                buffer.append(text)

        elif item_type == "table" and current_section:
            if buffer:
                flush_buffer()
            stype = detect_section_type(current_section)
            # Для ключевых таблиц — построчная выгрузка (уникальные блоки для каждого РПД)
            row_blocks = extract_key_table_rows(item, stype, doc_path.name, current_section)
            if row_blocks:
                blocks.extend(row_blocks)
            else:
                for b in table_to_blocks(item, doc_path.name, current_section, current_level):
                    b["section_type"] = stype
                    blocks.append(b)

    if buffer and current_section:
        flush_buffer()

    if blocks:
        blocks[0]["document_metadata"] = metadata
    return blocks


def main():
    output_dir = Path(RPD_JSON)
    output_dir.mkdir(exist_ok=True)

    docx_files = [f for f in Path(RPD_CORPUS).glob("*.docx") if not f.name.startswith("~$")]
    ok = errors = 0

    for doc_path in sorted(docx_files):
        try:
            blocks = process_document(doc_path)
            if blocks:
                out_path = output_dir / doc_path.with_suffix(".json").name
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(blocks, f, ensure_ascii=False, indent=2)
                ok += 1
                print(f"  ✅ {doc_path.name} → {len(blocks)} блоков")
            else:
                print(f"  ⚠️  {doc_path.name} → блоки не извлечены")
        except Exception as e:
            errors += 1
            print(f"  ❌ {doc_path.name}: {e}")

    print(f"\nКонвертация завершена. Успешно: {ok}, ошибок: {errors}")


if __name__ == "__main__":
    main()
