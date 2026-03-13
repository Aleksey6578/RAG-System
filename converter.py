"""
converter.py — конвертация DOCX-файлов РПД в JSON-блоки.

Исправления v3:
  - [E] Структурированные таблицы: process_table() теперь возвращает
    {"headers": [...], "rows": [[...]]} вместо pipe-текста.
    Табличная семантика сохраняется при embedding.
    table_to_text() переводит структуру в читаемый текст для chunking/RAG.
    Старые блоки с type="table" используют table_to_text() как раньше,
    но дополнительно получают поле "table_data" с исходной структурой.
  - [1] БАГ: flush_buffer(level) вызывался с уровнем НОВОЙ секции вместо
    текущей. Теперь уровень сохраняется в current_level.
  - [2] БАГ: split_into_chunks — при разбивке длинного параграфа по
    предложениям buffer/count не сбрасывались после continue. Исправлено.
  - [3] Длинные таблицы (> MAX_TABLE_ROWS строк) разбиваются на блоки.
  - [4] Глобальный try/except: один сломанный файл не останавливает конвертацию.
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

SECTION_TYPE_MAP = [
    (["цел", "задач"],                                              "goals"),
    (["компетенц"],                                                 "competencies"),
    (["доступн", "инвалид", "огранич", "здоровь", "овз"],         "accessibility"),
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
    if not section_title:
        return "other"
    t = section_title.lower()
    for keywords, stype in SECTION_TYPE_MAP:
        if any(kw in t for kw in keywords):
            return stype
    return "other"


# ---------------------------------------------------------------------------
# [E] Структурированные таблицы
# ---------------------------------------------------------------------------

def process_table(table: Table) -> Dict:
    """
    [E] Возвращает структурированное представление таблицы:
      {"headers": ["Кол.1", "Кол.2"], "rows": [["val1", "val2"], ...]}

    Вместо старого pipe-текста сохраняется реальная структура.
    Это улучшает embedding (заголовки ячеек сохраняются как метаданные)
    и позволяет downstream-коду формировать текст в нужном формате.
    """
    raw_rows = []
    for row in table.rows:
        cells = []
        for cell in row.cells:
            cell_text = " ".join(
                p.text.strip()
                for p in cell.paragraphs
                if p.text.strip()
            )
            cells.append(cell_text)
        # Пропускаем полностью пустые строки
        if any(c.strip() for c in cells):
            raw_rows.append(cells)

    if not raw_rows:
        return {"headers": [], "rows": []}

    return {
        "headers": raw_rows[0],
        "rows":    raw_rows[1:],
    }


def table_to_text(table_data: Dict) -> str:
    """
    [E] Переводит структурированную таблицу в читаемый текст для RAG.

    Формат: заголовок-строка через " | ", затем строки данных.
    Использует реальные заголовки столбцов вместо «ячейка | ячейка».
    """
    headers = table_data.get("headers", [])
    rows    = table_data.get("rows", [])
    if not headers and not rows:
        return ""

    lines = []
    if headers:
        lines.append(" | ".join(str(h) for h in headers))
        lines.append("-" * max(len(lines[0]), 20))

    for row in rows:
        if any(str(c).strip() for c in row):
            lines.append(" | ".join(str(c) for c in row))

    return "\n".join(lines)


def extract_key_table_rows(
    table: Table, section_type: str, doc_name: str, section_title: str
) -> List[Dict]:
    """
    Для ключевых таблиц (компетенции, результаты, содержание, ЛР/ПЗ)
    извлекает каждую строку данных как отдельный блок с section_type.

    [E] Использует структурированный process_table() — каждая строка
    получает заголовки в качестве контекста.
    """
    if section_type not in ("competencies", "learning_outcomes", "content", "assessment"):
        return []

    table_data = process_table(table)
    headers = table_data.get("headers", [])
    rows    = table_data.get("rows", [])

    if not rows:
        return []

    header_line = " | ".join(str(h) for h in headers) if headers else ""
    blocks = []

    for row_cells in rows:
        row_text = " | ".join(str(c) for c in row_cells)
        if not row_text.strip() or len(row_text.split()) < 5:
            continue

        # Текст чанка включает заголовки для контекста
        text = f"{header_line}\n{row_text}" if header_line else row_text

        # Определяем реальный тип строки
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


def table_to_blocks(
    table: Table,
    doc_name: str,
    section: str,
    section_level: Optional[str],
) -> List[Dict]:
    """
    [E] Разбивает большую таблицу на блоки по MAX_TABLE_ROWS строк.
    Каждый блок получает поле "table_data" с исходной структурой.
    """
    table_data = process_table(table)
    headers    = table_data.get("headers", [])
    data_rows  = table_data.get("rows", [])

    if not headers and not data_rows:
        return []

    blocks = []

    if not data_rows:
        text = table_to_text(table_data)
        if len(text.split()) >= MIN_CHUNK_WORDS:
            blocks.append({
                "title":         doc_name,
                "section_title": section,
                "section_level": section_level,
                "text":          "[ТАБЛИЦА]\n" + text,
                "type":          "table",
                "table_data":    table_data,   # [E] структурированная копия
            })
        return blocks

    for i in range(0, max(len(data_rows), 1), MAX_TABLE_ROWS):
        chunk_rows = data_rows[i: i + MAX_TABLE_ROWS]
        chunk_data = {"headers": headers, "rows": chunk_rows}
        text = "[ТАБЛИЦА]\n" + table_to_text(chunk_data)
        if len(text.split()) >= MIN_CHUNK_WORDS:
            blocks.append({
                "title":         doc_name,
                "section_title": section,
                "section_level": section_level,
                "text":          text,
                "type":          "table",
                "table_data":    chunk_data,   # [E]
            })

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
    в sentence_buffer корректно флашится до continue.
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
            if sentence_buffer:
                chunks.append(" ".join(sentence_buffer))
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

            # Для ключевых таблиц — построчная выгрузка [E]
            row_blocks = extract_key_table_rows(item, stype, doc_path.name, current_section)
            if row_blocks:
                blocks.extend(row_blocks)
            else:
                for b in table_to_blocks(item, doc_path.name, current_section, current_level):
                    b["section_type"] = stype
                    blocks.append(b)

    if buffer and current_section:
        flush_buffer()

    # [E] document_metadata хранится отдельно в первом блоке
    # (архитектурно правильнее выносить на уровень документа,
    #  но для обратной совместимости с prepare_texts.py оставляем в blocks[0])
    if blocks:
        blocks[0]["document_metadata"] = metadata

    return blocks


def main():
    output_dir = Path(RPD_JSON)
    output_dir.mkdir(exist_ok=True)

    docx_files = [
        f for f in Path(RPD_CORPUS).glob("*.docx")
        if not f.name.startswith("~$")
    ]
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
