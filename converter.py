from docx import Document
from docx.document import Document as DocumentClass
from docx.table import Table
from docx.oxml.text.paragraph import CT_P
from docx.oxml.table import CT_Tbl
import json
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple

RPD_CORPUS = "rpd_corpus"
RPD_JSON = "rpd_json"
MAX_CHUNK_WORDS = 180
MIN_CHUNK_WORDS = 20
MAX_HEADING_LENGTH = 300
SECTION_REGEX = r'^(\d{1,2}(\.\d{1,2}){0,3})(?:[.\s]+)(.+)$'
KEY_HEADERS = [
    "Цели дисциплины", "Формируемые компетенции",
    "Результаты обучения", "Содержание дисциплины"
]

SECTION_RE = re.compile(SECTION_REGEX)


def extract_document_metadata(doc: Document) -> Dict:
    core_props = doc.core_properties
    return {
        "title": core_props.title or "",
        "subject": core_props.subject or "",
        "author": core_props.author or "",
        "keywords": core_props.keywords or "",
        "created": str(core_props.created) if core_props.created else "",
        "modified": str(core_props.modified) if core_props.modified else "",
        "paragraphs_count": len(doc.paragraphs),
        "tables_count": len(doc.tables)
    }


def is_section_heading(text: str, style_name: Optional[str] = None) -> Tuple[bool, int]:
    text = text.strip()
    
    if style_name:
        style_lower = style_name.lower()
        if 'heading' in style_lower or 'заголовок' in style_lower:
            level_match = re.search(r'(\d+)', style_name)
            if level_match:
                return True, int(level_match.group(1))
            return True, 1
    
    if re.match(r'^\d{2}\.\d{2}\.\d{4}', text):
        return False, 0
    
    if re.match(r'^[\d\.]+$', text):
        return False, 0
    
    if len(text) > MAX_HEADING_LENGTH:
        return False, 0
    
    m = SECTION_RE.match(text)
    if m:
        number = m.group(1)
        level = number.count('.') + 1
        return True, min(level, 6)
    
    text_lower = text.lower()
    for keyword in KEY_HEADERS:
        if text_lower.startswith(keyword.lower()):
            return True, 1
    
    if text.isupper() and len(text) < 100 and not text.endswith('.'):
        return True, 2
    
    return False, 0


def process_table(table: Table) -> str:
    rows_text = []
    
    for row in table.rows:
        cells_text = []
        for cell in row.cells:
            cell_text = " ".join(p.text.strip() for p in cell.paragraphs if p.text.strip())
            cells_text.append(cell_text)
        
        if any(cells_text):
            rows_text.append(" | ".join(cells_text))
    
    return "\n".join(rows_text)


def clean_text(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = text.replace("\t", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def split_into_chunks(paragraphs: List[str], max_words: int = None) -> List[str]:
    if max_words is None:
        max_words = MAX_CHUNK_WORDS
    
    chunks = []
    buffer = []
    count = 0

    for p in paragraphs:
        p = clean_text(p)
        if not p:
            continue

        words = p.split()
        word_count = len(words)
        
        if word_count > max_words and not buffer:
            sentences = re.split(r'[.!?]\s+', p)
            for sentence in sentences:
                sentence = sentence.strip()
                if not sentence:
                    continue
                
                sent_words = sentence.split()
                count += len(sent_words)
                buffer.append(sentence + ".")
                
                if count >= max_words:
                    chunks.append(" ".join(buffer))
                    buffer = []
                    count = 0
            continue
        
        count += word_count
        buffer.append(p)

        if count >= max_words:
            chunks.append("\n\n".join(buffer))
            buffer = []
            count = 0

    if buffer:
        chunks.append("\n\n".join(buffer))

    return chunks


def iter_block_items(parent):
    from docx.oxml.xmlchemy import BaseOxmlElement
    
    if isinstance(parent, DocumentClass):
        parent_elm = parent.element.body
    else:
        parent_elm = parent._element

    for child in parent_elm.iterchildren():
        if isinstance(child, CT_P):
            yield 'paragraph', child
        elif isinstance(child, CT_Tbl):
            yield 'table', child


def process_document(doc_path: Path) -> List[Dict]:
    doc = Document(doc_path)
    metadata = extract_document_metadata(doc)
    
    blocks = []
    current_section = None
    current_level = None
    buffer = []
    
    table_counter = 0
    para_counter = 0
    
    for item_type, item in iter_block_items(doc):
        if item_type == 'paragraph':
            if para_counter < len(doc.paragraphs):
                para = doc.paragraphs[para_counter]
                para_counter += 1
            else:
                continue
            
            text = para.text.strip()
            
            if not text:
                continue
            
            style_name = para.style.name if para.style else None
            is_heading, level = is_section_heading(text, style_name)
            
            if is_heading:
                if buffer and current_section:
                    for chunk in split_into_chunks(buffer):
                        if len(chunk.split()) >= MIN_CHUNK_WORDS:
                            blocks.append({
                                "title": doc_path.name,
                                "section_title": current_section,
                                "section_level": current_level,
                                "level": level,
                                "text": chunk,
                                "type": "text"
                            })
                
                buffer = []
                
                m = SECTION_RE.match(text)
                if m:
                    current_level = m.group(1)
                    current_section = m.group(3).strip()
                else:
                    current_level = None
                    current_section = text.strip()
            else:
                buffer.append(text)
        
        elif item_type == 'table':
            if table_counter < len(doc.tables):
                table = doc.tables[table_counter]
                table_counter += 1
                
                table_text = process_table(table)
                
                if table_text and current_section:
                    blocks.append({
                        "title": doc_path.name,
                        "section_title": current_section,
                        "section_level": current_level,
                        "text": f"[ТАБЛИЦА]\n{table_text}",
                        "type": "table"
                    })
    
    if buffer and current_section:
        for chunk in split_into_chunks(buffer):
            if len(chunk.split()) >= MIN_CHUNK_WORDS:
                blocks.append({
                    "title": doc_path.name,
                    "section_title": current_section,
                    "section_level": current_level,
                    "text": chunk,
                    "type": "text"
                })
    
    if blocks:
        blocks[0]["document_metadata"] = metadata
    
    return blocks


def main():
    output_dir = Path(RPD_JSON)
    output_dir.mkdir(exist_ok=True)
    
    corpus_dir = Path(RPD_CORPUS)
    docx_files = list(corpus_dir.glob("*.docx"))
    docx_files = [f for f in docx_files if not f.name.startswith("~$")]
    
    for doc_path in docx_files:
        blocks = process_document(doc_path)
        
        if blocks:
            output_path = output_dir / doc_path.with_suffix('.json').name
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(blocks, f, ensure_ascii=False, indent=2)
    
    print("Конвертация завершена")


if __name__ == "__main__":
    main()
