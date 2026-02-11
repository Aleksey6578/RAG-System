from docx import Document
import json
import os
import re

DATA_DIR = "rpd_corpus"
OUTPUT_DIR = "rpd_json"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Заголовки вида 1, 1.2, 3.4.1 ...
SECTION_RE = re.compile(r'^(\d+(\.\d+)*)(?:[.\s]+)(.+)$')

KEYHEADERS = [
    "Аннотация",
    "Введение",
    "Заключение",
    "Содержание",
    "Цели дисциплины",
    "Формируемые компетенции",
    "Результаты обучения",
    "Содержание дисциплины",
    "Фонд оценочных средств"
]


def is_section_heading(text: str) -> bool:
    text = text.strip()

    if SECTION_RE.match(text):
        return True

    for k in KEYHEADERS:
        if text.lower().startswith(k.lower()):
            return True

    return False


def clean_text(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def split_paragraphs_into_chunks(paragraphs, max_words=180):
    chunks = []
    buffer = []
    count = 0

    for p in paragraphs:
        p = clean_text(p)
        if not p:
            continue

        words = p.split()
        count += len(words)
        buffer.append(p)

        if count >= max_words:
            chunks.append(" ".join(buffer))
            buffer = []
            count = 0

    if buffer:
        chunks.append(" ".join(buffer))

    return chunks


for fn in os.listdir(DATA_DIR):
    if not fn.endswith(".docx"):
        continue

    doc_path = os.path.join(DATA_DIR, fn)
    doc = Document(doc_path)

    blocks = []
    buffer = []
    current_section = None
    current_level = None

    for p in doc.paragraphs:
        text = p.text.strip()
        if not text:
            continue

        if is_section_heading(text):

            # сохранить предыдущую секцию
            if buffer and current_section:
                for chunk in split_paragraphs_into_chunks(buffer):
                    if len(chunk.split()) > 20:
                        blocks.append({
                            "title": fn,
                            "section_title": current_section,
                            "section_level": current_level,
                            "text": chunk
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

    # финальный блок
    if buffer and current_section:
        for chunk in split_paragraphs_into_chunks(buffer):
            if len(chunk.split()) > 20:
                blocks.append({
                    "title": fn,
                    "section_title": current_section,
                    "section_level": current_level,
                    "text": chunk
                })

    out_path = os.path.join(OUTPUT_DIR, fn.replace(".docx", ".json"))
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(blocks, f, ensure_ascii=False, indent=2)

print("Готово. DOCX → JSON (со структурой).")
