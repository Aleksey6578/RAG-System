"""
book_loader.py — загрузка учебников из rpd_books/ в pipeline.

Два режима (управляются флагами):
  --meta-only   только обновить config.json (библиография T15)
  --full        meta + чанкинг + загрузка в Qdrant            [default]

Поддерживаемые форматы: .pdf, .docx
"""

import argparse
import json
import re
from pathlib import Path

# ── Зависимости ──────────────────────────────────────────────────────────────
try:
    import fitz  # PyMuPDF — для PDF
except ImportError:
    fitz = None

from docx import Document

# ── Константы ────────────────────────────────────────────────────────────────
BOOKS_DIR   = Path("rpd_books")
CONFIG_PATH = Path("config.json")
CHUNK_TOKENS = 300   # целевой размер чанка для книжного контента
OVERLAP_TOKENS = 50

# ── Извлечение метаданных ─────────────────────────────────────────────────────
_BIBLIO_RE = re.compile(
    r"(?P<authors>[А-ЯA-Z][^.]+?)\.\s+"      # Фамилия И. О.
    r"(?P<title>[^/]+?)\s*/\s*"               # Название /
    r"(?P<rest>.+?)\.\s*—\s*"
    r"(?P<city>[^:]+?)\s*:\s*"
    r"(?P<publisher>[^,]+),\s*"
    r"(?P<year>\d{4})",
    re.DOTALL,
)

def extract_metadata_from_filename(path: Path) -> dict:
    """
    Пытается распарсить ГОСТ-описание из имени файла.
    Формат: 'Фамилия И.О. Название. Город, Год.pdf'
    Если не распарсить — возвращает минимальный dict с path.stem как desc.
    """
    stem = path.stem
    m = _BIBLIO_RE.search(stem)
    if m:
        desc = (
            f"{m['authors']}. {m['title'].strip()} — "
            f"{m['city'].strip()} : {m['publisher'].strip()}, {m['year']}."
        )
        return {"desc": desc, "year": m["year"], "raw": True}
    return {"desc": stem, "year": "", "raw": False}


def extract_metadata_from_docx(path: Path) -> dict:
    """Читает первые ~500 символов DOCX как потенциальный титульный лист."""
    doc = Document(path)
    text = "\n".join(p.text for p in doc.paragraphs[:20] if p.text.strip())
    m = _BIBLIO_RE.search(text)
    if m:
        desc = (
            f"{m['authors']}. {m['title'].strip()} — "
            f"{m['city'].strip()} : {m['publisher'].strip()}, {m['year']}."
        )
        return {"desc": desc, "year": m["year"], "raw": True}
    return extract_metadata_from_filename(path)


def extract_metadata_from_pdf(path: Path) -> dict:
    """Читает первую страницу PDF."""
    if fitz is None:
        return extract_metadata_from_filename(path)
    doc = fitz.open(str(path))
    text = doc[0].get_text() if len(doc) > 0 else ""
    doc.close()
    m = _BIBLIO_RE.search(text)
    if m:
        desc = (
            f"{m['authors']}. {m['title'].strip()} — "
            f"{m['city'].strip()} : {m['publisher'].strip()}, {m['year']}."
        )
        return {"desc": desc, "year": m["year"], "raw": True}
    return extract_metadata_from_filename(path)


def load_book_metadata(path: Path) -> dict:
    ext = path.suffix.lower()
    if ext == ".pdf":
        meta = extract_metadata_from_pdf(path)
    elif ext == ".docx":
        meta = extract_metadata_from_docx(path)
    else:
        meta = extract_metadata_from_filename(path)
    meta["source_file"] = str(path)
    return meta


# ── Извлечение текста ─────────────────────────────────────────────────────────
def extract_text_pdf(path: Path) -> str:
    if fitz is None:
        raise RuntimeError("PyMuPDF не установлен: pip install pymupdf")
    doc = fitz.open(str(path))
    pages = [page.get_text() for page in doc]
    doc.close()
    return "\n".join(pages)


def extract_text_docx(path: Path) -> str:
    doc = Document(path)
    return "\n".join(p.text for p in doc.paragraphs if p.text.strip())


def extract_text(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == ".pdf":
        return extract_text_pdf(path)
    elif ext == ".docx":
        return extract_text_docx(path)
    return ""


# ── Простой токен-аппроксимированный чанкинг ─────────────────────────────────
def _approx_tokens(text: str) -> int:
    return len(text) // 4  # ~4 символа/токен для русского

def chunk_text(text: str, source_file: str) -> list[dict]:
    """
    Разбивает текст книги на чанки с stype='book_content'.
    Эти чанки попадут в Qdrant и будут доступны для retrieval
    при генерации тем ЛР/ПЗ.
    """
    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    chunks = []
    buf, buf_tokens, idx = [], 0, 0

    for para in paragraphs:
        t = _approx_tokens(para)
        if buf_tokens + t > CHUNK_TOKENS and buf:
            chunks.append({
                "id":          f"{Path(source_file).stem}_chunk_{idx}",
                "text":        " ".join(buf),
                "stype":       "book_content",
                "source_file": source_file,
            })
            # overlap: оставляем последние OVERLAP_TOKENS
            # [FIX-OVL] добавляем параграф ДО проверки порога — иначе
            # сентенс, вызвавший break, не попадал в overlap_buf.
            overlap_buf, overlap_tok = [], 0
            for sent in reversed(buf):
                overlap_buf.insert(0, sent)
                overlap_tok += _approx_tokens(sent)
                if overlap_tok >= OVERLAP_TOKENS:
                    break
            buf, buf_tokens = overlap_buf, sum(_approx_tokens(s) for s in overlap_buf)
            idx += 1
        buf.append(para)
        buf_tokens += t

    if buf:
        chunks.append({
            "id":          f"{Path(source_file).stem}_chunk_{idx}",
            "text":        " ".join(buf),
            "stype":       "book_content",
            "source_file": source_file,
        })
    return chunks


# ── Обновление config.json ────────────────────────────────────────────────────
def build_biblio_entry(meta: dict, btype: str = "Основная литература") -> dict:
    return {
        "type":    btype,
        "purpose": "Для изучения теории;Для выполнения СРО;",
        "desc":    meta["desc"],
        "url":     "http://bibl.rusoil.net",
        "coeff":   "1.00",
    }


def update_config(entries: list[dict]):
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    cfg["main_bibliography"] = entries
    CONFIG_PATH.write_text(
        json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"✅ config.json обновлён: {len(entries)} записей в main_bibliography")


# ── Загрузка в Qdrant ─────────────────────────────────────────────────────────
def load_chunks_to_qdrant(all_chunks: list[dict]):
    """
    Переиспользует логику load_qdrant.py.
    Импортируем функцию embed_and_upsert если она вынесена,
    иначе дублируем минимальный upsert.
    """
    from qdrant_client import QdrantClient
    from qdrant_client.models import PointStruct
    import requests

    QDRANT_URL    = "http://localhost:6333"
    COLLECTION    = "rpd_rag"
    OLLAMA_URL    = "http://localhost:11434/api/embeddings"
    EMBED_MODEL   = "bge-m3"

    client = QdrantClient(url=QDRANT_URL)

    def embed(text: str) -> list[float]:
        r = requests.post(OLLAMA_URL, json={"model": EMBED_MODEL, "prompt": text[:4000]})
        r.raise_for_status()
        return r.json()["embedding"]

    points = []
    for i, chunk in enumerate(all_chunks):
        vec = embed(chunk["text"])
        points.append(PointStruct(
            # [FIX-HASH] hash() недетерминирован между процессами (PYTHONHASHSEED).
            # hashlib.sha256 стабилен → upsert работает корректно при перезапуске.
            id=int(hashlib.sha256(chunk["id"].encode()).hexdigest()[:15], 16),
            vector=vec,
            payload={
                "text":         chunk["text"],
                # [FIX-PAYLOAD] rpd_generate.py фильтрует по "section_type"
                # (верхний уровень payload, аналогично load_qdrant.py [S]).
                # "stype" игнорируется фильтром → чанки книг никогда не
                # попадали бы в retrieval для lab_works/practice.
                "section_type": chunk["stype"],
                "source_file":  chunk["source_file"],
                "chunk_id":     chunk["id"],
                # Обратная совместимость с metadata-фильтром
                "metadata":     {"section_type": chunk["stype"]},
            }
        ))
        if (i + 1) % 10 == 0:
            print(f"  embedded {i+1}/{len(all_chunks)}")

    client.upsert(collection_name=COLLECTION, points=points)
    print(f"✅ Загружено в Qdrant: {len(points)} чанков (stype=book_content)")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Загрузка книг из rpd_books/")
    parser.add_argument("--meta-only", action="store_true",
                        help="Только обновить config.json (без Qdrant)")
    args = parser.parse_args()

    books = sorted(BOOKS_DIR.glob("*.*"))
    books = [b for b in books if b.suffix.lower() in (".pdf", ".docx")]

    if not books:
        print(f"⚠️  Папка {BOOKS_DIR} пуста или не найдена")
        return

    print(f"📚 Найдено книг: {len(books)}")

    all_entries = []
    all_chunks  = []

    for path in books:
        print(f"  → {path.name}")
        meta = load_book_metadata(path)
        entry = build_biblio_entry(meta)
        all_entries.append(entry)

        if not args.meta_only:
            text = extract_text(path)
            chunks = chunk_text(text, str(path))
            all_chunks.extend(chunks)
            print(f"     {len(chunks)} чанков")

    # 1. config.json
    update_config(all_entries)

    # 2. Qdrant
    if not args.meta_only and all_chunks:
        load_chunks_to_qdrant(all_chunks)


if __name__ == "__main__":
    main()