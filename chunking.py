import json
import os
import hashlib

INPUT_FILE = "data_clean.jsonl"
OUTPUT_FILE = "chunks.jsonl"

MAX_TOKENS = 300      # ориентировочно 300 слов
OVERLAP = 50          # перекрытие 50 слов


def split_into_chunks(text, max_tokens=300, overlap=50):
    words = text.split()
    chunks = []

    start = 0
    while start < len(words):
        end = start + max_tokens
        chunk_words = words[start:end]
        chunk = " ".join(chunk_words)
        chunks.append(chunk)

        start += max_tokens - overlap

    return chunks


def generate_doc_id(source):
    return hashlib.md5(source.encode()).hexdigest()


def main():
    with open(INPUT_FILE, encoding="utf-8") as f:
        records = [json.loads(line) for line in f]

    chunks_out = []
    global_chunk_id = 0

    for record in records:
        text = record["text"]
        source = record["source"]
        section_title = record.get("section_title")
        section_level = record.get("section_level")

        doc_id = generate_doc_id(source)

        section_chunks = split_into_chunks(text, MAX_TOKENS, OVERLAP)

        for idx, chunk in enumerate(section_chunks):

            if len(chunk.split()) < 30:
                continue

            chunks_out.append({
                "id": global_chunk_id,
                "doc_id": doc_id,
                "chunk_index": idx,
                "source": source,
                "section_title": section_title,
                "section_level": section_level,
                "text": chunk
            })

            global_chunk_id += 1

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for c in chunks_out:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")

    print(f"Готово. Создано {len(chunks_out)} чанков.")


if __name__ == "__main__":
    main()
