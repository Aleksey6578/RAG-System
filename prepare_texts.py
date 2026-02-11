import os
import re
import json
import unicodedata

DATA_DIR = "rpd_json"
OUTPUT_FILE = "data_clean.jsonl"


def clean_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\x00", "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)

    lines = [line.strip() for line in text.split("\n") if line.strip()]
    return "\n".join(lines).strip()


def process_record(record, out_file, source):
    if "text" not in record:
        return

    cleaned = clean_text(record["text"])
    if not cleaned:
        return

    out_file.write(json.dumps({
        "source": source,
        "title": record.get("title"),
        "section_title": record.get("section_title"),
        "section_level": record.get("section_level"),
        "text": cleaned
    }, ensure_ascii=False) + "\n")


def process_file(path, out_file):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    source = os.path.basename(path)

    if isinstance(data, list):
        for r in data:
            process_record(r, out_file, source)


def main():
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for fn in sorted(os.listdir(DATA_DIR)):
            if fn.endswith(".json"):
                process_file(os.path.join(DATA_DIR, fn), out)

    print(f"Готово. Сохранено в {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
