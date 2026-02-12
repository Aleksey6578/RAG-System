import json
import requests
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

# =====================
# CONFIG
# =====================
QDRANT_URL = "http://localhost:6333"
COLLECTION = "rpd_rag"

OLLAMA_EMBED = "http://localhost:11434/api/embeddings"
OLLAMA_GEN = "http://localhost:11434/api/generate"

EMBED_MODEL = "bge-m3"
LLM_MODEL = "mistral:latest"

TOP_K = 5
MAX_CONTEXT = 6000

SECTIONS = [
    "Цели дисциплины",
    "Формируемые компетенции",
    "Результаты обучения",
    "Содержание дисциплины",
    "Фонд оценочных средств"
]


# =====================
# EMBEDDING (QUERY MODE)
# =====================
def embed_query(text: str):
    text = f"query: {text}"

    r = requests.post(
        OLLAMA_EMBED,
        json={"model": EMBED_MODEL, "prompt": text},
        timeout=60
    )

    r.raise_for_status()
    data = r.json()
    return data.get("embedding") or data["data"][0]["embedding"]


# =====================
# RETRIEVAL
# =====================
def retrieve(section_title: str, discipline: str, level: str):
    query_text = f"{discipline}. Уровень: {level}. Раздел: {section_title}"
    q_emb = embed_query(query_text)

    client = QdrantClient(QDRANT_URL, check_compatibility=False)

    flt = Filter(
        must=[
            FieldCondition(
                key="section_title",
                match=MatchValue(value=section_title)
            )
        ]
    )

    hits = client.search(
        collection_name=COLLECTION,
        query_vector=q_emb,
        limit=TOP_K,
        with_payload=True,
        query_filter=flt
    )

    return hits


# =====================
# GENERATION
# =====================
def generate_section(title, examples):
    context = ""

    for h in examples:
        text_block = h.payload["text"]
        block = f"\n---\n{text_block}\n"

        if len(context) + len(block) > MAX_CONTEXT:
            break

        context += block

    prompt = f"""
Ты — эксперт по разработке рабочих программ дисциплин.

Сформируй раздел: "{title}".

Используй стиль и структуру примеров.
Не копируй текст дословно.

ПРИМЕРЫ:
{context}

СГЕНЕРИРУЙ ТЕКСТ:
"""

    r = requests.post(
        OLLAMA_GEN,
        json={
            "model": LLM_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.2
            }
        },
        timeout=300
    )

    r.raise_for_status()
    return r.json().get("response", "Ошибка генерации")


# =====================
# MAIN
# =====================
def main(config_path):
    with open(config_path, encoding="utf-8") as f:
        cfg = json.load(f)

    discipline = cfg["discipline"]
    level = cfg["level"]

    result = []

    for sec_title in SECTIONS:
        print(f"Генерация раздела: {sec_title}")

        examples = retrieve(sec_title, discipline, level)
        text = generate_section(sec_title, examples)

        result.append(f"\n## {sec_title}\n{text}\n")

    with open("output_rpd.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(result))

    print("Готово. Файл: output_rpd.txt")


if __name__ == "__main__":
    import sys
    main(sys.argv[1])
