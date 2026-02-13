import json
import requests
import re
import sys
import os
from typing import List, Dict
from qdrant_client import QdrantClient

QDRANT = {
    "url": "http://localhost:6333",
    "collection": "rpd_rag"
}
OLLAMA = {
    "embed_url": "http://localhost:11434/api/embeddings",
    "generate_url": "http://localhost:11434/api/generate",
    "embed_model": "bge-m3",
    "llm_model": "qwen2.5:3b"
}
GENERATION = {
    "top_k": 2,
    "sections": [
        "Цели дисциплины",
        "Формируемые компетенции",
        "Результаты обучения",
        "Содержание дисциплины",
        "Фонд оценочных средств"
    ]
}
PATHS = {
    "output_rpd": "output_rpd.txt"
}

EMBEDDING_CACHE = {}


def clean_text_simple(text: str) -> str:
    text = re.sub(r' +', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    return '\n'.join(lines).strip()


def get_embedding(text: str) -> List[float]:
    if text in EMBEDDING_CACHE:
        return EMBEDDING_CACHE[text]
    
    response = requests.post(
        OLLAMA["embed_url"],
        json={"model": OLLAMA["embed_model"], "prompt": f"query: {text}"},
        timeout=30
    )
    data = response.json()
    embedding = data.get("embedding") or data["data"][0]["embedding"]
    EMBEDDING_CACHE[text] = embedding
    return embedding


def retrieve_examples(section_title: str, discipline: str) -> List[Dict]:
    query = f"{section_title} {discipline}"
    
    q_emb = get_embedding(query)
    client = QdrantClient(url=QDRANT["url"])
    results = client.query_points(
        collection_name=QDRANT["collection"],
        query=q_emb,
        limit=2,
        with_payload=True
    )
    
    examples = []
    for point in results.points:
        examples.append({
            'text': point.payload.get('text', ''),
            'section': point.payload.get('section_title', '')
        })
    
    return examples


def build_context_minimal(examples: List[Dict]) -> str:
    if not examples:
        return ""
    
    text = examples[0]['text']
    if len(text) > 400:
        text = text[:400] + "..."
    
    return f"Пример:\n{text}"


SIMPLE_TEMPLATES = {
    "Цели дисциплины": """Напиши раздел "Цели дисциплины" для РПД по дисциплине "{discipline}".

Структура:
1. Главная цель (1-2 предложения)
2. Основные задачи (3-5 пунктов)

Пиши кратко, академическим стилем, только на русском языке.""",

    "Формируемые компетенции": """Напиши раздел "Формируемые компетенции" для дисциплины "{discipline}".

Укажи:
- Универсальные компетенции (УК): 1-2 компетенции
- Общепрофессиональные (ОПК): 2-3 компетенции  
- Профессиональные (ПК): 1-2 компетенции

Каждая с кодом (например УК-1) и описанием.""",

    "Результаты обучения": """Напиши раздел "Результаты обучения" для дисциплины "{discipline}".

Формат:
Знать: (3-4 пункта)
Уметь: (3-4 пункта)
Владеть: (2-3 пункта)

Кратко и конкретно.""",

    "Содержание дисциплины": """Напиши раздел "Содержание дисциплины" для дисциплины "{discipline}".

Структура:
Раздел 1. [название]
  Тема 1.1. [название]
  Тема 1.2. [название]

Раздел 2. [название]
  Тема 2.1. [название]
  Тема 2.2. [название]

2-3 раздела, по 2-3 темы в каждом.""",

    "Фонд оценочных средств": """Напиши раздел "Фонд оценочных средств" для дисциплины "{discipline}".

Укажи:
Текущий контроль:
- Практические работы
- Тестирование
- Контрольные работы

Промежуточная аттестация:
- Экзамен или зачет

Кратко."""
}


def create_lightweight_prompt(section_title: str, discipline: str, context: str) -> str:
    template = SIMPLE_TEMPLATES.get(section_title, "Напиши раздел {section_title} для {discipline}")
    base_prompt = template.format(discipline=discipline, section_title=section_title)
    
    if context and len(context) < 500:
        return f"{base_prompt}\n\n{context}\n\nТеперь создай раздел:"
    else:
        return f"{base_prompt}\n\nСоздай раздел:"


def generate_section_light(section_title: str, discipline: str, context: str) -> str:
    prompt = create_lightweight_prompt(section_title, discipline, context)
    
    response = requests.post(
        OLLAMA["generate_url"],
        json={
            "model": OLLAMA["llm_model"],
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.4,
                "top_p": 0.9,
                "top_k": 40,
                "num_predict": 400,
                "num_ctx": 2048,
            }
        },
        timeout=60
    )
    
    data = response.json()
    generated_text = data.get("response", "")
    
    return clean_text_simple(generated_text)


def main(config_path=None):
    if config_path is None:
        config_path = "config.json" if os.path.exists("config.json") else None
    
    if config_path:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        cfg = {"discipline": "Интеллектуальные системы", "level": "бакалавриат"}
    
    discipline = cfg.get("discipline", "Неизвестная дисциплина")
    level = cfg.get("level", "бакалавриат")
    
    result_sections = []
    
    for section_title in GENERATION["sections"]:
        examples = retrieve_examples(section_title, discipline)
        context = build_context_minimal(examples)
        text = generate_section_light(section_title, discipline, context)
        
        result_sections.append({
            "title": section_title,
            "text": text
        })
    
    output_lines = [
        "РАБОЧАЯ ПРОГРАММА ДИСЦИПЛИНЫ",
        "",
        f"Дисциплина: {discipline}",
        f"Уровень: {level}",
        "",
        "="*60,
        ""
    ]
    
    for section in result_sections:
        output_lines.append(f"\n## {section['title']}\n")
        output_lines.append(section['text'])
        output_lines.append("\n")
    
    with open(PATHS["output_rpd"], "w", encoding="utf-8") as f:
        f.write("\n".join(output_lines))
    
    print(f"Готово. Файл: {PATHS['output_rpd']}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        main(None)
    else:
        main(sys.argv[1])
