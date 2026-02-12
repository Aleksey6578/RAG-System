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

# Структура РПД с обязательными разделами
RPD_STRUCTURE = [
    "Введение",
    "Цели",
    "Задачи",
    "Темы",
    "Заключение"
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
    # Поиск примеров для каждого типа раздела
    query_text = f"{discipline}. Уровень: {level}. Раздел: {section_title}"
    q_emb = embed_query(query_text)

    client = QdrantClient(QDRANT_URL, check_compatibility=False)

    # Определение возможных названий разделов в коллекции для текущего типа
    possible_titles = []
    if section_title == "Введение":
        possible_titles = ["Введение", "Аннотация", "Общие положения"]
    elif section_title == "Цели":
        possible_titles = ["Цели дисциплины", "Цель дисциплины", "Цели освоения дисциплины"]
    elif section_title == "Задачи":
        possible_titles = ["Задачи дисциплины", "Задачи освоения дисциплины", "Формируемые компетенции"]
    elif section_title == "Темы":
        possible_titles = ["Содержание дисциплины", "Тематический план", "Разделы дисциплины"]
    elif section_title == "Заключение":
        possible_titles = ["Заключение", "Заключительные положения", "Выводы"]

    # Поиск по любому из возможных названий
    hits = []
    for title in possible_titles:
        flt = Filter(
            must=[
                FieldCondition(
                    key="section_title",
                    match=MatchValue(value=title)
                )
            ]
        )

        current_hits = client.search(
            collection_name=COLLECTION,
            query_vector=q_emb,
            limit=TOP_K//len(possible_titles) or 1,  # Распределение лимита между разными названиями
            with_payload=True,
            query_filter=flt
        )
        hits.extend(current_hits)

    # Сортировка результатов по релевантности и ограничение до TOP_K
    hits.sort(key=lambda x: x.score, reverse=True)
    return hits[:TOP_K]


# =====================
# GENERATION
# =====================
def generate_section(title, examples, discipline, level):
    context = ""

    for h in examples:
        text_block = h.payload["text"]
        block = f"\n---\n{text_block}\n"

        if len(context) + len(block) > MAX_CONTEXT:
            break

        context += block

    # Создание специфического промпта для каждого раздела
    if title == "Введение":
        prompt = f"""
Ты — эксперт по разработке рабочих программ дисциплин.

Сформируй раздел "{title}" для рабочей программы дисциплины "{discipline}" уровня "{level}".

Раздел должен содержать:
- Обоснование актуальности дисциплины
- Место дисциплины в структуре ООП
- Краткое описание роли дисциплины в подготовке обучающихся

Используй стиль и структуру примеров.
Не копируй текст дословно.

ПРИМЕРЫ:
{context}

СГЕНЕРИРУЙ ТЕКСТ:
"""
    elif title == "Цели":
        prompt = f"""
Ты — эксперт по разработке рабочих программ дисциплин.

Сформируй раздел "{title}" для рабочей программы дисциплины "{discipline}" уровня "{level}".

Раздел должен содержать:
- Обобщенную цель освоения дисциплины
- Конкретизацию целей в соответствии с формируемыми компетенциями
- Связь с другими дисциплинами образовательной программы

Используй стиль и структуру примеров.
Не копируй текст дословно.

ПРИМЕРЫ:
{context}

СГЕНЕРИРУЙ ТЕКСТ:
"""
    elif title == "Задачи":
        prompt = f"""
Ты — эксперт по разработке рабочих программ дисциплин.

Сформируй раздел "{title}" для рабочей программы дисциплины "{discipline}" уровня "{level}".

Раздел должен содержать:
- Задачи освоения дисциплины, направленные на достижение целей
- Перечень формируемых общекультурных и профессиональных компетенций
- Связь задач с планируемыми результатами обучения

Используй стиль и структуру примеров.
Не копируй текст дословно.

ПРИМЕРЫ:
{context}

СГЕНЕРИРУЙ ТЕКСТ:
"""
    elif title == "Темы":
        prompt = f"""
Ты — эксперт по разработке рабочих программ дисциплин.

Сформируй раздел "{title}" для рабочей программы дисциплины "{discipline}" уровня "{level}".

Раздел должен содержать:
- Тематический план дисциплины
- Перечень разделов и тем с кратким описанием
- Объем в часах для каждой темы
- Формы текущего контроля по темам

Используй стиль и структуру примеров.
Не копируй текст дословно.

ПРИМЕРЫ:
{context}

СГЕНЕРИРУЙ ТЕКСТ:
"""
    elif title == "Заключение":
        prompt = f"""
Ты — эксперт по разработке рабочих программ дисциплин.

Сформируй раздел "{title}" для рабочей программы дисциплины "{discipline}" уровня "{level}".

Раздел должен содержать:
- Выводы о достижении целей дисциплины
- Оценку соответствия достигнутых результатов планируемым
- Рекомендации по совершенствованию рабочей программы

Используй стиль и структуру примеров.
Не копируй текст дословно.

ПРИМЕРЫ:
{context}

СГЕНЕРИРУЙ ТЕКСТ:
"""
    else:
        # Для других типов разделов используем общий промпт
        prompt = f"""
Ты — эксперт по разработке рабочих программ дисциплин.

Сформируй раздел: "{title}" для дисциплины "{discipline}" уровня "{level}".

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

    # Генерация всех обязательных разделов РПД
    for sec_title in RPD_STRUCTURE:
        print(f"Генерация раздела: {sec_title}")

        examples = retrieve(sec_title, discipline, level)
        text = generate_section(sec_title, examples, discipline, level)

        result.append(f"\n## {sec_title}\n{text}\n")

    # Добавление заголовка РПД
    header = f"# Рабочая программа дисциплины\n## {discipline}\n## Уровень: {level}\n\n"
    
    with open("output_rpd.txt", "w", encoding="utf-8") as f:
        f.write(header + "\n".join(result))

    print("Готово. Файл: output_rpd.txt")


if __name__ == "__main__":
    import sys
    main(sys.argv[1])
