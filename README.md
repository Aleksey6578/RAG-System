# RAG-Module

## Описание
RAG-модуль для генерации РПД из корпуса DOCX-документов.

### Основной пайплайн (локальный, Ollama)

1. `converter.py` — извлекает блоки из DOCX в `rpd_json/*.json`.
2. `prepare_texts.py` — очищает/дедуплицирует и собирает `data_clean.jsonl`.
3. `chunking.py` — режет на чанки и формирует `chunks.jsonl`.
4. `load_qdrant.py` — строит эмбеддинги (bge-m3) и загружает чанки в Qdrant.
5. `rpd_generate.py` — делает retrieval + LLM-генерацию итогового `output_rpd.docx`.

### RouterAI-вариант (внешний API)

- `load_qdrant_RouterAI.py` — загрузка чанков с эмбеддингами через RouterAI API (qwen3-embedding-4b, 2560-мерные векторы, параллельные запросы).
- `rpd_generate_RouterAI.py` — генерация РПД с LLM и эмбеддингами через RouterAI API. Содержит все исправления по отчёту от 16.04.2026 (FIX-01–10).
- `book_loader_routerai.py` — загрузка учебников из `rpd_books/` в Qdrant с чанкингом и RouterAI-эмбеддингами.

RouterAI-версии взаимозаменяемы с локальными по интерфейсу (`config.json` тот же).

## Требования
- Python 3.10+
- Docker (для Qdrant)
- Ollama (для локального варианта)
- RouterAI API-ключ (для RouterAI-варианта)

## Установка
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск инфраструктуры
### 1) Qdrant
```bash
docker run -d --name qdrant \
  -p 6333:6333 -p 6334:6334 \
  qdrant/qdrant:latest
```

Проверка:
```bash
curl http://localhost:6333/collections
```

### 2) Ollama (локальный вариант)
```bash
ollama pull bge-m3
ollama pull qwen2.5:14b
```

## Полный пайплайн (индексация + генерация)

### Локальный (Ollama)
```bash
python converter.py
python prepare_texts.py
python chunking.py
python load_qdrant.py
python rpd_generate.py config.json
```

### RouterAI
```bash
python converter.py
python prepare_texts.py
python chunking.py
python load_qdrant_RouterAI.py
python rpd_generate_RouterAI.py config.json
```

Загрузка учебников (опционально, оба варианта):
```bash
python book_loader_routerai.py
```

## Конфигурация генерации
Файл `config.json` задаёт параметры дисциплины и шаблона:
- `discipline`, `direction`, `level`, `department`
- трудоёмкость/часы/семестр/год
- `template` — путь к шаблонному DOCX
- `old_discipline`, `old_code` — что заменяем в шаблоне
- `sro_types` — явный список видов СРО (опционально)
- `prerequisite`, `postrequisite` — место дисциплины в плане (опционально)
- `exam_type` — тип контроля (`экзамен` / `зачёт`)

## Корпус и доменная мета
- DOCX-корпус: `rpd_corpus/*.docx`
- Промежуточные JSON: `rpd_json/*.json`
- Доменная мета для фильтрации retrieval: `rpd_json/corpus_meta.json`
- Учебники: `rpd_books/*.pdf`, `rpd_books/*.docx`

Рекомендуется поддерживать `corpus_meta.json` в актуальном состоянии для всех файлов `rpd_*.json`.

## Полезные команды
Пересоздать индекс в Qdrant:
```bash
python load_qdrant.py
# или
python load_qdrant_RouterAI.py
```

Добавить точки без пересоздания коллекции:
```bash
python load_qdrant.py --append
python load_qdrant_RouterAI.py --append
```

Сбросить кэш генерации:
```bash
python rpd_generate.py config.json --clear-cache
python rpd_generate_RouterAI.py config.json --clear-cache
```
