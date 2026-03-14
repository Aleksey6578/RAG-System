# RAG-System

## Описание
RAG-System — пайплайн для подготовки корпуса РПД, индексации в Qdrant и генерации итогового документа `.docx` с помощью LLM.

## Требования

### Python
- Python 3.10+
- `pip`

### Сервисы
1. **Qdrant** (по умолчанию `http://localhost:6333`) — векторная база для хранения чанков.
2. **Ollama** (по умолчанию `http://localhost:11434`) — локальный inference-сервер для:
   - эмбеддингов: модель **`bge-m3`** (используется в `load_qdrant.py`),
   - генерации текста: модель из `GENERATION["model"]` в `rpd_generate.py`.

Перед запуском убедитесь, что сервисы подняты, а нужные модели загружены в Ollama.

## Установка
1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/Aleksey6578/RAG-System.git
   cd RAG-System
   ```
2. Создайте и активируйте виртуальное окружение Python:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```
3. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```

## Порядок запуска пайплайна
Выполняйте шаги строго по порядку:

1. Конвертация исходных `.docx` в JSON:
   ```bash
   python converter.py
   ```
2. Очистка и нормализация текстов:
   ```bash
   python prepare_texts.py
   ```
3. Нарезка текста на чанки:
   ```bash
   python chunking.py
   ```
4. Загрузка чанков в Qdrant (с эмбеддингами через Ollama):
   ```bash
   python load_qdrant.py
   ```
5. Генерация итоговой РПД по конфигу:
   ```bash
   python rpd_generate.py config.json
   ```

## Выходные файлы
- `rpd_json/` — JSON-представления документов РПД после конвертации.
- `data_clean.jsonl` — очищенные и нормализованные тексты для дальнейшей обработки.
- `chunks.jsonl` — чанки, подготовленные для индексирования в Qdrant.
- `generation_log.json` — журнал генерации (промпты, промежуточные результаты, диагностика).
- `output_rpd.docx` — финальный сгенерированный документ РПД.
