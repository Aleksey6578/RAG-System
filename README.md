# RAG-System

Python-пайплайн для генерации РПД (рабочих программ дисциплин) с использованием:
- локального эмбеддера и LLM через **Ollama**,
- векторного хранилища **Qdrant**,
- retrieval-генерации из корпуса РПД.

## Что делает проект

Проект проходит полный цикл:
1. Конвертирует `.docx`-шаблоны РПД в структурированный JSON.
2. Очищает/нормализует текст и метаданные корпуса.
3. Режет корпус на чанки для retrieval.
4. Строит эмбеддинги и загружает их в Qdrant.
5. Генерирует новую РПД по `config.json` и сохраняет `.docx`.

## Технологический стек

- Python 3.10+
- Ollama
  - embed model: `bge-m3`
  - llm model: `qwen2.5:3b`
- Qdrant
- `python-docx`, `requests` и др. зависимости из `requirements.txt`

## Установка

```bash
git clone https://github.com/Aleksey6578/RAG-System.git
cd RAG-System
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Подготовка сервисов

### 1) Qdrant

```bash
docker run -p 6333:6333 -p 6334:6334 --name qdrant -d qdrant/qdrant:latest
```

Проверка:
```bash
curl http://localhost:6333/collections
```

### 2) Ollama

Убедитесь, что Ollama запущен локально и доступны модели:

```bash
ollama pull bge-m3
ollama pull qwen2.5:3b
```

Проверка:
```bash
curl http://localhost:11434/api/tags
```

## Структура пайплайна

Запуск выполняется по шагам:

```bash
python converter.py
python prepare_texts.py
python chunking.py
python load_qdrant.py
python rpd_generate.py config.json
```

### Назначение скриптов

- `converter.py` — извлекает структуру из `.docx` в `rpd_json/*.json`
- `prepare_texts.py` — чистит и дедуплицирует, пишет `data_clean.jsonl`
- `chunking.py` — формирует retrieval-чанки, пишет `chunks.jsonl`
- `load_qdrant.py` — строит эмбеддинги и загружает точки в коллекцию Qdrant
- `rpd_generate.py` — retrieval + генерация итогового `output_rpd.docx`

## Конфигурация генерации (`config.json`)

Пример минимального `config.json`:

```json
{
  "discipline": "Интеллектуальные системы",
  "direction": "09.03.01 Информатика и вычислительная техника",
  "level": "бакалавриат",
  "semester": "7",
  "competency_codes": "УК-1, ОПК-1, ОПК-2, ПК-1, ПК-2",
  "hours_lecture": 12,
  "hours_practice": 36,
  "hours_lab": 16,
  "hours_self": 62,
  "credits": 4,
  "exam_type": "экзамен"
}
```

Дополнительно можно указывать:
- `template` — путь к `.docx` шаблону,
- `old_discipline`, `old_code`, `new_code` — для точечной замены в шаблоне.

## Выходные артефакты

- `output_rpd.docx` — сгенерированная РПД
- `generation_log.json` — лог retrieval/генерации по секциям
- `chunks.jsonl` — чанки, загруженные в Qdrant
- `data_clean.jsonl` — очищенный корпус

## Диагностика и типовые проблемы

### JSON fallback в генерации

Если в консоли есть сообщения вида:
- `JSON не распарсился...`
- `JSON недоступен... — regex-fallback`

это означает, что LLM не вернула валидный JSON с первой попытки.
Проверьте:
1. доступность Ollama,
2. корректность модели `qwen2.5:3b`,
3. качество retrieval-контекста (см. `generation_log.json`).

### Предупреждения chunking про лимиты

`chunking.py` использует адаптивные лимиты по типам секций и может помечать часть чанков как soft-limited / low priority. Смотрите итоговую статистику в выводе скрипта.

### Проблемы с retrieval

Если `rpd_generate.py` пишет, что чанки не найдены по порогу:
- проверьте, что `load_qdrant.py` загрузил коллекцию,
- проверьте `direction/level` в `config.json` и метаданных корпуса,
- проверьте доступность `http://localhost:6333`.

## Быстрый smoke-check

```bash
python prepare_texts.py
python chunking.py
python load_qdrant.py
python rpd_generate.py config.json
```

После успешного прогона должны появиться `output_rpd.docx` и `generation_log.json`.

## Clean artifacts before commit

Перед коммитом удаляйте временные артефакты генерации и shell-ошибок:

```bash
# проверить подозрительные пустые файлы в корне
find . -maxdepth 1 -type f -empty \
  \( -name 'GENERATION*' -o -name '*_SOFT_LIMIT_CHUNKS' -o -name '[0-9]*' \
     -o -name 'dict[str' -o -name 'float' -o -name 'int' -o -name 'list' -o -name 'str' -o -name 'stype_limit' \)
```

В репозитории добавлен pre-commit hook `.githooks/pre-commit`:
- блокирует коммит пустых файлов с подозрительными именами,
- предупреждает о бинарных файлах вне whitelist.

Включение hook'а (один раз):

```bash
git config core.hooksPath .githooks
```
