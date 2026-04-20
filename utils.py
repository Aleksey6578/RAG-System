"""
utils.py — общая инфраструктура для всех скриптов пайплайна.

Замечание #18: embed-функция дублировалась в 5 скриптах с разными именами
(get_embedding / embed_text / embed), разными endpoint-ами (/api/embeddings vs
/api/embed), разными prefix-ами и разным retry-count.
Унифицируем здесь: единое место для изменения при обновлении Ollama API.
"""
import re
import time
import requests

OLLAMA_EMBED_URL = "http://localhost:11434/api/embed"
EMBED_MODEL = "bge-m3"
# Максимум символов передаётся в API; bge-m3 лимит ~8192 токенов ≈ 24 000 симв.
# Используем консервативный порог согласованный с load_qdrant.py.
MAX_EMBED_CHARS = 4000


def classify_section(title: str) -> str:
    """

    Вынесена из chunking.py и converter.py для устранения риска
    рассинхронизации: ранее два разных скрипта поддерживали параллельные
    реализации (SECTION_TYPE_MAP-список vs regex), требовавшие 3+ ручных
    синхронизаций при каждом изменении ключевых слов.

    Порядок проверок соответствует приоритету [FIX-BIBLIO-PRIO]:
    bibliography проверяется раньше accessibility и hours.
    """
    if not title:
        return "other"
    t = title.lower()
    if re.search(r"цел[ьи]|задач[аи]", t):                                     return "goals"
    if re.search(r"компетенц", t):                                               return "competencies"
    if re.search(r"результат.{0,10}обучен|индикатор", t):                       return "learning_outcomes"
    # [З-11] Subtypes идут перед общим "content" — порядок важен.
    # Расширены паттерны для синтетических РПД (rpd_52+): "учебные занятия",
    # "тематический план", "занятие", "модуль" и т.д.
    if re.search(r"лаборатор|лаб\.\s*работ", t):                               return "lab_content"
    if re.search(r"практич|практик", t):                                        return "practice_content"
    if re.search(r"лекц", t):                                                   return "lecture_content"
    if re.search(r"содержан|тем[аы]|тематическ|занятие|занятий"
                 r"|учебн.{0,10}(план|занят|модул)|модул|раздел.{0,10}дисципл", t):
                                                                                return "content"
    # [FIX-1б]
    if re.search(r"фос|фонд оценочн|оценочн|аттестац|контрол|виды\s+сро", t): return "assessment"
    # [FIX-BIBLIO-PRIO]
    if re.search(r"литератур|библиограф|учебно.метод|учебной литератур"
                 r"|обеспеченност|^сведени", t):                                 return "bibliography"
    # [FIX-BIBLIO-PRIO]
    if re.search(r"доступн|инвалид|огранич.{0,15}возможн|здоровь|овз", t):     return "accessibility"
    if re.search(r"методическ", t):                                             return "methodical"
    if re.search(r"место.{0,15}дисципл|структур.{0,10}опоп", t):               return "place"
    if re.search(r"матери.{0,10}техн|аудитор|оборудован", t):                  return "infrastructure"
    if re.search(r"час[ыа]|трудоёмк|трудоем|семестр", t):                      return "hours"
    return "other"


def get_embedding(text: str, prefix: str = "query", retry: int = 3, use_prefix: bool = True) -> list[float]:
    """
    Единая функция эмбеддинга через Ollama /api/embed (≥0.6).

    prefix:
      'passage' — для индексируемых текстов (load_qdrant, book_loader)
      'query'   — для поисковых запросов    (rpd_generate, test_generate, evaluate)

    use_prefix: [З-15] При смене embed-модели на модели без instruction-формата
      (например multilingual-e5) передавать use_prefix=False — тогда prefix игнорируется.
      Переключать через EMBED_MODEL в utils.py при необходимости.

    Возвращает пустой список при неудаче (не поднимает исключение),
    чтобы caller мог проверить `if not vec`.
    """
    if not text:
        return []
    if len(text) > MAX_EMBED_CHARS:
        text = text[:MAX_EMBED_CHARS]

    input_text = f"{prefix}: {text}" if use_prefix else text  # [З-15]

    delay = 2.0
    for attempt in range(retry):
        try:
            r = requests.post(
                OLLAMA_EMBED_URL,
                json={"model": EMBED_MODEL, "input": input_text},
                timeout=120,
            )
            r.raise_for_status()
            d = r.json()
            # Ollama ≥0.6: {"embeddings": [[...float...]]}
            embeddings = d.get("embeddings")
            if embeddings and isinstance(embeddings, list) and embeddings[0]:
                return embeddings[0]
            # Ollama <0.6 fallback: {"embedding": [...float...]}
            vec = d.get("embedding")
            if not vec:
                data_list = d.get("data") or []
                vec = data_list[0].get("embedding") if data_list else None
            if vec:
                return vec
            # [FIX-NORESP] API ответил без исключения, но без вектора — пауза перед retry
            time.sleep(delay)
            delay *= 2
        except Exception as e:
            if attempt == retry - 1:
                print(f"  ⚠️  Ошибка эмбеддинга (попытка {attempt+1}/{retry}): {e}")
                return []
            time.sleep(delay)
            delay *= 2
    return []
