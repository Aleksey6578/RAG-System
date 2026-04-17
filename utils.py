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
    [§3.2.1] Единая классификация типов разделов РПД по заголовку.

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
    if re.search(r"содержан|лекц|лаборатор|практич|тем[аы]", t):               return "content"
    # [FIX-1б] «самостоятельн» убрано из assessment: заголовки вида
    # «Самостоятельная работа студента» некорректно попадали в assessment.
    if re.search(r"фос|фонд оценочн|оценочн|аттестац|контрол|виды\s+сро", t): return "assessment"
    # [FIX-BIBLIO-PRIO] bibliography ВЫШЕ accessibility и hours:
    # комбинированные заголовки «Для лиц с ОВЗ, об обеспеченности литературой»
    # по содержанию библиографические → проверяем первым.
    if re.search(r"литератур|библиограф|учебно.метод|учебной литератур"
                 r"|обеспеченност|^сведени", t):                                 return "bibliography"
    # [FIX-BIBLIO-PRIO] accessibility ПОСЛЕ bibliography — ловит только
    # чисто ОВЗ-секции без библиографических ключевых слов.
    if re.search(r"доступн|инвалид|огранич.{0,15}возможн|здоровь|овз", t):     return "accessibility"
    if re.search(r"методическ", t):                                             return "methodical"
    if re.search(r"место.{0,15}дисципл|структур.{0,10}опоп", t):               return "place"
    if re.search(r"матери.{0,10}техн|аудитор|оборудован", t):                  return "infrastructure"
    if re.search(r"час[ыа]|трудоёмк|трудоем|семестр", t):                      return "hours"
    return "other"


def get_embedding(text: str, prefix: str = "query", retry: int = 3) -> list[float]:
    """
    Единая функция эмбеддинга через Ollama /api/embed (≥0.6).

    prefix:
      'passage' — для индексируемых текстов (load_qdrant, book_loader)
      'query'   — для поисковых запросов    (rpd_generate, test_generate, evaluate)

    Возвращает пустой список при неудаче (не поднимает исключение),
    чтобы caller мог проверить `if not vec`.
    """
    if not text:
        return []
    if len(text) > MAX_EMBED_CHARS:
        text = text[:MAX_EMBED_CHARS]

    delay = 2.0
    for attempt in range(retry):
        try:
            r = requests.post(
                OLLAMA_EMBED_URL,
                json={"model": EMBED_MODEL, "input": f"{prefix}: {text}"},
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
        except Exception as e:
            if attempt == retry - 1:
                print(f"  ⚠️  Ошибка эмбеддинга (попытка {attempt+1}/{retry}): {e}")
                return []
            time.sleep(delay)
            delay *= 2
    return []
