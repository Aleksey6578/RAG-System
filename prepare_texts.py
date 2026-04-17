"""
prepare_texts.py — очистка и дедупликация JSON-блоков РПД в data_clean.jsonl.

Исправления v3:
  - [F] Нормализация маркеров списков: •, ●, ▪, –, — → "- item".
  - [U] Сохранение word_count в каждой записи.
  - Предварительная фильтрация MIN_WORDS.

Исправления v3.1:
  - [T] Сохранение table_data для записей с type="table"/"table_row".

Исправления v3.2:
  - [W] Сохранение token_count_est в каждой записи.
  - [V] ИСПРАВЛЕНО: поддержка нового формата converter.py v3.2.

Исправления v3.3:
  - [X] ИСПРАВЛЕНО: direction/level/department никогда не записывались в данные.

Исправления v3.4:
  - [З-P1] ИСПРАВЛЕНО: полные дубли документов (разный source, но идентичный
    document_id) теперь пропускаются. Прежде text_hash() включал source, поэтому
    rpd_2.json и rpd_8.json (оба — «Нейронные сети») оба проходили в Qdrant,
    занимая ~40% коллекции дублями. Теперь при первом появлении document_id он
    регистрируется в seen_doc_ids; повторный файл с тем же document_id пропускается
    целиком с предупреждением.
  - [З-P3] ИСПРАВЛЕНО: corpus_meta.json проверяется на соответствие реальным
    файлам — выводятся предупреждения об «осиротевших» записях (в meta, но нет
    файла) и о файлах без записи в meta (direction/level будут пустыми)."""

import os
import re
import json
import hashlib
import unicodedata
from typing import Tuple

DATA_DIR    = "rpd_json"
OUTPUT_FILE = "data_clean.jsonl"
# [X] Опциональный файл метаданных корпуса.
# Формат: {"имя_файла.json": {"direction": "...", "level": "...", "department": "..."}}
# Позволяет проставить доменные поля для фильтрации в Qdrant / rpd_generate.
CORPUS_META_FILE = os.path.join(DATA_DIR, "corpus_meta.json")

# [FIX-#17] MIN_WORDS поднят с 10 до 21 (рекомендация аудита: 20–22).
# При MIN_WORDS=10 короткие строки вида «Зачётная единица: 4» (~4 слова)
# после округления count_tokens(*1.8) давали tc≈7 и проходили в corpus.
# Порог 21 слово ≈ ~38 токенов — соответствует MIN_TOKENS=40 в chunking.py
# и отсекает ~5–10% бессодержательных коротких записей.
MIN_WORDS = 21

# [§15.4.1] Паттерн для извлечения названия дисциплины из текста первых чанков.
# Ищет строку «РАБОЧАЯ ПРОГРАММА ДИСЦИПЛИНЫ» и берёт следующую непустую строку.
_RPD_TITLE_RE = re.compile(
    r"РАБОЧАЯ\s+ПРОГРАММА\s+ДИСЦИПЛИНЫ[^\n]*\n+([^\n]{5,120})",
    re.IGNORECASE,
)


def extract_discipline_title(records: list) -> str:
    """[§15.4.1] Ищет название дисциплины в тексте первых 10 чанков документа."""
    for rec in records[:10]:
        text = rec.get("text", "")
        m = _RPD_TITLE_RE.search(text)
        if m:
            return m.group(1).strip()
    return ""


def _normalize_title_field(title: str) -> str:
    """
    [FIX-TITLE-NORM] Нормализация title — удаление табличного мусора (отчёт §2.4).
    
    Заголовки в корпусе содержат табличный мусор:
    «2 | Извлечение знаний из нейронных сетей | 7 | 3 | 9 | 4 | 1».
    Это приводит к ненадёжному title-поиску (score 0.478) в evaluate.
    """
    if not title:
        return title
    if " | " in title:
        parts = [p.strip() for p in title.split("|")]
        text_parts = [p for p in parts if p and not re.match(r"^\d+$", p) and len(p) > 3]
        if text_parts:
            title = max(text_parts, key=len)
    title = re.sub(r"^\d+\s+", "", title.strip())
    title = re.sub(r"\s+\d+$", "", title.strip())
    return title.strip() or None


def normalize_list_markers(text: str) -> str:
    """[F] Унифицирует маркеры списков в формат '- item'."""
    text = re.sub(r"^[ \t]*[•●▪◦]\s+", "- ", text, flags=re.MULTILINE)
    text = re.sub(r"^[ \t]*[–—]\s+", "- ", text, flags=re.MULTILINE)
    # [БАГ 12 ИСПРАВЛЕНО]: добавлен негативный lookahead (?!\d{1,2}[.,]) чтобы
    # паттерн не срабатывал на даты вида "1. января", "21. марта" и т.п.
    # Capture group (\d+) сохранена для совместимости, но не используется (re.sub игнорирует).
    text = re.sub(r"^[ \t]*\(?\d+[.)]\s+(?!\d{1,2}[.,])", "- ", text, flags=re.MULTILINE)
    return text


def clean_text(text: str) -> str:
    """Очищает текст, сохраняя структуру параграфов и нормализуя маркеры."""
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\x00", "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = normalize_list_markers(text)
    paragraphs = text.split("\n\n")
    cleaned = []
    for para in paragraphs:
        lines = [l.strip() for l in para.split("\n") if l.strip()]
        if lines:
            cleaned.append("\n".join(lines))
    return "\n\n".join(cleaned).strip()


def text_hash(text: str, source: str = "") -> str:
    """Хеш включает source — одинаковые тексты из разных РПД не схлопываются."""
    return hashlib.sha256(f"{source}\x00{text}".encode("utf-8")).hexdigest()


def load_corpus_meta() -> dict:
    """
    [X] Загружает corpus_meta.json если он существует.

    Ожидаемый формат:
    {
      "rpd_ml.json":  {"direction": "09.03.01 Информатика и ВТ",
                       "level": "бакалавриат", "department": "ВТИК"},
      "rpd_ai.json":  {"direction": "09.03.01 Информатика и ВТ", ...},
      ...
    }
    Если файл отсутствует или повреждён — возвращает пустой dict,
    что не меняет поведение относительно предыдущих версий.
    """
    if not os.path.exists(CORPUS_META_FILE):
        return {}
    try:
        with open(CORPUS_META_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception as e:
        print(f"  ⚠️  corpus_meta.json не загружен: {e}")
    return {}


def process_record(
    record: dict, out_file, source: str, seen: set,
    document_meta: dict = None,   # [V] document-level metadata из нового формата
    domain_meta: dict = None,     # [X] direction/level/department из corpus_meta.json
    discipline_title: str = "",   # [§15.4.1] fallback название дисциплины
) -> Tuple[bool, bool]:
    """
    Возвращает (записан, пропущен_как_дубль).

    [T] Для type="table"/"table_row" сохраняет table_data.
    [V] Добавляет document_meta из верхнего уровня JSON (новый формат).
    [X] Добавляет direction/level/department из corpus_meta.json.
    """
    if "text" not in record:
        return False, False

    cleaned = clean_text(record["text"])
    if not cleaned:
        return False, False

    word_count = len(cleaned.split())
    if word_count < MIN_WORDS:
        return False, False

    h = text_hash(cleaned, source)
    if h in seen:
        return False, True
    seen.add(h)

    record_type = record.get("type", "text")

    # [W] token_count_est убран из записи ([FIX-#12]): chunking.py пересчитывает
    # через tiktoken и не читает это поле. Оставлен только в print-статистике ниже.
    token_count_est = round(word_count * 1.8)  # только для print

    output_record = {
        "source":           source,
        "document_id":      record.get("document_id", ""),
        # [FIX-TITLE-NORM] Нормализация title — удаление табличного мусора (отчёт §2.4).
        "title":            _normalize_title_field(record.get("title") or discipline_title or ""),
        "section_title":    record.get("section_title"),
        "section_level":    record.get("section_level", 0),   # [6] уже int из converter v3.2
        "section_type":     record.get("section_type"),
        "type":             record_type,
        "text":             cleaned,
        "word_count":       word_count,
        # token_count_est убран — [FIX-#12]
    }

    # [V] Document-level metadata из нового формата converter
    if document_meta:
        output_record["document_meta"] = document_meta

    # [X] Доменные поля для фильтрации в Qdrant / rpd_generate.py.
    # Берутся из corpus_meta.json; если не заданы — пустая строка (фильтр не применяется).
    dm = domain_meta or {}
    output_record["direction"]  = dm.get("direction",  "")
    output_record["level"]      = dm.get("level",      "")
    output_record["department"] = dm.get("department", "")

    # [T] Для табличных записей сохраняем структуру
    if record_type in ("table", "table_row"):
        table_data = record.get("table_data")
        if table_data:
            output_record["table_data"] = table_data

    out_file.write(json.dumps(output_record, ensure_ascii=False) + "\n")
    return True, False


def process_file(path: str, out_file, seen: set,
                 corpus_meta: dict = None) -> Tuple[int, int]:
    """
    [V] Поддерживает оба формата:
      • новый (converter v3.2): dict {"document_id", "metadata", "chunks"}
      • старый (converter v3.0): list блоков

    [X] Принимает corpus_meta (из load_corpus_meta()) и передаёт
    соответствующие доменные поля в каждую запись через process_record().
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    source  = os.path.basename(path)
    written = dups = 0

    # [X] Доменные поля для текущего файла из corpus_meta.json
    domain_meta = (corpus_meta or {}).get(source)

    # Определяем формат и извлекаем блоки и document_meta
    if isinstance(data, dict) and "chunks" in data:
        # Новый формат converter v3.2
        records       = data["chunks"]
        document_meta = data.get("metadata")
    else:
        # Старый формат: список блоков
        records       = data if isinstance(data, list) else [data]
        document_meta = None

    # [§15.4.1] Извлечь название дисциплины из текста — fallback для пустых title
    discipline_title = extract_discipline_title(records)

    for r in records:
        ok, dup = process_record(
            r, out_file, source, seen,
            document_meta=document_meta,
            domain_meta=domain_meta,         # [X]
            discipline_title=discipline_title,  # [§15.4.1]
        )
        if ok:
            written += 1
        elif dup:
            dups += 1

    return written, dups


def main():
    seen: set = set()
    # [З-P1] Два раздельных множества:
    # seen_doc_ids — MD5(filename) для пропуска полных дублей по имени.
    # seen_content_hashes — MD5(text) для пропуска файлов с идентичным содержимым.
    # [БАГ-В ИСПРАВЛЕНО]: прежде оба значения помещались в один set → проверка
    # `doc_id in seen_doc_ids` могла ложно срабатывать на content_hash другого файла.
    seen_doc_ids: set        = set()
    seen_content_hashes: set = set()
    total_written = total_dups = total_skipped_docs = 0
    table_count = 0

    # [X] Загружаем corpus_meta.json один раз для всего прогона
    corpus_meta = load_corpus_meta()
    if corpus_meta:
        print(f"corpus_meta.json: загружено {len(corpus_meta)} записей")
    else:
        print("corpus_meta.json: не найден — direction/level/department будут пустыми")

    # [З-P3] Проверяем соответствие corpus_meta и реальных файлов
    if corpus_meta:
        real_files = {fn for fn in os.listdir(DATA_DIR)
                      if fn.endswith(".json") and fn != "corpus_meta.json"}
        meta_keys  = set(corpus_meta.keys())
        orphaned   = meta_keys - real_files
        no_meta    = real_files - meta_keys
        if orphaned:
            print(f"  ⚠️  corpus_meta: записи без файла (осиротевшие): {sorted(orphaned)}")
        if no_meta:
            print(f"  ⚠️  corpus_meta: файлы без записи (direction будет пустым): {sorted(no_meta)}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for fn in sorted(os.listdir(DATA_DIR)):
            if not fn.endswith(".json") or fn == "corpus_meta.json":
                continue
            path = os.path.join(DATA_DIR, fn)
            try:
                with open(path, encoding="utf-8") as _f:
                    _raw = json.load(_f)

                # [З-P1] Проверка по document_id (MD5 имени файла)
                doc_id = _raw.get("document_id", "") if isinstance(_raw, dict) else ""
                if doc_id and doc_id in seen_doc_ids:
                    print(f"  ⏭  {fn}: пропущен (document_id совпадает — дубль)")
                    total_skipped_docs += 1
                    continue

                # [D-2] ИСПРАВЛЕНО: дополнительная проверка по хешу контента.
                # document_id = MD5(filename) — файлы с разными именами, но
                # идентичным содержимым (rpd_6 ≡ rpd_13) не обнаруживались.
                # Теперь вычисляем MD5 по тексту всех чанков документа и
                # пропускаем файл если такой контент уже был обработан.
                if isinstance(_raw, dict) and "chunks" in _raw:
                    _chunks_texts = "".join(
                        c.get("text", "") for c in _raw["chunks"]
                    )
                    content_hash = hashlib.md5(_chunks_texts.encode("utf-8")).hexdigest()
                    if content_hash in seen_content_hashes:
                        print(f"  ⏭  {fn}: пропущен (идентичное содержимое — контент-дубль)")
                        total_skipped_docs += 1
                        continue
                    seen_content_hashes.add(content_hash)

                if doc_id:
                    seen_doc_ids.add(doc_id)

                w, d = process_file(path, out, seen, corpus_meta=corpus_meta)
                total_written += w
                total_dups    += d
                dm = corpus_meta.get(fn, {})
                meta_info = f" [{dm.get('direction','—')}]" if dm else ""
                print(f"  {fn}{meta_info}: записано={w}, дублей={d}")
            except Exception as e:
                print(f"  ❌ {fn}: {e}")

    with open(OUTPUT_FILE, encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("type") in ("table", "table_row") and rec.get("table_data"):
                table_count += 1

    print(f"\nГотово → {OUTPUT_FILE}")
    print(f"  Записано    : {total_written}")
    print(f"  Дублей      : {total_dups}")
    if total_skipped_docs:
        print(f"  Пропущено документов-дублей: {total_skipped_docs}")
    print(f"  Таблиц с table_data: {table_count}")
    print(f"  Уникальность: {total_written / max(total_written + total_dups, 1) * 100:.1f}%")

    # [FIX-#12] Блок статистики token_count_est удалён — поле убрано из записей.

    # [X] Статистика доменных полей
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        directions = {json.loads(l).get("direction", "") for l in f}
    filled = {d for d in directions if d}
    if filled:
        print(f"  direction-значений: {len(filled)} уникальных")
    else:
        print("  ⚠️  direction пуст — создайте corpus_meta.json для доменной фильтрации")


if __name__ == "__main__":
    main()
