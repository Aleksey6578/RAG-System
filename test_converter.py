"""
test_converter.py — unit-тесты для is_section_heading().

[ЗАМЕЧАНИЕ отчёта §7.1]: Функция содержит 5 уровней проверок
(стиль → regex → KEY_HEADERS → UPPER case → fallback). При 61 документе
с разной структурой одна ложная классификация заголовка ломает весь каскад
секций ниже. Рекомендация: создать test_converter.py с 20–30 тестовыми заголовками.

Запуск:
    python -m pytest test_converter.py -v
    python test_converter.py  # без pytest
"""

import sys
import os

# Добавляем текущий каталог в PATH для импорта converter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from converter import is_section_heading


# ──────────────────────────────────────────────────────────────────────────────
# Тестовые данные: (текст, стиль, ожидаемый_is_heading, ожидаемый_уровень)
# ──────────────────────────────────────────────────────────────────────────────

# === Положительные: должны быть распознаны как заголовки ===
POSITIVE_CASES = [
    # Нумерованные заголовки (основной regex)
    ("1. Цели дисциплины",                      None,        True,  1),
    ("2. Место дисциплины в структуре ОПОП",    None,        True,  1),
    ("3.1. Формируемые компетенции",            None,        True,  2),
    ("4.2.1. Тематический план лекций",         None,        True,  3),
    ("5. Содержание дисциплины",                None,        True,  1),

    # KEY_HEADERS (ключевые слова)
    ("Цели дисциплины освоения курса",          None,        True,  1),
    ("Формируемые компетенции выпускников",     None,        True,  1),
    ("Результаты обучения по дисциплине",       None,        True,  1),
    ("Содержание дисциплины по модулям",        None,        True,  1),

    # UPPER case заголовки (>= 3 слов, без точки)
    ("УЧЕБНО-МЕТОДИЧЕСКОЕ ОБЕСПЕЧЕНИЕ ДИСЦИПЛИНЫ", None,    True,  2),
    ("ФОНД ОЦЕНОЧНЫХ СРЕДСТВ",                 None,        True,  2),
    ("ПЕРЕЧЕНЬ ОСНОВНОЙ ЛИТЕРАТУРЫ",            None,        True,  2),

    # Через стиль Word
    ("Любой текст заголовка",                   "Heading 1", True,  1),
    ("Подзаголовок",                            "Heading 2", True,  2),
    ("Заголовок третьего уровня",               "Заголовок 3", True, 3),
]

# === Отрицательные: НЕ должны быть распознаны как заголовки ===
NEGATIVE_CASES = [
    # Даты
    ("01.09.2024 протокол заседания кафедры",   None,        False, 0),
    ("15.06.2025",                              None,        False, 0),

    # Только числа
    ("1.2.3",                                   None,        False, 0),
    ("42",                                      None,        False, 0),

    # Слишком длинный текст (> MAX_HEADING_LENGTH=300)
    ("А" * 301,                                 None,        False, 0),

    # Короткие UPPER (< 3 слов) — ложные срабатывания [§10.2]
    ("ИТОГО",                                   None,        False, 0),
    ("ОВЗ",                                     None,        False, 0),
    ("ФГОС",                                    None,        False, 0),

    # UPPER с точкой — не заголовок
    ("СОГЛАСОВАНО С БИБЛИОТЕКОЙ.",              None,        False, 0),

    # Обычный текст (тело параграфа)
    ("Студент должен знать основные методы машинного обучения и уметь "
     "применять их для решения практических задач.",
                                                None,        False, 0),

    # Табличные данные
    ("2 | Извлечение знаний | 7 | 3 | 9",      None,        False, 0),

    # Короткий текст без структуры
    ("да",                                      None,        False, 0),
    ("Зачётная единица: 4",                     None,        False, 0),
]


def run_tests():
    """Запуск тестов без pytest."""
    passed = failed = 0

    print("=" * 70)
    print("  ТЕСТЫ is_section_heading()")
    print("=" * 70)

    print("\n--- Положительные (ожидается: heading=True) ---")
    for text, style, expected_heading, expected_level in POSITIVE_CASES:
        is_h, lvl = is_section_heading(text, style)
        ok = (is_h == expected_heading)
        level_ok = (lvl == expected_level) if expected_heading else True
        status = "✅" if (ok and level_ok) else "❌"
        if ok and level_ok:
            passed += 1
        else:
            failed += 1
        short = text[:60] + ("..." if len(text) > 60 else "")
        if not ok:
            print(f"  {status} «{short}» → heading={is_h} (ожидалось {expected_heading})")
        elif not level_ok:
            print(f"  {status} «{short}» → level={lvl} (ожидалось {expected_level})")
        else:
            print(f"  {status} «{short}» → heading={is_h}, level={lvl}")

    print("\n--- Отрицательные (ожидается: heading=False) ---")
    for text, style, expected_heading, expected_level in NEGATIVE_CASES:
        is_h, lvl = is_section_heading(text, style)
        ok = (is_h == expected_heading)
        status = "✅" if ok else "❌"
        if ok:
            passed += 1
        else:
            failed += 1
        short = text[:60] + ("..." if len(text) > 60 else "")
        if not ok:
            print(f"  {status} «{short}» → heading={is_h}, level={lvl} (ЛОЖНОЕ СРАБАТЫВАНИЕ)")
        else:
            print(f"  {status} «{short}» → heading={is_h}")

    print(f"\n{'=' * 70}")
    print(f"  Результат: {passed} ✅  {failed} ❌  (всего {passed + failed})")
    print(f"{'=' * 70}")

    return failed == 0


# ──────────────────────────────────────────────────────────────────────────────
# pytest-совместимые тесты
# ──────────────────────────────────────────────────────────────────────────────

def test_positive_cases():
    for text, style, expected_heading, expected_level in POSITIVE_CASES:
        is_h, lvl = is_section_heading(text, style)
        assert is_h == expected_heading, (
            f"is_section_heading({text!r}, {style!r}) → {is_h}, expected {expected_heading}"
        )
        if expected_heading:
            assert lvl == expected_level, (
                f"is_section_heading({text!r}, {style!r}) level → {lvl}, expected {expected_level}"
            )


def test_negative_cases():
    for text, style, expected_heading, expected_level in NEGATIVE_CASES:
        is_h, lvl = is_section_heading(text, style)
        assert is_h == expected_heading, (
            f"is_section_heading({text!r}, {style!r}) → {is_h}, expected {expected_heading} "
            f"(ЛОЖНОЕ СРАБАТЫВАНИЕ)"
        )


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
