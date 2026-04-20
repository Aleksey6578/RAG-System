"""
run_pipeline.py — последовательный запуск всего RAG-пайплайна.

Порядок:
  1. converter.py       — конвертация DOCX-корпуса в JSON-чанки
  2. prepare_texts.py   — дедупликация и сборка corpus.jsonl
  3. chunking.py        — смарт-чанкинг и сохранение chunks.jsonl
  4. load_qdrant.py     — загрузка чанков в Qdrant (--recreate для пересборки)
  5. rpd_generate.py    — генерация РПД в DOCX по config.json
  6. evaluate.py        — вычисление BLEU/ROUGE/semantic_sim

Использование:
  python run_pipeline.py                      # полный прогон
  python run_pipeline.py --from load_qdrant   # начать с шага 4
  python run_pipeline.py --only evaluate      # только оценка
  python run_pipeline.py --recreate           # передать --recreate в load_qdrant

[OI-08]
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

# ─── Порядок и параметры шагов ────────────────────────────────────────────────
STEPS = [
    {
        "name": "converter",
        "script": "converter.py",
        "args": [],
        "desc": "Конвертация DOCX → JSON",
    },
    {
        "name": "prepare_texts",
        "script": "prepare_texts.py",
        "args": [],
        "desc": "Дедупликация и сборка корпуса",
    },
    {
        "name": "chunking",
        "script": "chunking.py",
        "args": [],
        "desc": "Смарт-чанкинг",
    },
    {
        "name": "load_qdrant",
        "script": "load_qdrant.py",   # или load_qdrant_RouterAI.py
        "args": [],                    # --recreate добавляется динамически
        "desc": "Загрузка чанков РПД в Qdrant",
    },
    {
        "name": "book_loader",
        "script": "book_loader.py",   # или book_loader_routerai.py
        "args": [],
        "desc": "Загрузка учебников в Qdrant",
    },
    {
        "name": "rpd_generate",
        "script": "rpd_generate.py",  # или rpd_generate_RouterAI.py
        "args": ["config.json"],
        "desc": "Генерация РПД",
    },
    {
        "name": "evaluate",
        "script": "evaluate.py",      # или evaluate_routerai.py
        "args": [],
        "desc": "Оценка BLEU/ROUGE/semantic_sim",
    },
    {
        "name": "test_generate",
        "script": "test_generate.py",  # или test_generate_routerai.py
        "args": [],
        "desc": "Генерация тестовых вопросов ФОС",
    },
]

STEP_NAMES = [s["name"] for s in STEPS]


def run_step(step: dict, extra_args: list[str]) -> bool:
    script = Path(step["script"])
    if not script.exists():
        print(f"  ⚠️  {script} не найден — пропуск шага '{step['name']}'")
        return True  # не считаем фатальной ошибкой

    cmd = [sys.executable, str(script)] + step["args"] + extra_args
    print(f"\n{'─'*60}")
    print(f"▶  {step['desc']} ({script})")
    print(f"   {' '.join(cmd)}")
    print(f"{'─'*60}")

    t0 = time.time()
    result = subprocess.run(cmd)
    elapsed = time.time() - t0

    if result.returncode != 0:
        print(f"\n❌ Шаг '{step['name']}' завершился с кодом {result.returncode} "
              f"(~{elapsed:.0f}с)")
        return False

    print(f"\n✅ Шаг '{step['name']}' завершён за ~{elapsed:.0f}с")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Последовательный запуск RAG-пайплайна",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--from", dest="from_step", default=None,
        choices=STEP_NAMES,
        metavar="STEP",
        help=f"Начать с указанного шага (варианты: {', '.join(STEP_NAMES)})",
    )
    parser.add_argument(
        "--only", dest="only_step", default=None,
        choices=STEP_NAMES,
        metavar="STEP",
        help="Запустить только один шаг",
    )
    parser.add_argument(
        "--recreate", action="store_true",
        help="Передать --recreate в load_qdrant (пересоздание коллекции)",
    )
    parser.add_argument(
        "--routerai", action="store_true",
        help="Использовать RouterAI-варианты скриптов (load_qdrant_RouterAI.py, "
             "rpd_generate_RouterAI.py, evaluate_routerai.py)",
    )
    args = parser.parse_args()

    # RouterAI-режим: переключаем скрипты
    if args.routerai:
        for step in STEPS:
            base = step["name"]
            if base == "load_qdrant":
                step["script"] = "load_qdrant_RouterAI.py"
            elif base == "book_loader":
                step["script"] = "book_loader_routerai.py"
            elif base == "rpd_generate":
                step["script"] = "rpd_generate_RouterAI.py"
            elif base == "evaluate":
                step["script"] = "evaluate_routerai.py"
            elif base == "test_generate":
                step["script"] = "test_generate_routerai.py"

    # Определяем активные шаги
    if args.only_step:
        active = [s for s in STEPS if s["name"] == args.only_step]
    elif args.from_step:
        idx = STEP_NAMES.index(args.from_step)
        active = STEPS[idx:]
    else:
        active = STEPS

    # --recreate → дополнительный аргумент для load_qdrant
    recreate_extra: dict[str, list[str]] = {}
    if args.recreate:
        recreate_extra["load_qdrant"] = ["--recreate"]

    print(f"\n{'='*60}")
    print(f"  RAG-пайплайн: {len(active)} шаг(ов)")
    print(f"  Режим: {'RouterAI' if args.routerai else 'Local Ollama'}")
    print(f"{'='*60}")

    t_total = time.time()
    for step in active:
        extra = recreate_extra.get(step["name"], [])
        ok = run_step(step, extra)
        if not ok:
            print(f"\n💥 Пайплайн прерван на шаге '{step['name']}'")
            sys.exit(1)

    elapsed_total = time.time() - t_total
    print(f"\n{'='*60}")
    print(f"  ✅ Пайплайн завершён за ~{elapsed_total/60:.1f} мин")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
