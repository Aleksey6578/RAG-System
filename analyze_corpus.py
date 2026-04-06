"""
analyze_corpus.py — анализ существующего корпуса RPD и рекомендации по расширению.

Что делает:
  1. Читает все .docx в rpd_corpus/ → извлекает название дисциплины, коды компетенций,
     семестр, трудоёмкость.
  2. Кластеризует дисциплины по тематическим доменам (ключевые слова).
  3. Показывает распределение корпуса по доменам.
  4. Выявляет слабо покрытые домены (< 2 РПД).
  5. Выдаёт конкретный список рекомендуемых дисциплин для синтетических РПД.
  6. Сохраняет corpus_analysis.json для последующего использования.

Запуск:
  python analyze_corpus.py
  python analyze_corpus.py --corpus-dir path/to/rpd_corpus
  python analyze_corpus.py --jsonl data_clean.jsonl   # режим jsonl (быстрее)
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict

# ---------------------------------------------------------------------------
# Тематические домены — ключевые слова для классификации
# ---------------------------------------------------------------------------
DOMAINS: dict[str, list[str]] = {
    # --- Специфичные домены (проверяются первыми) ---
    "Веб-разработка": [
        "веб", "web", "html", "css", "javascript", "react", "frontend",
        "backend", "api", "rest", "http", "браузер",
    ],
    "Компьютерная графика / Игры": [
        "график", "визуализац", "opengl", "directx", "игр", "3d",
        "рендеринг", "анимац", "геймдев",
    ],
    "Облачные технологии / DevOps": [
        "облачн", "cloud", "docker", "kubernetes", "контейнер",
        "ci/cd", "автоматизац", "развёртыван", "инфраструктур",
    ],
    "Встроенные / IoT системы": [
        "встроенн", "iot", "интернет вещей", "arduino", "raspberry",
        "fpga", "плис", "микропроцессор", "цифров", "схем",
    ],
    "Анализ данных / BI": [
        "анализ данн", "аналитик", "bi", "визуализац данн",
        "статистик", "прогнозирован", "pandas", "tableau", "power bi",
    ],
    "Кибербезопасность": [
        "безопасност", "криптограф", "защит", "угроз", "уязвимост",
        "аутентификац", "шифрован", "атак", "penetration",
    ],
    "Сети / Телекоммуникации": [
        "протокол", "tcp", "ip", "маршрутиз", "коммутац",
        "телекоммуник", "беспровод", "wi-fi", "vpn",
        "сетев", "компьютерные сет",
    ],
    # --- Общие домены (проверяются последними) ---
    "Искусственный интеллект / МО": [
        "машинн", "нейрон", "интеллект", "обучени", "deep learning",
        "классификаци", "регрессия", "кластериз", "распознавани",
        "зрени", "nlp", "обработка текст",
    ],
    "Программная инженерия": [
        "программ", "software", "методолог", "тестирован",
        "жизненн", "цикл", "agile", "проектирован",
        "архитектур", "паттерн",
    ],
    "Базы данных / Хранилища": [
        "баз", "данн", "sql", "субд", "хранилищ", "запрос",
        "реляцион", "nosql", "mongodb", "администрирован",
    ],
    "Системное / Низкоуровневое ПО": [
        "операционн", "систем", "ядро", "процесс", "поток", "памят",
        "компилятор", "ассемблер", "микроконтроллер",
        "системн", "программирован",
    ],
    "Математика / Алгоритмы": [
        "математик", "алгоритм", "теория", "граф", "дискретн",
        "вычислительн", "числен", "оптимизац", "исследован", "операц",
    ],
    "Управление проектами / Экономика ИТ": [
        "управлен", "проект", "менеджмент", "экономик", "стоимост",
        "бюджет", "риск", "планирован", "организац", "деятельност",
    ],
    "Научно-исследовательская деятельность": [
        "исследовательск", "научн", "квалификацион", "написан",
        "публикац", "методологи",
    ],
}

# Минимальное число РПД в домене, при котором домен считается «достаточно покрытым»
MIN_COVERAGE = 2

# ---------------------------------------------------------------------------
# Рекомендуемые дисциплины для слабо покрытых доменов
# ---------------------------------------------------------------------------
RECOMMENDATIONS: dict[str, list[dict]] = {
    "Веб-разработка": [
        {"name": "Веб-программирование", "focus": "HTML5, CSS3, JavaScript ES6+, REST API, Node.js"},
        {"name": "Разработка веб-приложений", "focus": "React, Vue.js, SPA, webpack, TypeScript"},
        {"name": "Серверное программирование", "focus": "Python Flask/Django, JWT, PostgreSQL, REST"},
    ],
    "Кибербезопасность": [
        {"name": "Информационная безопасность", "focus": "криптография, PKI, протоколы TLS, угрозы OWASP Top 10"},
        {"name": "Защита программного обеспечения", "focus": "анализ уязвимостей, SAST/DAST, безопасная разработка"},
        {"name": "Сетевая безопасность", "focus": "межсетевые экраны, IDS/IPS, VPN, анализ трафика"},
    ],
    "Облачные технологии / DevOps": [
        {"name": "Облачные вычисления", "focus": "AWS/Azure/GCP, IaaS/PaaS/SaaS, виртуализация, Terraform"},
        {"name": "Технологии контейнеризации", "focus": "Docker, Kubernetes, CI/CD, GitLab, Jenkins"},
        {"name": "DevOps-практики", "focus": "Git flow, автоматизация, мониторинг, Prometheus, Grafana"},
    ],
    "Компьютерная графика / Игры": [
        {"name": "Компьютерная графика", "focus": "OpenGL, GLSL, растеризация, трассировка лучей, Three.js"},
        {"name": "Разработка игр", "focus": "Unity3D, C#, игровые паттерны, физический движок, UI"},
    ],
    "Встроенные / IoT системы": [
        {"name": "Программирование микроконтроллеров", "focus": "STM32, C/C++, RTOS, SPI/I2C/UART, прерывания"},
        {"name": "Интернет вещей (IoT)", "focus": "MQTT, Zigbee, LoRa, облачные платформы, Edge Computing"},
    ],
    "Анализ данных / BI": [
        {"name": "Анализ и визуализация данных", "focus": "Python pandas, matplotlib, seaborn, дашборды, Power BI"},
        {"name": "Большие данные (Big Data)", "focus": "Hadoop, Spark, HDFS, MapReduce, потоковая обработка"},
    ],
    "Системное / Низкоуровневое ПО": [
        {"name": "Операционные системы", "focus": "процессы, потоки, синхронизация, файловые системы, Linux API"},
        {"name": "Системное программирование", "focus": "C/C++, POSIX, межпроцессное взаимодействие, память"},
    ],
    "Математика / Алгоритмы": [
        {"name": "Алгоритмы и структуры данных", "focus": "сортировка, деревья, графы, динамическое программирование"},
        {"name": "Дискретная математика", "focus": "булева алгебра, графы, комбинаторика, логика предикатов"},
    ],
    "Базы данных / Хранилища": [
        {"name": "Проектирование баз данных", "focus": "ER-диаграммы, нормализация, индексы, транзакции ACID"},
        {"name": "NoSQL базы данных", "focus": "MongoDB, Redis, Cassandra, документно-ориентированные СУБД"},
    ],
    "Сети / Телекоммуникации": [
        {"name": "Компьютерные сети", "focus": "модель OSI, TCP/IP, маршрутизация, VLAN, Cisco IOS"},
        {"name": "Протоколы передачи данных", "focus": "HTTP/2, WebSocket, gRPC, AMQP, DNS, DHCP"},
    ],
}


# ---------------------------------------------------------------------------
# Извлечение данных из DOCX
# ---------------------------------------------------------------------------

_CODE_RE    = re.compile(r"^\s*\(([А-ЯA-Z0-9Б1-9\.]{2,12})\)\s*(.+)", re.IGNORECASE)
_COMP_RE    = re.compile(r"\b(УК-\d+|ОПК-\d+|ПК-\d+)\b")
_SEM_RE     = re.compile(r"\b([1-9]|10)\s*(?:семестр|сем\.)", re.IGNORECASE)
_HOURS_RE   = re.compile(r"\b(\d{2,3})\s*час", re.IGNORECASE)
_CREDITS_RE = re.compile(r"\b([1-9])\s*з\.е\.", re.IGNORECASE)


def _extract_from_docx(path: str) -> dict:
    """Извлекает метаданные из .docx файла."""
    try:
        from docx import Document
        doc = Document(path)
    except Exception as e:
        return {"error": str(e)}

    full_text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())

    # Название дисциплины
    name = ""
    for line in full_text.split("\n"):
        m = _CODE_RE.match(line.strip())
        if m:
            candidate = m.group(2).strip()
            # Фильтруем вузовскую шапку
            if (len(candidate) > 4 and
                    not re.search(r"кафедр|университет|втик|уфа", candidate, re.I)):
                name = candidate
                break

    # Коды компетенций
    comp_codes = sorted(set(_COMP_RE.findall(full_text)))

    # Семестр
    sem_matches = _SEM_RE.findall(full_text)
    semester = sem_matches[0] if sem_matches else ""

    # Часы и з.е.
    hours_matches   = _HOURS_RE.findall(full_text)
    credits_matches = _CREDITS_RE.findall(full_text)
    hours   = int(hours_matches[0])   if hours_matches   else 0
    credits = int(credits_matches[0]) if credits_matches else 0

    return {
        "name":     name,
        "file":     os.path.basename(path),
        "comp_codes": comp_codes,
        "semester": semester,
        "hours":    hours,
        "credits":  credits,
    }


def _extract_from_jsonl(jsonl_path: str) -> list[dict]:
    """Быстрый режим: читает data_clean.jsonl вместо DOCX."""
    raw: dict = {}
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            src = r.get("source", "")
            if not src or src in raw:
                continue
            text = r.get("text", "")
            m = _CODE_RE.match(text.strip().split("\n")[0])
            name = m.group(2).strip() if m else ""
            if not name:
                dm = r.get("document_meta") or {}
                name = dm.get("discipline", "") or dm.get("title", "")
            comp_codes = sorted(set(_COMP_RE.findall(text)))
            raw[src] = {
                "name":       name,
                "file":       src,
                "comp_codes": comp_codes,
                "semester":   "",
                "hours":      0,
                "credits":    0,
            }
    return list(raw.values())


# ---------------------------------------------------------------------------
# Классификация по домену
# ---------------------------------------------------------------------------

def classify_domain(name: str) -> str:
    """Возвращает название домена или 'Прочее'."""
    name_lower = name.lower()
    for domain, keywords in DOMAINS.items():
        if any(kw in name_lower for kw in keywords):
            return domain
    return "Прочее"


# ---------------------------------------------------------------------------
# Основная логика
# ---------------------------------------------------------------------------

def analyze(items: list[dict]) -> dict:
    by_domain: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        if not item.get("name"):
            continue
        domain = classify_domain(item["name"])
        by_domain[domain].append(item)

    return dict(by_domain)


def print_report(by_domain: dict[str, list[dict]], items: list[dict]) -> None:
    total = len(items)
    print(f"\n{'='*65}")
    print(f"  АНАЛИЗ КОРПУСА РПД  ({total} документов)")
    print(f"{'='*65}")
    print(f"\n{'Домен':<42} {'РПД':>4}  Дисциплины")
    print("-"*65)

    # Сортируем по убыванию числа РПД
    for domain, docs in sorted(by_domain.items(), key=lambda x: -len(x[1])):
        names = ", ".join(d["name"] for d in docs[:3])
        if len(docs) > 3:
            names += f" ... (+{len(docs)-3})"
        flag = "✅" if len(docs) >= MIN_COVERAGE else "❌"
        print(f"  {flag} {domain:<40} {len(docs):>3}  {names}")

    # Непокрытые домены из рекомендаций
    covered = set(by_domain.keys())
    missing_domains = [d for d in RECOMMENDATIONS if d not in covered
                       or len(by_domain.get(d, [])) < MIN_COVERAGE]

    print(f"\n{'='*65}")
    print(f"  СЛАБО ПОКРЫТЫЕ ДОМЕНЫ (< {MIN_COVERAGE} РПД):")
    print(f"{'='*65}")
    if not missing_domains:
        print("  Все ключевые домены покрыты.")
    else:
        for d in missing_domains:
            have = len(by_domain.get(d, []))
            print(f"  ⚠️  {d}  (текущих: {have})")

    print(f"\n{'='*65}")
    print(f"  РЕКОМЕНДУЕМЫЕ ДИСЦИПЛИНЫ ДЛЯ СИНТЕТИЧЕСКИХ РПД:")
    print(f"{'='*65}")
    total_recs = 0
    for domain in missing_domains:
        recs = RECOMMENDATIONS.get(domain, [])
        if not recs:
            continue
        have = len(by_domain.get(domain, []))
        need = max(0, MIN_COVERAGE - have)
        print(f"\n  [{domain}]")
        for i, r in enumerate(recs[:need + 1]):
            marker = "★" if i < need else " "
            print(f"   {marker} {r['name']}")
            print(f"       focus: {r['focus']}")
            total_recs += 1
    if not total_recs:
        print("  Нет рекомендаций — корпус сбалансирован.")

    print(f"\n{'='*65}")
    print(f"  СВОДКА: нужно добавить ~{total_recs} синтетических РПД")
    print(f"  для достижения минимального покрытия по всем доменам.")
    print(f"{'='*65}\n")


def save_json(by_domain: dict, output_path: str, items: list[dict]) -> None:
    report = {
        "total_documents": len(items),
        "domains": {
            domain: {
                "count": len(docs),
                "covered": len(docs) >= MIN_COVERAGE,
                "disciplines": [d["name"] for d in docs],
            }
            for domain, docs in sorted(by_domain.items(), key=lambda x: -len(x[1]))
        },
        "recommendations": {
            domain: [
                {"name": r["name"], "focus": r["focus"]}
                for r in recs
            ]
            for domain, recs in RECOMMENDATIONS.items()
            if domain not in by_domain or len(by_domain.get(domain, [])) < MIN_COVERAGE
        },
        "all_disciplines": [
            {"file": i["file"], "name": i["name"], "domain": classify_domain(i["name"])}
            for i in items if i.get("name")
        ],
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"✅ Отчёт сохранён: {output_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Анализ корпуса РПД")
    parser.add_argument(
        "--corpus-dir", default="rpd_corpus",
        help="Папка с .docx файлами корпуса (по умолчанию: rpd_corpus/)",
    )
    parser.add_argument(
        "--jsonl", default=None,
        help="Путь к data_clean.jsonl (быстрый режим, без чтения DOCX)",
    )
    parser.add_argument(
        "--out", default="corpus_analysis.json",
        help="Путь для сохранения JSON-отчёта (по умолчанию: corpus_analysis.json)",
    )
    args = parser.parse_args()

    # --- Загрузка данных ---
    if args.jsonl and os.path.exists(args.jsonl):
        print(f"📦 Режим: data_clean.jsonl  ({args.jsonl})")
        items = _extract_from_jsonl(args.jsonl)
    elif os.path.isdir(args.corpus_dir):
        docx_files = sorted(
            os.path.join(args.corpus_dir, f)
            for f in os.listdir(args.corpus_dir)
            if f.endswith(".docx") and not f.startswith("~$")
               and not f.startswith("Шаблон")
        )
        if not docx_files:
            print(f"❌ .docx файлы не найдены в {args.corpus_dir!r}")
            sys.exit(1)
        print(f"📦 Режим: DOCX  ({len(docx_files)} файлов из {args.corpus_dir}/)")
        items = []
        for path in docx_files:
            result = _extract_from_docx(path)
            if "error" not in result:
                items.append(result)
            else:
                print(f"  ⚠️  {os.path.basename(path)}: {result['error']}")
    else:
        print(f"❌ Ни jsonl, ни corpus_dir не найдены. "
              f"Укажи --corpus-dir или --jsonl")
        sys.exit(1)

    # Фильтруем записи без названия
    named = [i for i in items if i.get("name")]
    unnamed = len(items) - len(named)
    if unnamed:
        print(f"  ⚠️  {unnamed} документов без распознанного названия пропущены")

    print(f"  ✅ Распознано: {len(named)} дисциплин\n")

    # --- Анализ ---
    by_domain = analyze(named)

    # --- Вывод ---
    print_report(by_domain, named)

    # --- Сохранение ---
    save_json(by_domain, args.out, named)


if __name__ == "__main__":
    main()
