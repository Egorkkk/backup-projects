# ROADMAP.md

## Проект: backup-projects

---

# Общая стратегия

Разработка идёт по принципу:
1. сначала ядро и данные
2. потом inventory
3. потом policy
4. потом backup
5. потом orchestration
6. потом UI
7. потом deployment polishing

Цель — на каждом этапе иметь рабочий кусок, пригодный для локальной проверки в VS Code.

---

# Phase 1 — Foundation

## Цель
Поднять каркас проекта и минимальную инженерную основу.

## Результат
- репозиторий готов
- конфиг загружается
- БД создаётся
- seed rules работают
- есть базовый CLI

## Что входит
- project structure
- pyproject
- config loader
- DB schema
- init-db
- default settings/rules
- basic docs

---

# Phase 2 — Inventory Base

## Цель
Научить систему видеть корневые папки проектов.

## Результат
- сканирование RAID roots работает
- новые/отсутствующие roots фиксируются в SQLite

## Что входит
- root discovery service
- roots repo
- scan-roots CLI
- tests

---

# Phase 3 — Structural Discovery

## Цель
Научить систему строить первичную карту project-папок и project-файлов.

## Результат
- one-root structural scan работает
- project_dirs фиксируются
- project_files фиксируются

## Что входит
- structural scan service
- finder
- path/stat adapters
- sync with DB
- tests on sample trees

---

# Phase 4 — Incremental Updates

## Цель
Уйти от полного пересканирования всего дерева и работать по known project_dirs.

## Результат
- incremental scan работает
- new/changed/missing files определяются корректно

## Что входит
- project_dir_scan_service
- file comparison logic
- incremental DB updates
- tests

---

# Phase 5 — Manual Control + Rules

## Цель
Сделать систему пригодной для реальной эксплуатации.

## Результат
- ручные includes работают
- rules управляются
- excludes работают
- `.aaf` limit 100MB работает
- autosave проходят
- cache режется

## Что входит
- manual includes CRUD
- extension rules CRUD
- exclude matcher
- decision engine
- manifest builder
- dry-run

---

# Phase 6 — Backup Integration

## Цель
Связать final manifest с restic.

## Результат
- система реально делает backup
- snapshot result сохраняется
- ошибки логируются

## Что входит
- restic adapter
- backup service
- backup CLI
- parser stdout/stderr
- mocked tests

---

# Phase 7 — Full Jobs

## Цель
Собрать всё в ежедневный и еженедельный сценарии.

## Результат
- `run-daily` выполняет весь pipeline
- `run-weekly` делает structural rescan и отчёт
- есть locking
- есть run history

## Что входит
- jobs
- run service
- locking
- reports
- logging

---

# Phase 8 — Flask UI

## Цель
Добавить операторский web-интерфейс.

## Результат
- dashboard
- roots
- includes
- rules
- runs
- actions
- export links

## Что входит
- Flask app
- templates
- routes
- report download
- action handlers

---

# Phase 9 — Production Readiness

## Цель
Подготовить проект к реальному серверному использованию.

## Результат
- cron setup documented
- runtime paths documented
- reverse proxy notes documented
- end-to-end smoke test completed

## Что входит
- deployment docs
- cron examples
- operational checklist
- acceptance run

---

# Рекомендуемый порядок работ по неделям

## Неделя 1
- Foundation
- DB
- config
- init-db
- repos skeleton

## Неделя 2
- root discovery
- structural scan
- sample trees
- tests

## Неделя 3
- incremental scan
- manual includes
- basic CLI

## Неделя 4
- policy engine
- excludes
- size rules
- manifest builder

## Неделя 5
- restic adapter
- backup service
- daily job
- locking
- reports

## Неделя 6
- Flask UI basics
- dashboard
- rules/includes/runs pages
- export

## Неделя 7
- polishing
- cron integration
- docs
- acceptance tests

---

# Критический путь

Самая важная последовательность, которую нельзя нарушать:

1. config
2. db schema
3. repositories
4. root discovery
5. structural scan
6. incremental scan
7. manual includes
8. policy engine
9. manifest builder
10. restic adapter
11. daily job
12. locking
13. reports
14. Flask UI

Если начать UI раньше policy/inventory, придётся переписывать интерфейс под меняющуюся логику.

---

# Правила управления сложностью

## 1. Не смешивать слои
- web не знает про sqlite напрямую
- cli не знает про SQL напрямую
- jobs не знают про HTML
- services не знают про Flask request/response

## 2. Каждую большую часть завершать тестируемым артефактом
Пример:
- после structural scan уже можно проверять реальные деревья
- после policy engine уже можно dry-run
- после restic adapter уже можно локальный snapshot

## 3. Каждую новую фичу сначала добавлять в CLI, потом в Web UI
Это снижает риск расползания логики.

## 4. Сначала фиксировать DTO и сигнатуры, потом реализацию
Это особенно важно при работе через Codex.

---

# Риски

## Риск 1: слишком ранний UI
Снижать:
- не начинать UI до готового daily pipeline skeleton

## Риск 2: избыточная магия в find/scan
Снижать:
- сперва сделать предсказуемый вариант
- оптимизировать потом

## Риск 3: бизнес-логика утекает в CLI/Web
Снижать:
- всё только через services/jobs

## Риск 4: сложно отлаживать на реальных RAID
Снижать:
- сделать хорошие sample trees
- сделать dry-run
- сначала гонять на тестовых деревьях

---

# Definition of Done для v1

v1 считается достигнутой, если:

- есть SQLite registry
- есть roots discovery
- есть structural scan
- есть incremental project_dir scan
- есть manual includes
- есть extension/size/exclude policy
- `.aaf` > 100MB корректно skip + warning
- autosave включаются
- cache исключаются
- строится manifest
- restic backup отрабатывает
- `run-daily` работает
- есть run history и export reports
- есть Flask UI
- есть cron-ready CLI pipeline