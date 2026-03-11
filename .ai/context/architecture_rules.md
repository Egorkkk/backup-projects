# architecture_rules.md

## Главные границы

### 1. Не смешивать слои
Запрещено:
- web знает SQL / SQLite напрямую
- CLI знает SQL напрямую
- jobs знают HTML/templates
- services знают Flask request/response

### 2. Направление зависимостей
Предпочтительно:

web -> services -> repositories/adapters
cli -> services -> repositories/adapters
jobs -> services -> repositories/adapters

Domain DTO и enums должны быть достаточно нейтральными, чтобы не зависеть от Flask и конкретного CLI.

---

## Слои

### Domain
Хранит:
- enums
- DTO / models
- decision objects
- общие типы

Не должен знать:
- Flask
- click/typer internals
- SQL details
- restic subprocess details

### Adapters
Хранят:
- db session/schema/sqlite helpers
- filesystem helpers
- subprocess wrappers
- restic adapter
- file stat/path adapters

Не должны содержать orchestration уровня jobs/web.

### Repositories
Инкапсулируют доступ к БД.
CLI и web не должны собирать SQL сами.

### Services
Содержат бизнес-логику:
- root discovery
- structural scan
- incremental scan
- policy
- manifest build
- backup orchestration
- reports

### Jobs
Компонуют несколько services в daily/weekly сценарии.

### CLI
Тонкий слой вызова services/jobs.

### Web
Тонкий слой routes/templates/actions поверх services/jobs.

---

## Правило реализации

Сначала:
- DTO
- enums
- module boundaries
- function signatures

Потом:
- реализация
- тесты
- CLI wrapper
- web wrapper

---

## Правило UI

Нельзя начинать полноценный Flask UI до тех пор, пока не готов хотя бы минимальный daily pipeline skeleton.

Если сделать UI раньше:
- придётся переписывать templates и handlers
- будет протекать бизнес-логика в routes

---

## Правило baseline-first

Сначала:
- предсказуемая, простая, тестируемая версия

Потом:
- оптимизация scan
- улучшение heuristics
- advanced convenience

Не строить сложную "умную" магию раньше рабочей baseline-системы.

---

## Правило артефактов

Каждый большой этап должен завершаться проверяемым артефактом:

- config -> valid loading / validation
- db -> schema init + seed
- roots -> scan-roots
- structural scan -> sample tree scan
- policy -> dry-run
- restic -> local snapshot
- jobs -> run-daily
- reports -> export
- web -> operator pages