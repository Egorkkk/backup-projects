# TASKS.md

## Проект: backup-projects
Цель: реализовать v1 системы автоматического бэкапа project-файлов с RAID-массивов через inventory + SQLite + policy engine + restic + Flask UI + CLI.

---

## Статусы
- [ ] not started
- [~] in progress
- [x] done
- [!] blocked

---

# 0. Подготовка репозитория

## 0.1. Базовый каркас
- [ ] Создать git-репозиторий
- [ ] Создать ветку `main`
- [ ] Создать базовую структуру папок проекта
- [ ] Добавить `README.md`
- [ ] Добавить `ARCHITECTURE.md`
- [ ] Добавить `TASKS.md`
- [ ] Добавить `ROADMAP.md`
- [ ] Добавить `docs/tdz_v1.md`
- [ ] Добавить `.gitignore`

## 0.2. Python-проект
- [ ] Создать `pyproject.toml`
- [ ] Зафиксировать Python version: 3.12+
- [ ] Добавить зависимости:
  - [ ] Flask
  - [ ] Jinja2
  - [ ] Typer
  - [ ] SQLAlchemy или выбранный DB слой
  - [ ] pydantic
  - [ ] pytest
  - [ ] ruff
  - [ ] black
- [ ] Добавить `requirements.txt` или lock-файл
- [ ] Добавить `Makefile` с базовыми командами

## 0.3. Developer tooling
- [ ] Настроить `ruff`
- [ ] Настроить `black`
- [ ] Настроить `pytest`
- [ ] Добавить базовую команду `make test`
- [ ] Добавить базовую команду `make lint`
- [ ] Добавить базовую команду `make format`

## 0.4. Runtime layout
- [ ] Создать папки `runtime/logs`
- [ ] Создать папки `runtime/manifests`
- [ ] Создать папки `runtime/reports`
- [ ] Создать папки `runtime/db`
- [ ] Создать папки `runtime/locks`

---

# 1. Конфигурация

## 1.1. Конфиг-файлы
- [ ] Создать `config/app.example.yaml`
- [ ] Создать `config/rules.example.yaml`

## 1.2. Загрузка конфигурации
- [ ] Реализовать `config.py`
- [ ] Описать структуру app settings
- [ ] Описать структуру raid roots
- [ ] Описать runtime paths
- [ ] Описать web settings
- [ ] Описать DB settings
- [ ] Описать restic settings
- [ ] Описать scheduler mode (`cron`)
- [ ] Реализовать валидацию конфигурации

## 1.3. Константы
- [ ] Создать `constants.py`
- [ ] Зафиксировать default `.aaf` limit = 100MB
- [ ] Зафиксировать default allowed extensions
- [ ] Зафиксировать default exclude patterns

---

# 2. База данных

## 2.1. DB skeleton
- [ ] Выбрать окончательно DB слой
- [ ] Реализовать `adapters/db/session.py`
- [ ] Реализовать `adapters/db/schema.py`
- [ ] Реализовать `adapters/db/sqlite_utils.py`

## 2.2. Таблицы
- [ ] Создать таблицу `roots`
- [ ] Создать таблицу `project_dirs`
- [ ] Создать таблицу `project_files`
- [ ] Создать таблицу `manual_includes`
- [ ] Создать таблицу `extension_rules`
- [ ] Создать таблицу `excluded_patterns`
- [ ] Создать таблицу `settings`
- [ ] Создать таблицу `runs`
- [ ] Создать таблицу `run_events`
- [ ] Создать таблицу `unrecognized_extensions`

## 2.3. Инициализация БД
- [ ] Реализовать `init-db`
- [ ] Реализовать seed default settings
- [ ] Реализовать seed extension rules
- [ ] Реализовать seed exclude rules
- [ ] Реализовать проверку повторного init

## 2.4. Репозитории
- [ ] Реализовать `roots_repo.py`
- [ ] Реализовать `project_dirs_repo.py`
- [ ] Реализовать `project_files_repo.py`
- [ ] Реализовать `manual_includes_repo.py`
- [ ] Реализовать `rules_repo.py`
- [ ] Реализовать `runs_repo.py`
- [ ] Реализовать `settings_repo.py`

## 2.5. Тесты БД
- [ ] Написать integration tests для schema init
- [ ] Написать integration tests для repositories CRUD
- [ ] Написать test fixtures для sqlite

---

# 3. Domain layer

## 3.1. Enums
- [ ] Описать enums для статусов roots
- [ ] Описать enums для статусов project_dirs
- [ ] Описать enums для статусов project_files
- [ ] Описать enums для job types
- [ ] Описать enums для oversize actions
- [ ] Описать enums для include path type

## 3.2. DTO / models
- [ ] Описать DTO для RootRecord
- [ ] Описать DTO для ProjectDirRecord
- [ ] Описать DTO для ProjectFileRecord
- [ ] Описать DTO для ManualInclude
- [ ] Описать DTO для ExtensionRule
- [ ] Описать DTO для ExcludedPattern
- [ ] Описать DTO для RunSummary
- [ ] Описать DTO для CandidateFile
- [ ] Описать DTO для FinalDecision
- [ ] Описать DTO для ManifestResult

---

# 4. Адаптеры файловой системы

## 4.1. Listing/stat
- [ ] Реализовать `dir_listing.py`
- [ ] Реализовать `stat_reader.py`
- [ ] Реализовать `path_utils.py`

## 4.2. Finder
- [ ] Реализовать `file_finder.py`
- [ ] Поддержать поиск по расширениям
- [ ] Поддержать исключение path patterns
- [ ] Поддержать ограничение на stay-on-fs
- [ ] Поддержать ignore symlinks
- [ ] Поддержать режим поиска только в одной root
- [ ] Поддержать режим поиска только в одной project_dir

## 4.3. Командный runner
- [ ] Реализовать `command_runner.py`
- [ ] Реализовать общий subprocess wrapper
- [ ] Логировать exit code/stdout/stderr
- [ ] Добавить timeout support

---

# 5. Inventory: roots discovery

## 5.1. Root discovery service
- [ ] Реализовать `root_discovery_service.py`
- [ ] Реализовать `list_root_directories(raid_path)`
- [ ] Реализовать фильтрацию только first-level dirs
- [ ] Реализовать сравнение с DB
- [ ] Реализовать создание новых roots
- [ ] Реализовать пометку missing roots
- [ ] Реализовать обновление last_seen

## 5.2. Root change detection
- [ ] Реализовать сравнение inode
- [ ] Реализовать сравнение mtime
- [ ] Реализовать сравнение ctime
- [ ] Реализовать флаг `needs_structural_rescan`

## 5.3. CLI
- [ ] Добавить CLI команду `scan-roots`

## 5.4. Тесты
- [ ] Unit tests для root discovery
- [ ] Integration test для root sync на sample tree

---

# 6. Inventory: structural scan

## 6.1. Structural scan service
- [ ] Реализовать `structural_scan_service.py`
- [ ] Реализовать scan одной root-папки
- [ ] Находить project_dirs по allowed extensions
- [ ] Находить project_files внутри найденных project_dirs
- [ ] Определять `dir_type` (premiere/avid/aftereffects/resolve/mixed/unknown)

## 6.2. Sync with DB
- [ ] Реализовать регистрацию новых project_dirs
- [ ] Реализовать обновление existing project_dirs
- [ ] Реализовать пометку missing project_dirs
- [ ] Реализовать регистрацию найденных project_files
- [ ] Реализовать first_seen / last_seen обновления

## 6.3. CLI
- [ ] Добавить CLI команду `scan-structure`
- [ ] Поддержать `scan-structure --root-id`
- [ ] Поддержать `scan-structure --path`

## 6.4. Тесты
- [ ] Unit tests structural scan
- [ ] Integration test structural scan with sample tree
- [ ] Fixture: nested project dirs
- [ ] Fixture: autosave files
- [ ] Fixture: cache dirs

---

# 7. Inventory: incremental scan project_dirs

## 7.1. Project dir scan service
- [ ] Реализовать `project_dir_scan_service.py`
- [ ] Сканировать только known project_dirs
- [ ] Выявлять new files
- [ ] Выявлять changed files
- [ ] Выявлять missing files
- [ ] Обновлять size/mtime/ctime

## 7.2. Decision support
- [ ] Реализовать `file_stat_service.py`
- [ ] Реализовать lightweight compare logic

## 7.3. CLI
- [ ] Добавить CLI команду `scan-project-dirs`

## 7.4. Тесты
- [ ] Unit tests file change detection
- [ ] Integration test incremental scan

---

# 8. Manual includes

## 8.1. DB + repo
- [ ] Реализовать CRUD для `manual_includes`

## 8.2. Service
- [ ] Реализовать `manual_include_scan_service.py`
- [ ] Поддержать file include
- [ ] Поддержать dir include
- [ ] Поддержать recursive
- [ ] Поддержать force_include
- [ ] Поддержать disabled include

## 8.3. CLI
- [ ] Добавить `include add-file`
- [ ] Добавить `include add-dir`
- [ ] Добавить `include list`
- [ ] Добавить `include disable`
- [ ] Добавить `include enable`

## 8.4. Тесты
- [ ] Unit tests manual include resolution
- [ ] Integration tests for manual includes

---

# 9. Policy engine

## 9.1. Rule loading
- [ ] Реализовать `rule_loader.py`
- [ ] Загружать extension rules из DB
- [ ] Загружать excluded patterns из DB
- [ ] Загружать global settings

## 9.2. Excludes
- [ ] Реализовать `exclude_matcher.py`
- [ ] Поддержать dirname rule
- [ ] Поддержать glob rule
- [ ] Поддержать substring rule
- [ ] Поддержать regex rule

## 9.3. Extension rules
- [ ] Реализовать `extension_policy_service.py`
- [ ] Проверка allowed extension
- [ ] Проверка max_size_bytes
- [ ] Реакция на oversize:
  - [ ] skip
  - [ ] warn
  - [ ] include

## 9.4. Final decision engine
- [ ] Реализовать `decision_engine.py`
- [ ] Учитывать manual include
- [ ] Учитывать force_include
- [ ] Учитывать excludes
- [ ] Учитывать extension rules
- [ ] Учитывать size rules
- [ ] Возвращать `FinalDecision`

## 9.5. Manifest
- [ ] Реализовать `manifest_builder.py`
- [ ] Формировать final file list
- [ ] Сохранять manifest на диск
- [ ] Сохранять machine-readable JSON manifest
- [ ] Сохранять human-readable summary

## 9.6. CLI
- [ ] Добавить `dry-run`
- [ ] Добавить `rules list`
- [ ] Добавить `rules add-extension`
- [ ] Добавить `rules update-extension`
- [ ] Добавить `rules add-exclude`
- [ ] Добавить `rules disable-exclude`

## 9.7. Тесты
- [ ] Unit tests exclude matcher
- [ ] Unit tests extension rules
- [ ] Unit tests oversize decisions
- [ ] Unit tests force include override
- [ ] Unit tests manifest builder

---

# 10. Restic integration

## 10.1. Adapter
- [ ] Реализовать `restic_runner.py`
- [ ] Реализовать `restic_adapter.py`
- [ ] Поддержать backup from manifest
- [ ] Поддержать env-based password/repo config
- [ ] Логировать stdout/stderr
- [ ] Парсить snapshot id

## 10.2. Backup service
- [ ] Реализовать `backup_service.py`
- [ ] Интегрировать final manifest
- [ ] Сохранять restic result
- [ ] Обрабатывать failures

## 10.3. Verify/retention placeholders
- [ ] Создать `verify_service.py` placeholder
- [ ] Создать `retention_service.py` placeholder
- [ ] Не реализовывать полноценный restore verify в v1

## 10.4. CLI
- [ ] Добавить `backup`
- [ ] Добавить `run-daily`
- [ ] Добавить `run-weekly` placeholder flow

## 10.5. Тесты
- [ ] Unit tests restic output parsing
- [ ] Integration tests with mocked restic runner

---

# 11. Runs, reports, logging

## 11.1. Run lifecycle
- [ ] Реализовать `run_service.py`
- [ ] Реализовать `start_run`
- [ ] Реализовать `append_run_event`
- [ ] Реализовать `finish_run`

## 11.2. Reports
- [ ] Реализовать `report_service.py`
- [ ] Генерировать JSON report
- [ ] Генерировать human-readable text report
- [ ] Генерировать HTML export report

## 11.3. Summaries
- [ ] Реализовать `summary_service.py`
- [ ] Собирать counts по new/changed/skipped/included

## 11.4. Logging
- [ ] Реализовать `logging_setup.py`
- [ ] Пер-run log files
- [ ] Console logging
- [ ] File logging

## 11.5. CLI
- [ ] Добавить `runs list`
- [ ] Добавить `runs show`
- [ ] Добавить `files list-skipped`

## 11.6. Тесты
- [ ] Tests for run lifecycle
- [ ] Tests for report generation

---

# 12. Locking / concurrency

## 12.1. File lock
- [ ] Реализовать `file_lock.py`
- [ ] Реализовать глобальный lock file

## 12.2. Run lock service
- [ ] Реализовать `run_lock.py`
- [ ] Проверка already running state
- [ ] Корректное завершение с `locked`

## 12.3. Интеграция
- [ ] Подключить lock в daily job
- [ ] Подключить lock в backup job
- [ ] Подключить lock в manual web actions

## 12.4. Тесты
- [ ] Unit tests locking
- [ ] Integration test double-run protection

---

# 13. Jobs orchestration

## 13.1. Daily job
- [ ] Реализовать `daily_job.py`
- [ ] Последовательность:
  - [ ] start run
  - [ ] acquire lock
  - [ ] scan roots
  - [ ] structural rescan changed/new roots
  - [ ] scan project dirs
  - [ ] scan manual includes
  - [ ] evaluate policy
  - [ ] build manifest
  - [ ] run restic backup
  - [ ] write report
  - [ ] finish run

## 13.2. Weekly job
- [ ] Реализовать `weekly_job.py`
- [ ] Full structural rescan all active roots
- [ ] Report only / maintenance summary
- [ ] Без реального restore verify

## 13.3. Scan / backup jobs
- [ ] Реализовать `scan_job.py`
- [ ] Реализовать `backup_job.py`
- [ ] Реализовать `verify_job.py` placeholder

## 13.4. CLI
- [ ] Проверить все job-команды end-to-end

## 13.5. Тесты
- [ ] Integration test daily job full flow
- [ ] Integration test weekly job flow

---

# 14. Flask Web UI

## 14.1. Flask app skeleton
- [ ] Реализовать `web/app.py`
- [ ] Подключить Jinja templates
- [ ] Подключить static css/js
- [ ] Добавить base layout

## 14.2. Dashboard
- [ ] Реализовать `routes_dashboard.py`
- [ ] Показать last scan
- [ ] Показать last backup
- [ ] Показать run status
- [ ] Показать counts
- [ ] Показать skipped oversized summary

## 14.3. Roots page
- [ ] Реализовать `routes_roots.py`
- [ ] Таблица roots
- [ ] Фильтры
- [ ] Кнопка rescan root

## 14.4. Project dirs page
- [ ] Реализовать `routes_dirs.py`
- [ ] Таблица project_dirs
- [ ] Связка с root

## 14.5. Rules page
- [ ] Реализовать `routes_rules.py`
- [ ] CRUD extension rules
- [ ] CRUD excludes
- [ ] Показ default `.aaf` = 100MB

## 14.6. Includes page
- [ ] Реализовать `routes_includes.py`
- [ ] CRUD manual includes

## 14.7. Runs page
- [ ] Реализовать `routes_runs.py`
- [ ] История запусков
- [ ] Просмотр run details
- [ ] Ссылка на export report

## 14.8. Actions
- [ ] Реализовать `routes_actions.py`
- [ ] `Run daily now`
- [ ] `Dry-run now`
- [ ] `Rescan root`
- [ ] `Backup now`

## 14.9. Exceptions / review
- [ ] Добавить страницу oversized skipped files
- [ ] Добавить страницу unrecognized extensions
- [ ] Добавить страницу manual override files

## 14.10. Тесты
- [ ] Integration tests basic Flask routes
- [ ] Template render tests
- [ ] Action route tests with mocked jobs

---

# 15. CLI

## 15.1. App skeleton
- [ ] Реализовать `cli/app.py`
- [ ] Разбить команды по файлам

## 15.2. Init commands
- [ ] `init-db`
- [ ] `seed-default-rules`

## 15.3. Scan commands
- [ ] `scan-roots`
- [ ] `scan-structure`
- [ ] `scan-project-dirs`
- [ ] `scan-manual`

## 15.4. Job commands
- [ ] `run-daily`
- [ ] `run-weekly`
- [ ] `backup`
- [ ] `dry-run`

## 15.5. Rules commands
- [ ] `rules list`
- [ ] `rules add-extension`
- [ ] `rules update-extension`
- [ ] `rules add-exclude`
- [ ] `rules disable-exclude`

## 15.6. Includes commands
- [ ] `include add-file`
- [ ] `include add-dir`
- [ ] `include list`
- [ ] `include disable`
- [ ] `include enable`

## 15.7. Info commands
- [ ] `runs list`
- [ ] `runs show`
- [ ] `roots list`
- [ ] `dirs list`
- [ ] `files list-skipped`
- [ ] `doctor`

## 15.8. Тесты
- [ ] CLI smoke tests
- [ ] CLI integration tests for main command groups

---

# 16. Экспорт отчётов

## 16.1. Форматы
- [ ] JSON report
- [ ] text report
- [ ] HTML export

## 16.2. Web download
- [ ] Добавить download link в Runs UI

## 16.3. CLI export
- [ ] Добавить `runs export --id`

---

# 17. Cron integration

## 17.1. Скрипты
- [ ] Создать `scripts/dev_run_cli.sh`
- [ ] Создать `scripts/dev_run_web.sh`
- [ ] Создать пример cron entry для daily run
- [ ] Создать пример cron entry для weekly run

## 17.2. Документация
- [ ] Описать cron setup в `deployment.md`
- [ ] Описать возможный переход на systemd позже

---

# 18. Документация

## 18.1. README
- [ ] Quick start
- [ ] Dev setup
- [ ] Run CLI
- [ ] Run Flask UI

## 18.2. ARCHITECTURE
- [ ] Слои
- [ ] Data flow
- [ ] Jobs flow

## 18.3. Policy doc
- [ ] Allowed extensions
- [ ] `.aaf` size policy = 100MB
- [ ] Autosave included
- [ ] Cache excluded
- [ ] Manual include overrides

## 18.4. Deployment doc
- [ ] Runtime paths
- [ ] cron
- [ ] restic local repo
- [ ] reverse proxy notes

## 18.5. CLI doc
- [ ] Список команд
- [ ] Примеры использования

## 18.6. Web UI doc
- [ ] Описание разделов
- [ ] Типовые сценарии

---

# 19. Финальные проверки перед тестами

## 19.1. End-to-end
- [ ] init-db works
- [ ] scan-roots works
- [ ] scan-structure works
- [ ] scan-project-dirs works
- [ ] manual include works
- [ ] dry-run builds manifest
- [ ] backup creates restic snapshot
- [ ] reports export correctly
- [ ] Flask UI works
- [ ] cron command works non-interactively

## 19.2. Acceptance check
- [ ] oversized `.aaf` > 100MB skipped with warning
- [ ] autosave included
- [ ] cache excluded
- [ ] manual force_include overrides policy
- [ ] lock prevents double run

---

# 20. Post-v1 backlog
- [ ] systemd timers
- [ ] restore verify
- [ ] better HTML reports
- [ ] richer file review UI
- [ ] retention automation
- [ ] rename/move heuristics
- [ ] optional hashing for selected files