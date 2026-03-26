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
- [x] Создать git-репозиторий
- [x] Создать ветку `main`
- [x] Создать базовую структуру папок проекта
- [x] Добавить `README.md`
- [x] Добавить `ARCHITECTURE.md`
- [x] Добавить `TASKS.md`
- [x] Добавить `ROADMAP.md`
- [x] Добавить `docs/tdz_v1.md`
- [x] Добавить `.gitignore`

## 0.2. Python-проект
- [x] Создать `pyproject.toml`
- [x] Зафиксировать Python version: 3.12+
- [x] Добавить зависимости:
  - [x] Flask
  - [x] Jinja2
  - [x] Typer
  - [x] SQLAlchemy или выбранный DB слой
  - [x] pydantic
  - [x] pytest
  - [x] ruff
  - [x] black
- [x] Добавить `requirements.txt` или lock-файл
- [x] Добавить `Makefile` с базовыми командами

## 0.3. Developer tooling
- [x] Настроить `ruff`
- [ ] Настроить `black`
- [x] Настроить `pytest`
- [x] Добавить базовую команду `make test`
- [x] Добавить базовую команду `make lint`
- [x] Добавить базовую команду `make format`

## 0.4. Runtime layout
- [x] Создать папки `runtime/logs`
- [x] Создать папки `runtime/manifests`
- [x] Создать папки `runtime/reports`
- [x] Создать папки `runtime/db`
- [x] Создать папки `runtime/locks`

---

# 1. Конфигурация

## 1.1. Конфиг-файлы
- [x] Создать `config/app.example.yaml`
- [x] Создать `config/rules.example.yaml`

## 1.2. Загрузка конфигурации
- [x] Реализовать `config.py`
- [x] Описать структуру app settings
- [x] Описать структуру raid roots
- [x] Описать runtime paths
- [x] Описать web settings
- [x] Описать DB settings
- [x] Описать restic settings
- [x] Описать scheduler mode (`cron`)
- [x] Реализовать валидацию конфигурации

## 1.3. Константы
- [x] Создать `constants.py`
- [x] Зафиксировать default `.aaf` limit = 100MB
- [x] Зафиксировать default allowed extensions
- [x] Зафиксировать default exclude patterns

---

# 2. База данных

## 2.1. DB skeleton
- [x] Выбрать окончательно DB слой
- [x] Реализовать `adapters/db/session.py`
- [x] Реализовать `adapters/db/schema.py`
- [x] Реализовать `adapters/db/sqlite_utils.py`

## 2.2. Таблицы
- [x] Создать таблицу `roots`
- [x] Создать таблицу `project_dirs`
- [x] Создать таблицу `project_files`
- [x] Создать таблицу `manual_includes`
- [x] Создать таблицу `extension_rules`
- [x] Создать таблицу `excluded_patterns`
- [x] Создать таблицу `settings`
- [x] Создать таблицу `runs`
- [x] Создать таблицу `run_events`
- [x] Создать таблицу `unrecognized_extensions`

## 2.3. Инициализация БД
- [x] Реализовать `init-db`
- [x] Реализовать seed default settings
- [x] Реализовать seed extension rules
- [x] Реализовать seed exclude rules
- [x] Реализовать проверку повторного init

## 2.4. Репозитории
- [x] Реализовать `roots_repo.py`
- [x] Реализовать `project_dirs_repo.py`
- [x] Реализовать `project_files_repo.py`
- [x] Реализовать `manual_includes_repo.py`
- [x] Реализовать `rules_repo.py`
- [x] Реализовать `runs_repo.py`
- [x] Реализовать `settings_repo.py`

## 2.5. Тесты БД
- [x] Написать integration tests для schema init
- [x] Написать integration tests для repositories CRUD
- [x] Написать test fixtures для sqlite

---

# 3. Domain layer

## 3.1. Enums
- [x] Описать enums для статусов roots
- [x] Описать enums для статусов project_dirs
- [x] Описать enums для статусов project_files
- [x] Описать enums для job types
- [x] Описать enums для oversize actions
- [x] Описать enums для include path type

## 3.2. DTO / models
- [x] Описать DTO для RootRecord
- [x] Описать DTO для ProjectDirRecord
- [x] Описать DTO для ProjectFileRecord
- [x] Описать DTO для ManualInclude
- [x] Описать DTO для ExtensionRule
- [x] Описать DTO для ExcludedPattern
- [x] Описать DTO для RunSummary
- [x] Описать DTO для CandidateFile
- [x] Описать DTO для FinalDecision
- [x] Описать DTO для ManifestResult

---

# 4. Адаптеры файловой системы

## 4.1. Listing/stat
- [x] Реализовать `dir_listing.py`
- [x] Реализовать `stat_reader.py`
- [x] Реализовать `path_utils.py`

## 4.2. Finder
- [x] Реализовать `file_finder.py`
- [x] Поддержать поиск по расширениям
- [x] Поддержать исключение path patterns
- [x] Поддержать ограничение на stay-on-fs
- [x] Поддержать ignore symlinks
- [x] Поддержать режим поиска только в одной root
- [x] Поддержать режим поиска только в одной project_dir

## 4.3. Командный runner
- [x] Реализовать `command_runner.py`
- [x] Реализовать общий subprocess wrapper
- [x] Логировать exit code/stdout/stderr
- [x] Добавить timeout support

---

# 5. Inventory: roots discovery

## 5.1. Root discovery service
- [x] Реализовать `root_discovery_service.py`
- [x] Реализовать `list_root_directories(raid_path)`
- [x] Реализовать фильтрацию только first-level dirs
- [x] Реализовать сравнение с DB
- [x] Реализовать создание новых roots
- [x] Реализовать пометку missing roots
- [x] Реализовать обновление last_seen

## 5.2. Root change detection
- [x] Реализовать сравнение inode
- [x] Реализовать сравнение mtime
- [x] Реализовать сравнение ctime
- [x] Реализовать флаг `needs_structural_rescan`

## 5.3. CLI
- [x] Добавить CLI команду `scan-roots`

## 5.4. Тесты
- [x] Unit tests для root discovery
- [x] Integration test для root sync на sample tree

---

# 6. Inventory: structural scan

## 6.1. Structural scan service
- [x] Реализовать `structural_scan_service.py`
- [x] Реализовать scan одной root-папки
- [x] Находить project_dirs по allowed extensions
- [x] Находить project_files внутри найденных project_dirs
- [x] Определять `dir_type` (premiere/avid/aftereffects/resolve/mixed/unknown)

## 6.2. Sync with DB
- [x] Реализовать регистрацию новых project_dirs
- [x] Реализовать обновление existing project_dirs
- [x] Реализовать пометку missing project_dirs
- [x] Реализовать регистрацию найденных project_files
- [x] Реализовать first_seen / last_seen обновления

## 6.3. CLI
- [x] Добавить CLI команду `scan-structure`
- [x] Поддержать `scan-structure --root-id`
- [x] Поддержать `scan-structure --path`

## 6.4. Тесты
- [x] Unit tests structural scan
- [x] Integration test structural scan with sample tree
- [x] Fixture: nested project dirs
- [x] Fixture: autosave files
- [x] Fixture: cache dirs

---

# 7. Inventory: incremental scan project_dirs

## 7.1. Project dir scan service
- [x] Реализовать `project_dir_scan_service.py`
- [x] Сканировать только known project_dirs
- [x] Выявлять new files
- [x] Выявлять changed files
- [x] Выявлять missing files
- [x] Обновлять size/mtime/ctime

## 7.2. Decision support
- [x] Реализовать `file_stat_service.py`
- [x] Реализовать lightweight compare logic

## 7.3. CLI
- [x] Добавить CLI команду `scan-project-dirs`

## 7.4. Тесты
- [x] Unit tests file change detection
- [x] Integration test incremental scan

---

# 8. Manual includes

## 8.1. DB + repo
- [x] Реализовать CRUD для `manual_includes`

## 8.2. Service
- [x] Реализовать `manual_include_scan_service.py`
- [x] Поддержать file include
- [x] Поддержать dir include
- [x] Поддержать recursive
- [x] Поддержать force_include
- [x] Поддержать disabled include

## 8.3. CLI
- [x] Добавить `include add-file`
- [x] Добавить `include add-dir`
- [x] Добавить `include list`
- [x] Добавить `include disable`
- [x] Добавить `include enable`

## 8.4. Тесты
- [x] Unit tests manual include resolution
- [x] Integration tests for manual includes

---

# 9. Policy engine

## 9.1. Rule loading
- [x] Реализовать `rule_loader.py`
- [x] Загружать extension rules из DB
- [x] Загружать excluded patterns из DB
- [x] Загружать global settings

## 9.2. Excludes
- [x] Реализовать `exclude_matcher.py`
- [x] Поддержать dirname rule
- [x] Поддержать glob rule
- [x] Поддержать substring rule
- [x] Поддержать regex rule

## 9.3. Extension rules
- [x] Реализовать `extension_policy_service.py`
- [x] Проверка allowed extension
- [x] Проверка max_size_bytes
- [x] Реакция на oversize:
  - [x] skip
  - [x] warn
  - [x] include

## 9.4. Final decision engine
- [x] Реализовать `decision_engine.py`
- [x] Учитывать manual include
- [x] Учитывать force_include
- [x] Учитывать excludes
- [x] Учитывать extension rules
- [x] Учитывать size rules
- [x] Возвращать `FinalDecision`

## 9.5. Manifest
- [x] Реализовать `manifest_builder.py`
- [x] Формировать final file list
- [x] Сохранять manifest на диск
- [x] Сохранять machine-readable JSON manifest
- [x] Сохранять human-readable summary

## 9.6. CLI
- [x] Добавить `dry-run`
- [x] Добавить `rules list`
- [x] Добавить `rules add-extension`
- [x] Добавить `rules update-extension`
- [x] Добавить `rules add-exclude`
- [x] Добавить `rules disable-exclude`

## 9.7. Тесты
- [x] Unit tests exclude matcher
- [x] Unit tests extension rules
- [x] Unit tests oversize decisions
- [x] Unit tests force include override
- [x] Unit tests manifest builder

---

# 10. Restic integration

## 10.1. Adapter
- [x] Реализовать `restic_runner.py`
- [x] Реализовать `restic_adapter.py`
- [x] Поддержать backup from manifest
- [x] Поддержать env-based password/repo config
- [x] Логировать stdout/stderr
- [x] Парсить snapshot id

## 10.2. Backup service
- [x] Реализовать `backup_service.py`
- [x] Интегрировать final manifest
- [x] Сохранять restic result
- [x] Обрабатывать failures

## 10.3. Verify/retention placeholders
- [x] Создать `verify_service.py` placeholder
- [x] Создать `retention_service.py` placeholder
- [x] Не реализовывать полноценный restore verify в v1

## 10.4. CLI
- [x] Добавить `backup`
- [x] Добавить `run-daily`
- [x] Добавить `run-weekly` placeholder flow

## 10.5. Тесты
- [x] Unit tests restic output parsing
- [x] Integration tests with mocked restic runner

---

# 11. Runs, reports, logging

## 11.1. Run lifecycle
- [x] Реализовать `run_service.py`
- [x] Реализовать `start_run`
- [x] Реализовать `append_run_event`
- [x] Реализовать `finish_run`

## 11.2. Reports
- [x] Реализовать `report_service.py`
- [x] Генерировать JSON report
- [x] Генерировать human-readable text report
- [x] Генерировать HTML export report

## 11.3. Summaries
- [x] Реализовать `summary_service.py`
- [x] Собирать counts по new/changed/skipped/included

## 11.4. Logging
- [x] Реализовать `logging_setup.py`
- [x] Пер-run log files
- [x] Console logging
- [x] File logging

## 11.5. CLI
- [x] Добавить `runs list`
- [x] Добавить `runs show`
- [x] Добавить `files list-skipped`

## 11.6. Тесты
- [x] Tests for run lifecycle
- [x] Tests for report generation

---

# 12. Locking / concurrency

## 12.1. File lock
- [x] Реализовать `file_lock.py`
- [x] Реализовать глобальный lock file

## 12.2. Run lock service
- [x] Реализовать `run_lock.py`
- [x] Проверка already running state
- [x] Корректное завершение с `locked`

## 12.3. Интеграция
- [x] Подключить lock в daily job
- [x] Подключить lock в backup job
- [x] Подключить lock в manual web actions

## 12.4. Тесты
- [x] Unit tests locking
- [x] Integration test double-run protection

---

# 13. Jobs orchestration

## 13.1. Daily job
- [x] Реализовать `daily_job.py`
- [x] Последовательность:
  - [x] start run
  - [x] acquire lock
  - [x] scan roots
  - [x] structural rescan changed/new roots
  - [x] scan project dirs
  - [x] scan manual includes
  - [x] evaluate policy
  - [x] build manifest
  - [x] run restic backup
  - [x] write report
  - [x] finish run

## 13.2. Weekly job
- [x] Реализовать `weekly_job.py`
- [x] Full structural rescan all active roots
- [x] Report only / maintenance summary
- [x] Без реального restore verify

## 13.3. Scan / backup jobs
- [x] Реализовать `scan_job.py`
- [x] Реализовать `backup_job.py`
- [x] Реализовать `verify_job.py` placeholder

## 13.4. CLI
- [x] Проверить все job-команды end-to-end

## 13.5. Тесты
- [x] Integration test daily job full flow
- [x] Integration test weekly job flow

---

# 14. Flask Web UI

## 14.1. Flask app skeleton
- [x] Реализовать `web/app.py`
- [x] Подключить Jinja templates
- [x] Подключить static css/js
- [x] Добавить base layout

## 14.2. Dashboard
- [x] Реализовать `routes_dashboard.py`
- [x] Показать last scan
- [x] Показать last backup
- [x] Показать run status
- [x] Показать counts
- [x] Показать skipped oversized summary

## 14.3. Roots page
- [x] Реализовать `routes_roots.py`
- [x] Таблица roots
- [x] Фильтры
- [x] Кнопка rescan root

## 14.4. Project dirs page
- [x] Реализовать `routes_dirs.py`
- [x] Таблица project_dirs
- [x] Связка с root

## 14.5. Rules page
- [x] Реализовать `routes_rules.py`
- [x] CRUD extension rules
- [x] CRUD excludes
- [x] Показ default `.aaf` = 100MB

## 14.6. Includes page
- [x] Реализовать `routes_includes.py`
- [x] CRUD manual includes

## 14.7. Runs page
- [x] Реализовать `routes_runs.py`
- [x] История запусков
- [x] Просмотр run details
- [x] Ссылка на export report

## 14.8. Actions
- [x] Реализовать `routes_actions.py`
- [x] `Run daily now`
- [x] `Dry-run now`
- [x] `Rescan root`
- [x] `Backup now`

## 14.9. Exceptions / review
- [x] Добавить страницу oversized skipped files
- [x] Добавить страницу unrecognized extensions
- [x] Добавить страницу manual override files

## 14.10. Тесты
- [x] Integration tests basic Flask routes
- [x] Template render tests
- [x] Action route tests with mocked jobs

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