# Техническое задание

**Проект: система автоматического бэкапа project-файлов монтажных/VFX/постпродакшн-проектов**

## 1. Цель проекта

Разработать серверное приложение для Linux, которое:

автоматически обнаруживает project-файлы в известных и новых корневых папках проектов на нескольких XFS RAID;

ведёт локальный реестр структуры в SQLite;

ежедневно определяет новые и изменённые project-файлы;

передаёт их в restic для версионного бэкапа;

позволяет вручную добавлять в бэкап отдельные файлы и папки;

поддерживает настраиваемые правила отбора по расширениям, путям и размерам;

имеет лёгкий Web UI для операционного контроля;

имеет полноценный CLI для администрирования, запуска задач и диагностики.

Система должна быть спроектирована так, чтобы один разработчик мог последовательно реализовывать её в VS Code/Codex, не ломая архитектуру при расширении.

## 2. Исходные условия и ограничения

### 2.1. Исходные условия

ОС: Linux

Файловые системы: XFS

Носители: 4 RAID-массива по ~160 ТБ

В корне каждого RAID находятся крупные корневые папки проектов: сериалов, фильмов, шоу и т.д.

Строгой унифицированной структуры внутри проекта нет

Полный ежедневный find по всему объёму нежелателен

Нужен автоматический запуск раз в сутки

Нужен web-доступ и CLI

Бэкап-хранилище: restic repository

### 2.2. Типы файлов

По умолчанию должны поддерживаться project-файлы и обменные файлы, включая autosave:

.prproj

.avb

.avp

.aep

.aepx

.drp

.drt

.edl

.xml

.fcpxml

.aaf

Список должен быть расширяемым через конфиг/БД/UI/CLI.

### 2.3. Специальные ограничения

cache-папки не должны попадать в бэкап

autosave-файлы должны попадать в бэкап

.aaf должен поддерживаться, но с политикой ограничения по размеру

должны поддерживаться ручные include paths

должны поддерживаться size rules per extension

приложение должно работать без тяжёлой внешней БД

основная БД: SQLite

## 3. Цели v1

### 3.1. Что входит в v1

inventory scan корневых папок RAID

структурное обнаружение папок с project-файлами

реестр roots, project_dirs, project_files

ручные include paths

правила по расширениям и размерам

генерация manifest для backup

запуск restic backup

журнал запусков

Web UI

CLI

защита от параллельных запусков

планировщик через cron или systemd timer

dry-run режим

weekly rescan и verify pipeline

### 3.2. Что не входит в v1

кластеризация

многопользовательская ролевая модель

сложная очередь фоновых задач

realtime websocket UI

распределённый agent-based scan на нескольких серверах

hash каждого файла на каждом проходе

автоматическое “умное” распознавание rename/move как единого события


## 4. Архитектурные принципы

### 4.1. Один core — два интерфейса

Вся бизнес-логика должна жить в одном ядре приложения.

Поверх ядра работают:

CLI

Web UI/backend

Нельзя дублировать логику поиска, policy или backup отдельно в CLI и отдельно в Web UI.

### 4.2. Разделение на слои

Проект должен быть разделён на слои:

domain/models

repositories

services

jobs/workflows

adapters (filesystem, restic, sqlite, locking)

interfaces (cli, web)

### 4.3. Минимальная связность

Изменение UI не должно менять логику inventory. Изменение policy не должно требовать переписывания DB-слоя. Изменение restic adapter не должно ломать inventory scan.

### 4.4. Политика через данные, а не код

Список расширений, лимитов размера, ручных include и исключений должен храниться в конфигурации/SQLite, а не быть зашитым только в код.

### 4.5. Постепенная реализация

Каждый этап разработки должен давать рабочий результат. Нельзя строить систему так, чтобы до конца проекта нельзя было проверить базовый pipeline.

## 5. Общая логическая схема системы

### 5.1. Основной pipeline

Проверить RAID roots

Получить список корневых папок первого уровня

Обнаружить новые/исчезнувшие/изменившиеся root projects

Для новых или изменившихся roots выполнить structural scan

Обновить список project_dirs

Выполнить incremental scan известных project_dirs

Просканировать manual include paths

Собрать candidate files

Прогнать candidate files через policy engine

Сформировать final manifest

Запустить restic backup

Зафиксировать run log

Сформировать отчёт и warnings

### 5.2. Периодические режимы

Daily: inventory + policy + backup

Weekly: full structural rescan всех active roots

Weekly/Monthly: verification run / restore test / maintenance report

## 6. Выбор технологии реализации

### 6.1. Язык

Рекомендуется Python 3.12+.

Причины:

удобен для работы с SQLite

удобен для CLI

удобен для web backend

удобен для системных вызовов и subprocess

хорошо подходит для разработки в VS Code и с Codex

### 6.2. Библиотеки/стек

Рекомендуемый стек v1:

Python 3.12+

Typer — CLI

- FastAPI или Flask — web backend
- Предпочтительно FastAPI, если хочется API-ориентированную архитектуру
- Предпочтительно Flask, если хочется максимально просто
- Для v1 допустим любой из двух, но зафиксировать один

Jinja2 — шаблоны

- SQLAlchemy или sqlite3 + thin repository layer
- Для v1 лучше SQLAlchemy Core/ORM либо минималистичный repository abstraction

pydantic — схемы/настройки/DTO

pytest — тесты

ruff — lint

black — форматирование

mypy — по возможности частичная типизация

alembic не обязателен, но желателен, если используется SQLAlchemy

python-dotenv — при необходимости

стандартный subprocess для restic/find

pathlib

logging

### 6.3. Планировщик

Для production:

предпочтительно systemd timer

допустимо cron

Система должна быть совместима с обоими.


## 7. Роли интерфейсов

### 7.1. CLI

CLI — обязательный служебный интерфейс. Через CLI должны быть доступны все ключевые операции.

### 7.2. Web UI

Web UI — основной операторский интерфейс для ежедневной работы.

## 8. Функциональные требования

### 8.1. Inventory engine

#### 8.1.1. Обнаружение root-папок

Система должна:

принимать список RAID root paths

получать список директорий первого уровня на каждом root

считать эти директории root_projects

сверять их с SQLite-реестром

#### 8.1.2. Обнаружение новых root-папок

Если обнаружена новая папка первого уровня:

создать запись в БД

поставить её в очередь initial structural scan

#### 8.1.3. Обнаружение изменившихся root-папок

Система должна отслеживать:

path

inode

mtime

ctime

last_seen

Изменившаяся root-папка ставится в очередь structural rescan.

#### 8.1.4. Обнаружение отсутствующих root-папок

Если ранее известная root-папка исчезла:

пометить её как missing

не удалять из БД физически

### 8.2. Structural scan

Structural scan выполняется:

для новых root-папок

для изменившихся root-папок

в weekly full rescan

Задачи:

пройти дерево root-проекта

найти директории, содержащие project-файлы разрешённых расширений

зарегистрировать эти директории как project_dirs

зарегистрировать найденные project-файлы

### 8.3. Incremental scan известных project_dirs

Система должна:

проверять known project_dirs

если директория изменилась — пересканировать её

выявлять:

новые файлы

изменившиеся файлы

исчезнувшие файлы

Для v1 сравнение выполнять по:

path

size

mtime

ctime

### 8.4. Manual includes

Система должна поддерживать ручное добавление:

конкретного файла

конкретной директории

Атрибуты:

path_type: file/dir

recursive

force_include

enabled

comment

Manual include paths должны:

сканироваться независимо от автообнаружения

участвовать в candidate set

при force_include иметь приоритет над policy, кроме жёстких системных ошибок

### 8.5. Policy engine

Policy engine должен уметь:

#### 8.5.1. Rules by extension

Для каждого расширения:

enabled / disabled

max_size_bytes (nullable)

action_if_oversize: skip, warn, include

#### 8.5.2. Global rules

default max size (nullable)

follow symlink: false по умолчанию

stay on expected filesystem: true по умолчанию

exclude mountpoints inside tree: true по умолчанию

weekly full structural rescan interval

log skipped oversize files: true

#### 8.5.3. Exclusion rules

Поддержка исключений:

по имени директории

по glob

по подстроке пути

опционально по regex

Примеры типовых исключений:

Cache

Render Cache

Media Cache

Preview Files

и аналогичные

#### 8.5.4. Autosave

Autosave не должен исключаться автоматически только по имени. Если autosave соответствует allowed extensions и не попадает под исключение cache/misc, он должен бэкапиться.

#### 8.5.5. AAF policy

AAF должен поддерживаться, но с отдельным size-limit rule.

Oversized AAF должны:

не попадать в backup по умолчанию

фиксироваться в skipped/warnings log

### 8.6. Manifest builder

Система должна уметь:

собирать candidate files из auto-discovered и manual includes

прогонять их через policy

строить final manifest

сохранять manifest на диск как служебный артефакт run

Manifest должен содержать:

полный список файлов к backup

источник файла: auto/manual

применённое правило

предупреждения

### 8.7. Restic integration

Система должна:

инициализировать или использовать существующий restic repository

запускать backup только final manifest

фиксировать результат в БД

сохранять stdout/stderr и exit code

сохранять snapshot id, если доступен

Не требуется собственная реализация дедупликации — она обеспечивается restic.

### 8.8. Logging and run history

Система должна вести историю запусков:

scan run

backup run

verify run

maintenance run

Логировать:

start/end time

duration

status

counts

warnings

errors

snapshot id

skipped oversized files count

manual override count

### 8.9. Dry-run

Система должна поддерживать dry-run режим для:

scan

policy evaluation

backup plan

Dry-run не должен менять внешнее состояние backup-хранилища.

### 8.10. Locking / concurrency

Система должна исключать параллельный запуск несовместимых задач.

Обязательные механизмы:

file lock / flock

состояние active job в БД

повторный запуск должен завершаться с понятным сообщением


## 9. Требования к Web UI

### 9.1. Общие требования

лёгкий и минималистичный

без тяжёлого SPA

server-rendered или очень лёгкий JS

должен работать удалённо через reverse proxy

должен быть пригоден для внутренней сети

### 9.2. Разделы UI

#### 9.2.1. Dashboard

Отображает:

время последнего scan

время последнего backup

статус последнего запуска

число active roots

число active project_dirs

число candidate files

число skipped oversized files

число ошибок за последние N запусков

#### 9.2.2. Roots

Таблица:

raid

path

status

inode

mtime/ctime

last_seen

last_structural_scan

actions: rescan / disable / inspect

#### 9.2.3. Project directories

Таблица:

path

root project

type

file_count

status

last_scan

actions

#### 9.2.4. Files / Review

Таблица:

path

extension

size

source

status

last_seen

backed up / skipped

reason

#### 9.2.5. Manual includes

CRUD:

add file

add dir

enable/disable

recursive

force_include

comment

#### 9.2.6. Rules

CRUD:

extensions

size limits

oversize action

excluded patterns

global settings subset

#### 9.2.7. Runs / Logs

История запусков:

job type

start/end

status

duration

files included

files skipped

warnings/errors

link to detailed report

#### 9.2.8. Exceptions / Review

Отдельная страница:

oversized skipped files

unrecognized extensions

files included via manual override

missing roots / missing dirs

### 9.3. UI actions

UI должен позволять:

запустить scan now

запустить structural rescan

запустить backup now

dry-run now

re-scan one root

re-scan one project dir

добавить include

изменить rule

просмотреть последний manifest/report

### 9.4. Безопасность доступа

В v1 не требуется собственная сложная auth-система приложения, но нужно предусмотреть:

запуск за reverse proxy

возможность ограничить доступ basic auth / VPN / IP allowlist

bind на localhost или внутренний интерфейс

## 10. Требования к CLI

CLI должен покрывать все основные операции.

### 10.1. Примерная группа команд

app init-db

app seed-default-rules

app scan-roots

app scan-structure

app scan-project-dirs

app scan-manual

app run-daily

app run-weekly

app backup

app dry-run

app verify

app include add-file

app include add-dir

app include list

app include disable

app rules list

app rules add-extension

app rules update-extension

app rules add-exclude

app runs list

app runs show

app roots list

app dirs list

app files list-skipped

app doctor

### 10.2. Требования к CLI

понятные help-сообщения

минимальное количество магии

предсказуемые exit codes

возможность non-interactive использования

пригодность для cron/systemd


## 11. Структура данных и SQLite schema

Ниже — логическая схема. Имена таблиц могут быть уточнены, но сущности должны сохраниться.

### 11.1. roots

Поля:

id

raid_name

raid_path

path

name

inode

mtime

ctime

first_seen_at

last_seen_at

last_scanned_at

last_structural_scan_at

status (active, missing, disabled)

notes

### 11.2. project_dirs

Поля:

id

root_id

path

inode

mtime

ctime

dir_type (premiere, avid, aftereffects, resolve, mixed, unknown)

first_seen_at

last_seen_at

last_scanned_at

status (active, missing, disabled)

notes

### 11.3. project_files

Поля:

id

project_dir_id nullable

manual_include_id nullable

path

filename

extension

inode

size

mtime

ctime

first_seen_at

last_seen_at

last_backed_up_at

status (active, missing, skipped, disabled)

last_decision

last_decision_reason

### 11.4. manual_includes

Поля:

id

path

path_type (file, dir)

recursive

force_include

enabled

comment

created_at

updated_at

### 11.5. extension_rules

Поля:

id

extension

enabled

max_size_bytes nullable

action_if_oversize (skip, warn, include)

comment

created_at

updated_at

### 11.6. excluded_patterns

Поля:

id

pattern_type (dirname, glob, substring, regex)

pattern

enabled

comment

created_at

updated_at

### 11.7. settings

Поля:

key

value

updated_at

Примеры ключей:

default_max_size_bytes

follow_symlinks

stay_on_same_fs

weekly_rescan_day

lock_timeout_sec

report_dir

manifest_dir

### 11.8. runs

Поля:

id

job_type (daily, weekly, scan, backup, verify, manual)

started_at

finished_at

duration_sec

status (running, success, warning, failed, locked)

dry_run

summary_json

manifest_path

log_path

restic_snapshot_id

error_text

### 11.9. run_events

Поля:

id

run_id

level

event_type

message

details_json

created_at

### 11.10. unrecognized_extensions

Поля:

id

path

extension

root_id nullable

first_seen_at

last_seen_at

seen_count

## 12. Служебные файлы и артефакты

На диске должны храниться:

config file

sqlite db

logs

per-run reports

manifests

temp/lock files


## 13. Предлагаемая структура проекта

Ниже — рекомендуемая структура папок репозитория.

- backup-projects/
- ├─ README.md
- ├─ ARCHITECTURE.md
- ├─ ROADMAP.md
- ├─ TASKS.md
- ├─ .env.example
- ├─ pyproject.toml
- ├─ requirements.txt
- ├─ Makefile
- ├─ .gitignore
- ├─ config/
- │  ├─ app.example.yaml
- │  └─ rules.example.yaml
- ├─ scripts/
- │  ├─ dev_run_web.sh
- │  ├─ dev_run_cli.sh
- │  ├─ init_dev_db.sh
- │  └─ systemd/
- │     ├─ backup-projects.service
- │     ├─ backup-projects.timer
- │     └─ backup-projects-weekly.timer
- ├─ docs/
- │  ├─ tdz_v1.md
- │  ├─ db_schema.md
- │  ├─ cli.md
- │  ├─ web_ui.md
- │  ├─ policy.md
- │  ├─ workflows.md
- │  └─ deployment.md
- ├─ src/
- │  └─ backup_projects/
- │     ├─ __init__.py
- │     ├─ main.py
- │     ├─ config.py
- │     ├─ logging_setup.py
- │     ├─ constants.py
- │     ├─ domain/
- │     │  ├─ __init__.py
- │     │  ├─ enums.py
- │     │  ├─ models.py
- │     │  ├─ dtos.py
- │     │  └─ decisions.py
- │     ├─ repositories/
- │     │  ├─ __init__.py
- │     │  ├─ base.py
- │     │  ├─ roots_repo.py
- │     │  ├─ project_dirs_repo.py
- │     │  ├─ project_files_repo.py
- │     │  ├─ manual_includes_repo.py
- │     │  ├─ rules_repo.py
- │     │  ├─ runs_repo.py
- │     │  └─ settings_repo.py
- │     ├─ services/
- │     │  ├─ __init__.py
- │     │  ├─ inventory/
- │     │  │  ├─ root_discovery_service.py
- │     │  │  ├─ structural_scan_service.py
- │     │  │  ├─ project_dir_scan_service.py
- │     │  │  ├─ manual_include_scan_service.py
- │     │  │  └─ file_stat_service.py
- │     │  ├─ policy/
- │     │  │  ├─ rule_loader.py
- │     │  │  ├─ exclude_matcher.py
- │     │  │  ├─ extension_policy_service.py
- │     │  │  ├─ decision_engine.py
- │     │  │  └─ manifest_builder.py
- │     │  ├─ backup/
- │     │  │  ├─ restic_adapter.py
- │     │  │  ├─ backup_service.py
- │     │  │  ├─ verify_service.py
- │     │  │  └─ retention_service.py
- │     │  ├─ runs/
- │     │  │  ├─ run_service.py
- │     │  │  ├─ report_service.py
- │     │  │  └─ summary_service.py
- │     │  └─ maintenance/
- │     │     ├─ weekly_rescan_service.py
- │     │     ├─ doctor_service.py
- │     │     └─ cleanup_service.py
- │     ├─ jobs/
- │     │  ├─ __init__.py
- │     │  ├─ daily_job.py
- │     │  ├─ weekly_job.py
- │     │  ├─ scan_job.py
- │     │  ├─ backup_job.py
- │     │  └─ verify_job.py
- │     ├─ adapters/
- │     │  ├─ __init__.py
- │     │  ├─ db/
- │     │  │  ├─ session.py
- │     │  │  ├─ schema.py
- │     │  │  ├─ migrations.py
- │     │  │  └─ sqlite_utils.py
- │     │  ├─ fs/
- │     │  │  ├─ file_finder.py
- │     │  │  ├─ dir_listing.py
- │     │  │  ├─ path_utils.py
- │     │  │  └─ stat_reader.py
- │     │  ├─ process/
- │     │  │  ├─ command_runner.py
- │     │  │  └─ restic_runner.py
- │     │  ├─ locking/
- │     │  │  ├─ file_lock.py
- │     │  │  └─ run_lock.py
- │     │  └─ clock/
- │     │     └─ time_provider.py
- │     ├─ cli/
- │     │  ├─ __init__.py
- │     │  ├─ app.py
- │     │  ├─ commands_init.py
- │     │  ├─ commands_scan.py
- │     │  ├─ commands_backup.py
- │     │  ├─ commands_rules.py
- │     │  ├─ commands_includes.py
- │     │  ├─ commands_runs.py
- │     │  └─ commands_doctor.py
- │     ├─ web/
- │     │  ├─ __init__.py
- │     │  ├─ app.py
- │     │  ├─ routes_dashboard.py
- │     │  ├─ routes_roots.py
- │     │  ├─ routes_dirs.py
- │     │  ├─ routes_rules.py
- │     │  ├─ routes_includes.py
- │     │  ├─ routes_runs.py
- │     │  ├─ routes_actions.py
- │     │  ├─ templates/
- │     │  │  ├─ base.html
- │     │  │  ├─ dashboard.html
- │     │  │  ├─ roots.html
- │     │  │  ├─ project_dirs.html
- │     │  │  ├─ files_review.html
- │     │  │  ├─ rules.html
- │     │  │  ├─ includes.html
- │     │  │  ├─ runs.html
- │     │  │  └─ exceptions.html
- │     │  └─ static/
- │     │     ├─ css/
- │     │     │  └─ app.css
- │     │     └─ js/
- │     │        └─ app.js
- │     └─ tests/
- │        ├─ conftest.py
- │        ├─ unit/
- │        │  ├─ test_policy_engine.py
- │        │  ├─ test_extension_rules.py
- │        │  ├─ test_exclude_matcher.py
- │        │  ├─ test_root_discovery.py
- │        │  ├─ test_structural_scan.py
- │        │  ├─ test_manifest_builder.py
- │        │  └─ test_locking.py
- │        ├─ integration/
- │        │  ├─ test_sqlite_repos.py
- │        │  ├─ test_daily_job_flow.py
- │        │  ├─ test_manual_includes.py
- │        │  ├─ test_restic_adapter_mock.py
- │        │  └─ test_web_routes.py
- │        └─ fixtures/
- │           ├─ sample_tree_01/
- │           ├─ sample_tree_02/
- │           ├─ rules/
- │           └─ sqlite/
- └─ runtime/
- ├─ logs/
- ├─ manifests/
- ├─ reports/
- ├─ locks/
- └─ db/

## 14. Назначение основных модулей

### 14.1. config.py

Отвечает за:

загрузку yaml/env settings

валидацию путей

предоставление app settings

Не должен содержать бизнес-логику.

### 14.2. domain/

Описывает:

enums

dataclasses / entities / DTO

decision result objects

Не должен зависеть от web/cli.

### 14.3. repositories/

Описывают работу с SQLite через abstraction layer.

Каждый репозиторий отвечает только за свою сущность.

### 14.4. services/inventory/

Вся логика обнаружения и сканирования.

### 14.5. services/policy/

Вся логика принятия решения:

allowed/disallowed

size rules

exclude rules

final decision

manifest generation

### 14.6. services/backup/

Интеграция с restic и backup workflow.

### 14.7. services/runs/

Управление run lifecycle:

создать run

записать события

закрыть run

построить summary/report

### 14.8. jobs/

Оркестрация больших сценариев:

daily job

weekly job

backup job

verify job

Jobs используют services, но не содержат низкоуровневую логику поиска или SQL.

### 14.9. adapters/

Изоляция внешнего мира:

filesystem

subprocess

locking

sqlite session

### 14.10. cli/

Тонкий слой, который вызывает jobs/services.

### 14.11. web/

Тонкий слой web-маршрутов и рендеринга.


## 15. Описание ключевых функций без реализации

Ниже — обязательные логические функции/методы, которые должны быть реализованы как отдельные единицы.

### 15.1. Inventory

list_root_directories(raid_path) -> list[Path]

sync_roots_with_db(raid_name, raid_path, found_paths) -> RootSyncResult

detect_root_changes(root_record, current_stat) -> bool

scan_root_structure(root_path, allowed_extensions) -> StructuralScanResult

register_project_dir(root_id, dir_path, dir_stat, dir_type)

scan_project_dir(dir_path, allowed_extensions) -> ProjectDirScanResult

sync_project_files(project_dir_id, discovered_files) -> FileSyncResult

scan_manual_include(path_config) -> ManualIncludeScanResult

### 15.2. Policy

load_extension_rules()

load_excluded_patterns()

is_path_excluded(path) -> ExcludeDecision

is_extension_allowed(extension) -> bool

resolve_size_policy(extension, file_size) -> SizeDecision

evaluate_candidate(file_candidate) -> FinalDecision

build_manifest(candidates) -> ManifestResult

### 15.3. Backup

run_restic_backup(manifest_path) -> ResticBackupResult

parse_restic_output(stdout, stderr) -> ResticParsedResult

run_verify() -> VerifyResult

### 15.4. Runs

start_run(job_type, dry_run=False) -> RunContext

append_run_event(run_id, level, message, details)

finish_run(run_id, status, summary)

write_report(run_id, report_payload) -> Path

### 15.5. Locking

acquire_global_lock(job_name) -> LockHandle

release_global_lock(lock_handle)

is_job_running() -> bool

### 15.6. CLI

handle_run_daily()

handle_run_weekly()

handle_add_include_file()

handle_add_include_dir()

handle_add_extension_rule()

handle_update_extension_rule()

### 15.7. Web

dashboard_view()

roots_list_view()

project_dirs_view()

rules_view()

includes_view()

runs_view()

trigger_daily_run_action()

trigger_rescan_root_action(root_id)

trigger_backup_now_action()

## 16. Конфиг-файлы

Нужны как минимум:

### 16.1. app.yaml

Содержит:

список RAID roots

пути runtime

DB path

restic repo location/env references

web bind host/port

scheduler mode

logging level

### 16.2. rules.yaml или seed rules

Содержит:

default extension rules

default excluded patterns

default global settings

В v1 допустимо:

initial seed в YAML

затем перенос в SQLite при init


## 17. Default policy для v1

### 17.1. Default allowed extensions

Включить по умолчанию:

prproj

avb

avp

aep

aepx

drp

drt

edl

xml

fcpxml

aaf

### 17.2. Default excludes

Исключить типовые cache-папки:

Cache

Render Cache

Media Cache

Preview Files

Исключения должны настраиваться.

### 17.3. Default size policy

Для большинства project extensions — без жёсткого лимита либо очень высокий

Для .aaf — отдельный лимит, configurable

Oversized .aaf по умолчанию: skip + warning

## 18. Форматы отчётов

Каждый запуск должен создавать:

machine-readable JSON report

human-readable text/HTML summary

JSON report должен содержать:

timestamps

counts

discovered roots

new dirs

changed files

skipped files

rules applied

restic result

errors

## 19. План разработки по этапам

Это самый важный раздел для одиночной реализации.

Этап 0. Подготовка репозитория

Сделать:

repo init

pyproject

базовую структуру папок

README

TASKS.md

ROADMAP.md

минимальный run skeleton

tooling: ruff/black/pytest

Результат:

чистый каркас проекта

Этап 1. Configuration + SQLite skeleton

Сделать:

config loading

DB schema init

repositories skeleton

seed default settings/rules

CLI команду init-db

Результат:

приложение умеет поднять БД и заполнить default rules

Этап 2. Root discovery

Сделать:

список RAID roots

listing first-level directories

sync with DB

статусы new/missing/active

CLI scan-roots

Результат:

приложение умеет обнаружить верхние папки проектов

Этап 3. Structural scan

Сделать:

scan одной root-папки

найти project_dirs

найти project_files

записать в DB

CLI scan-structure

Результат:

приложение умеет построить первичную структуру одного проекта

Этап 4. Incremental project_dir scan

Сделать:

scan known project dirs

detect new/changed/missing files

sync in DB

Результат:

приложение умеет обновлять inventory без полного сканирования всего массива

Этап 5. Manual includes

Сделать:

DB table

CRUD в CLI

scan manual include paths

Результат:

можно вручную добавлять пути в бэкап

Этап 6. Policy engine

Сделать:

extension rules

exclude rules

size rules

decision engine

manifest builder

dry-run decision report

Результат:

приложение умеет принимать решение, что реально бэкапить

Этап 7. Restic integration

Сделать:

adapter

backup command runner

parse result

сохранять snapshot info

dry-run compatible flow

Результат:

можно сделать реальный backup

Этап 8. Daily job orchestration

Сделать:

daily_job

locks

runs table

logs/report

CLI run-daily

Результат:

полный рабочий nightly pipeline

Этап 9. Weekly job

Сделать:

full structural rescan

verify run

cleanup/report

CLI run-weekly

Результат:

система готова к долговременной эксплуатации

Этап 10. Web UI

Сначала:

dashboard

roots

runs

includes

rules

Потом:

actions

exceptions/review

project dirs

files review

Результат:

операторский интерфейс

Этап 11. Deployment

Сделать:

systemd service/timer

runtime dirs

reverse proxy notes

backup/restore ops guide

Результат:

готовность к production тестам


## 20. Рекомендуемая очередность задач внутри этапов

Для одиночной разработки идти в таком порядке:

каркас проекта

config

sqlite schema

roots discovery

structural scan

project_dir incremental scan

manual includes

policy engine

manifest

restic adapter

daily orchestration

lock

logging/reporting

weekly job

web ui

deployment

Не начинать Web UI раньше, чем:

есть DB

есть roots scan

есть runs

есть includes/rules CRUD хотя бы в CLI

## 21. Правила проектирования для VS Code / Codex

Так как значительная часть кода будет писаться в VS Code и Codex, нужно заранее ограничить риск расползания архитектуры.

### 21.1. Каждый модуль — узкая зона ответственности

Codex должен получать задачи уровня:

“реализуй roots_repo”

“реализуй structural_scan_service”

“добавь CLI команду list includes”

Нельзя давать слишком расплывчатые задания типа:

“сделай весь backend”

### 21.2. Сначала интерфейсы и DTO

Перед реализацией каждого крупного блока сначала фиксировать:

входы

выходы

dataclasses/DTO

ошибки

### 21.3. Не писать web и core одновременно в одном PR

Сначала core/service, потом CLI/web слой.

### 21.4. Любая логика сначала покрывается unit/integration tests

Минимум для:

policy engine

exclude matcher

root discovery

structural scan

manifest builder

### 21.5. Не смешивать subprocess и business logic

Команда restic должна быть изолирована в adapter.

### 21.6. Не допускать hidden side effects

Каждая job должна явно:

читать

писать

логировать

возвращать результат

## 22. Минимальный скелет документов в репозитории

README.md

Кратко:

что делает проект

как поднять dev среду

как инициализировать БД

как запустить scan

как запустить web

ARCHITECTURE.md

diagram/slim text architecture

services and boundaries

data flow

TASKS.md

Чеклист разработки по этапам

ROADMAP.md

План v1, v1.1, v2

db_schema.md

Описание таблиц и связей

policy.md

Правила отбора файлов

deployment.md

systemd, reverse proxy, runtime paths

## 23. Требования к тестированию

### 23.1. Unit tests

Обязательны для:

rules parsing

exclude matcher

size decision

final decision

root discovery logic

manifest builder

locking

### 23.2. Integration tests

Обязательны для:

sqlite repositories

daily job flow на sample tree

manual includes

restic adapter mock

web basic routes

### 23.3. Test fixtures

Создать искусственные sample trees:

root with premiere project

root with nested ae project

autosave files

cache dirs

oversized aaf

manual include only path

## 24. Требования к логированию

Логирование должно быть:

структурированным

читаемым

пригодным для grep и разбора

Уровни:

INFO

WARNING

ERROR

DEBUG

Каждый run должен иметь свой log/report file.

## 25. Требования к производительности v1

### 25.1. Не делать полный ежедневный scan всего массива

Сканирование должно быть двухуровневым:

roots

known project dirs

structural rescan only when needed

### 25.2. Не считать hash всего подряд

Для v1 не делать тотальное хеширование.

### 25.3. Минимизировать обход

Manual include и changed roots/dirs сканировать точечно.


## 26. Требования к отказоустойчивости

Система должна:

не падать целиком из-за одного битого пути

логировать permission errors

продолжать scan по другим путям

помечать проблемный path в report

Если restic завершился ошибкой:

run получает status failed

manifest и report сохраняются

данные inventory не откатываются назад полностью

## 27. Деплой и runtime layout

На production-сервере предусмотреть структуру:

- /opt/backup-projects/           # код приложения
- /etc/backup-projects/           # конфиги
- /var/lib/backup-projects/       # sqlite db, state
- /var/log/backup-projects/       # logs
- /var/lib/backup-projects/manifests/
- /var/lib/backup-projects/reports/
- /run/backup-projects/           # locks, pid/runtime

## 28. Планировщик

### 28.1. Daily timer

Запуск:

## 1. раз в сутки ночью

Сценарий:

run-daily

### 28.2. Weekly timer

Запуск:

## 1. раз в неделю

Сценарий:

run-weekly

## 29. MVP-критерии готовности

Система считается готовой к первым тестам, если:

БД инициализируется

roots обнаруживаются

structural scan работает

project_dirs и files попадают в DB

manual includes работают

extension/size/exclude policy работает

manifest строится

restic backup запускается

daily job работает из CLI

есть базовый Web UI с Dashboard, Roots, Rules, Includes, Runs

## 30. Критерии приёмки v1

Можно проинициализировать систему с чистого состояния

Можно выполнить первичный scan RAID roots

Можно выполнить структурный scan и получить список project dirs

Можно выполнить ежедневный run без полного пересканирования всего массива

Можно вручную добавить путь в backup

Можно изменить список расширений

Можно задать лимит для .aaf

Oversized .aaf логируются и не попадают в backup по default rule

Autosave попадают в backup

Cache-папки исключаются

Restic snapshot создаётся

Есть история запусков

Есть блокировка параллельных запусков

Есть web UI для контроля

Есть CLI для всех основных операций


