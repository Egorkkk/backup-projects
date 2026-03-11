# testing_policy.md

## Цель

Тесты должны подтверждать корректность слоя и не тащить лишнюю связность.

---

## Общие правила

1. Для каждого нового важного слоя нужен свой тип проверки.
2. Не тащить Flask UI в тесты service-логики.
3. Не тестировать CLI как замену unit/integration tests там, где можно проверить сервис напрямую.
4. Тесты должны отражать реальный сценарий этапа roadmap.

---

## По типам задач

### Config / constants
Проверять:
- валидная загрузка
- невалидный конфиг
- default values
- path validation if applicable

### DB schema / init
Проверять:
- создание схемы
- повторный init
- seed defaults
- корректность sqlite fixture

### Repositories
Проверять:
- CRUD
- upsert/lookup where applicable
- handling missing rows
- expected status transitions where applicable

### Services
Проверять:
- ожидаемые inputs/outputs
- edge cases
- decision logic
- side effects через controlled adapters/mocks

### Filesystem scanning
Проверять:
- sample trees
- nested dirs
- autosave
- cache dirs
- missing files
- changed stat cases

### CLI
Проверять:
- smoke запуск
- ключевые команды
- базовые аргументы
- корректный exit behavior

### Flask UI
Проверять:
- route availability
- template rendering
- service integration boundaries
- отсутствие прямого DB access в route logic

---

## Предпочтительная стратегия

- unit tests для чистой логики
- integration tests для DB/repositories
- fixture-driven tests для scanning
- mocked integration для restic / subprocess
- smoke tests для CLI

---

## Чего избегать

- e2e вместо нормальных локальных tests на ранних этапах
- слишком хрупких snapshot tests без пользы
- web-heavy tests до готовности service layer
- тестов, которые не проверяют реальную бизнес-ценность текущего этапа

---

## Definition of useful test

Полезный тест:
- ловит архитектурно значимую поломку
- помогает безопасно продолжать следующий этап
- быстро выполняется
- читаем
- изолирован ровно настолько, насколько нужно