# review_template.md

# Review Task
<название изменения>

## Review focus
- layering
- scope
- architecture boundaries
- risk
- diff size

## Check specifically
- нет ли прямого SQL вне repo/adapters
- нет ли Flask internals inside services
- не начат ли UI слишком рано
- нет ли скрытого scope creep
- разумен ли размер diff

## Output
- что хорошо
- critical issues
- important issues
- optional issues
- архитектурная оценка
- рекомендуемые правки