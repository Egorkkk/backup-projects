# task_selection_policy.md

## Общий принцип

Следующая задача выбирается не по удобству, а по критическому пути и зависимостям.

---

## Базовые правила выбора

1. Брать первую незавершённую задачу, от которой зависят следующие.
2. Не переходить к UI, пока не стабилизированы services/CLI для соответствующей области.
3. Не брать оптимизацию до рабочей baseline-версии.
4. Не брать полировку deployment/docs до базовой работоспособности функции.
5. Не брать две архитектурно связанные задачи одновременно.

---

## Приоритетный порядок

1. Foundation / tooling / runtime layout
2. Config
3. Constants
4. DB skeleton
5. Schema + init-db + seeds
6. Repositories
7. Domain DTO/enums
8. Filesystem / subprocess adapters
9. Root discovery
10. Structural scan
11. Incremental scan
12. Manual includes
13. Policy engine
14. Manifest builder
15. Restic adapter
16. Backup flow
17. Jobs / locking
18. Reports
19. CLI completeness
20. Flask UI
21. Cron docs / deployment docs
22. Final acceptance

---

## Что считать "слишком ранним"

Слишком рано:
- Flask templates до service contracts
- сложные report exports до working runs
- deployment polishing до локального end-to-end
- smart heuristics до простого deterministic scanning

---

## Как выбирать между двумя близкими задачами

Выбирать ту, которая:
- меньше зависит от ещё неготовых частей
- даёт проверяемый артефакт быстрее
- помогает следующему этапу
- снижает архитектурную неопределённость

---

## Когда нужен design-first

Использовать режим design-first перед реализацией, если задача касается:
- config structures
- schema
- repositories
- domain DTO
- service contracts
- CLI contract
- report formats
- web routes backed by services