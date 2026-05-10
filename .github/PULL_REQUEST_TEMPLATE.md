## Описание

<!-- Что сделано и зачем -->

## Тип изменения

- [ ] Bug fix
- [ ] New feature
- [ ] Refactoring
- [ ] Documentation

## Чеклист

- [ ] Тесты написаны / обновлены
- [ ] `pytest` проходит локально
- [ ] Если изменены defaults в `seed_target_db.py` — проверены `_BASE_ROWS` и `_GROWTH_PER_DAY` в `seed_metrics_history.py` (или используется `reset_db.py` с авто-деривацией)
- [ ] Если изменена схема БД — обновлены `scripts/schema.sql` и соответствующие коллекторы
