# Быстрый старт 🚀

## Установка

```bash
poetry install
```

Создайте `.env` файл с вашим токеном:
```
TRAVELPAYOUTS_TOKEN=ваш_токен_здесь
```

## Использование

### 1️⃣ Автоматический поиск (РЕКОМЕНДУЕТСЯ)

API сам найдёт все доступные направления:

```bash
# Полный маршрут: origin → все направления → destination
poetry run python collect_flights.py MOW BKK \
  --leg1-dates 2026-02-15 2026-02-20 \
  --leg2-dates 2026-02-25 2026-03-05

# Только исходящие рейсы из города
poetry run python collect_flights.py MOW \
  --leg1-dates 2026-02-15 2026-02-20
```

**Результат:**
- ⚡ Минимум API запросов (только по датам)
- 🌍 Все доступные направления (до 1000 маршрутов)
- ⏱️ Быстрое выполнение

### 2️⃣ Ручной выбор промежуточных городов

Если хотите ограничить поиск конкретными городами:

```bash
poetry run python collect_flights.py MOW BKK \
  --leg1-dates 2026-02-15 2026-02-20 \
  --leg2-dates 2026-02-25 2026-03-05 \
  --intermediate IST DXB AUH
```

### 2️⃣.5 Непрямые перелеты (с пересадками)

Для поиска рейсов с пересадками используйте флаг `--allow-indirect`:

```bash
poetry run python collect_flights.py MOW BKK \
  --leg1-dates 2026-02-15 2026-02-20 \
  --leg2-dates 2026-02-25 2026-03-05 \
  --allow-indirect
```

### 3️⃣ Анализ собранных данных

```bash
# Базовый анализ
poetry run python aggregate_flights.py data/flights_MOW_BKK_*.json

# С фильтрами
poetry run python aggregate_flights.py data/flights_MOW_BKK_*.json \
  --min-stay 3 \
  --max-stay 7 \
  --top 20

# Фильтр по конкретным датам вылета
poetry run python aggregate_flights.py data/flights_MOW_BKK_*.json \
  --leg1-date 2026-02-19 \
  --leg2-from 2026-02-25 \
  --leg2-to 2026-02-28

# С сохранением результатов
poetry run python aggregate_flights.py data/flights_MOW_BKK_*.json \
  --output results/best_options.json
```

## Популярные коды городов (IATA)

### Россия
- `MOW` - Москва
- `LED` - Санкт-Петербург
- `KZN` - Казань
- `AER` - Сочи

### Европа и Ближний Восток
- `IST` - Стамбул 🔥 (популярный хаб)
- `DXB` - Дубай 🔥 (популярный хаб)
- `AUH` - Абу-Даби 🔥 (популярный хаб)
- `DOH` - Доха 🔥 (популярный хаб)
- `BCN` - Барселона
- `PAR` - Париж

### Азия
- `BKK` - Бангкок
- `HKT` - Пхукет
- `SIN` - Сингапур
- `DEL` - Дели
- `GOI` - Гоа

## Советы 💡

1. **Используйте автоматический поиск** - не нужно гадать, какие промежуточные города выбрать
2. **Расширяйте диапазон дат** - больше вариантов для выбора
3. **Фильтруйте при анализе** - собирайте широко, фильтруйте узко
4. **Сохраняйте результаты** - используйте `--output` для последующего анализа

## Типичный рабочий процесс

```bash
# 1. Соберите данные (автоматический поиск)
poetry run python collect_flights.py MOW BKK \
  --leg1-dates 2026-02-15 2026-02-20 \
  --leg2-dates 2026-02-25 2026-03-05

# 2. Проанализируйте с разными фильтрами
poetry run python aggregate_flights.py data/flights_MOW_BKK_*.json \
  --min-stay 3 --max-stay 7 --top 10

poetry run python aggregate_flights.py data/flights_MOW_BKK_*.json \
  --leg1-date 2026-02-19 --min-stay 5 --max-stay 7 --top 5

# 3. Сохраните лучшие варианты
poetry run python aggregate_flights.py data/flights_MOW_BKK_*.json \
  --min-stay 5 --max-stay 5 --top 20 \
  --output results/5days_stay.json
```

## Примеры результатов

После запуска вы увидите:

```
✓ Найдено комбинаций: 145
Статистика по ценам:
  Минимальная: 26,591 RUB
  Средняя:     45,230 RUB
  Медианная:   42,150 RUB

Статистика по промежуточным городам:
  DXB: 45 комбинаций, мин. цена: 28,450 RUB
  IST: 38 комбинаций, мин. цена: 26,591 RUB
  DOH: 32 комбинаций, мин. цена: 31,200 RUB
  ...
```

## API лимиты

- До **1000** уникальных маршрутов за один запрос
- Скрипт автоматически добавляет паузы между запросами (0.5 сек)
- По умолчанию используются только прямые рейсы (`direct=true`)
- Используйте `--allow-indirect` для поиска рейсов с пересадками

---

**Удачи в поиске дешёвых билетов! ✈️**
