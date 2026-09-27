"""Даты и время планировщика: разбор ISO (таймзона срезается — все времена
«наивные» локальные), прилёт по длительности, дни пребывания, список дат окна."""
from datetime import datetime, timedelta
from typing import List


def parse_datetime(date_str: str) -> datetime:
    """
    Парсит дату и время из ISO формата.

    Args:
        date_str: Строка с датой в ISO формате

    Returns:
        Объект datetime
    """
    # Обрабатываем разные форматы дат
    for fmt in ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
        try:
            # Убираем timezone для простоты
            date_clean = date_str.split('+')[0].split('-', 3)
            if len(date_clean) == 4:
                date_clean = '-'.join(date_clean[:3])
            else:
                date_clean = date_str.split('+')[0]
            dt = datetime.strptime(date_clean, fmt)
            # Всегда возвращаем naive: строковая срезка выше не убирает суффикс
            # 'Z' (UTC), и тогда %z даёт tz-aware datetime — при сравнении с naive
            # (напр. якорь даты цепочки) Python бросает "can't compare offset-naive
            # and offset-aware datetimes". Отбрасываем tz, чтобы все даты были naive.
            return dt.replace(tzinfo=None)
        except (ValueError, IndexError):
            continue
    raise ValueError(f"Не удалось распарсить дату: {date_str}")


def calculate_arrival(departure_at: str, duration: int) -> str:
    """
    Вычисляет время прибытия на основе времени вылета и длительности.

    Args:
        departure_at: Дата/время вылета в ISO формате
        duration: Длительность полета в минутах

    Returns:
        Дата/время прибытия в ISO формате
    """
    try:
        departure = parse_datetime(departure_at)
        arrival = departure + timedelta(minutes=duration)
        return arrival.isoformat()
    except Exception as e:
        print(f"Ошибка вычисления прибытия: {e}")
        return departure_at


def calculate_stay_duration(leg1_arrival: str, leg2_departure: str) -> int:
    """
    Вычисляет длительность пребывания в днях между двумя рейсами.

    Args:
        leg1_arrival: Дата/время прибытия первого рейса
        leg2_departure: Дата/время отправления второго рейса

    Returns:
        Количество дней пребывания
    """
    try:
        arrival = parse_datetime(leg1_arrival)
        departure = parse_datetime(leg2_departure)
        delta = departure - arrival
        return delta.days
    except Exception:
        # Если не можем распарсить время, считаем по датам
        try:
            arrival_date = leg1_arrival.split('T')[0] if 'T' in leg1_arrival else leg1_arrival
            departure_date = leg2_departure.split('T')[0] if 'T' in leg2_departure else leg2_departure

            arrival = datetime.strptime(arrival_date, "%Y-%m-%d")
            departure = datetime.strptime(departure_date, "%Y-%m-%d")
            return (departure - arrival).days
        except Exception:
            return 0


def get_date_range(start_date: str, end_date: str) -> List[str]:
    """
    Генерирует список дат между start_date и end_date включительно.

    Args:
        start_date: Дата в формате YYYY-MM-DD
        end_date: Дата в формате YYYY-MM-DD

    Returns:
        Список дат в формате YYYY-MM-DD
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates
