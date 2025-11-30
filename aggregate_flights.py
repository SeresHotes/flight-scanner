#!/usr/bin/env python3
"""
Скрипт для агрегации и анализа собранных данных о перелетах.
Находит оптимальные комбинации перелетов с учетом стоимости и времени пребывания.
"""

import json
import argparse
import subprocess
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Set
from pathlib import Path
from collections import defaultdict


def load_data(file_path: str) -> Dict[str, Any]:
    """
    Загружает данные из JSON файла.

    Args:
        file_path: Путь к файлу с данными

    Returns:
        Словарь с данными о перелетах
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_airport_network(network_file: str = "data/airport_network.json") -> Dict[str, List[Dict]]:
    """
    Загружает сеть аэропортов. Если файл не существует, запускает скрипт для его создания.

    Args:
        network_file: Путь к файлу с сетью аэропортов

    Returns:
        Словарь с сетью аэропортов {IATA: [{"iata": ..., "distance_km": ...}, ...]}
    """
    network_path = Path(network_file)

    if not network_path.exists():
        print(f"\nФайл {network_file} не найден.")
        print("Запускаем построение сети аэропортов...")

        try:
            # Запускаем скрипт построения сети
            result = subprocess.run(
                ["python", "build_airport_network.py", "--output", network_file],
                capture_output=True,
                text=True,
                timeout=600  # 10 минут таймаут
            )

            if result.returncode != 0:
                print(f"Ошибка при построении сети аэропортов: {result.stderr}")
                print("Продолжаем без учета переходов между аэропортами...")
                return {}

            print("Сеть аэропортов успешно построена!")

        except Exception as e:
            print(f"Ошибка при запуске build_airport_network.py: {e}")
            print("Продолжаем без учета переходов между аэропортами...")
            return {}

    try:
        with open(network_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка при загрузке сети аэропортов: {e}")
        return {}


def get_nearby_airports(airport_iata: str, network: Dict[str, List[Dict]],
                       max_distance_km: float = 100) -> Set[str]:
    """
    Возвращает набор IATA кодов аэропортов, близких к указанному аэропорту.

    Args:
        airport_iata: IATA код аэропорта
        network: Сеть аэропортов
        max_distance_km: Максимальное расстояние для учета аэропорта

    Returns:
        Набор IATA кодов близких аэропортов (включая сам аэропорт)
    """
    nearby = {airport_iata}  # Включаем сам аэропорт

    if airport_iata in network:
        for neighbor in network[airport_iata]:
            if neighbor.get("distance_km", float('inf')) <= max_distance_km:
                nearby.add(neighbor["iata"])

    return nearby


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
            return datetime.strptime(date_clean, fmt)
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


def find_combinations(data: Dict[str, Any], min_stay: int = 1,
                     max_stay: int = 30,
                     leg1_depart_from: str = None, leg1_depart_to: str = None,
                     leg2_depart_from: str = None, leg2_depart_to: str = None,
                     via_city: str = None,
                     airport_network: Dict[str, List[Dict]] = None,
                     max_airport_distance: float = 100) -> List[Dict[str, Any]]:
    """
    Находит все возможные комбинации перелетов.

    Args:
        data: Данные о перелетах
        min_stay: Минимальная длительность пребывания в днях
        max_stay: Максимальная длительность пребывания в днях
        leg1_depart_from: Минимальная дата вылета первого рейса (YYYY-MM-DD)
        leg1_depart_to: Максимальная дата вылета первого рейса (YYYY-MM-DD)
        leg2_depart_from: Минимальная дата вылета второго рейса (YYYY-MM-DD)
        leg2_depart_to: Максимальная дата вылета второго рейса (YYYY-MM-DD)
        via_city: Фильтр по конкретному промежуточному городу (IATA код)
        airport_network: Сеть аэропортов для учета переходов между близкими аэропортами
        max_airport_distance: Максимальное расстояние между аэропортами (км)

    Returns:
        Список комбинаций перелетов
    """
    leg1_flights = data.get("leg1_flights", [])
    leg2_flights = data.get("leg2_flights", [])

    # Фильтруем первые рейсы по дате вылета, если указано
    if leg1_depart_from or leg1_depart_to:
        filtered_leg1 = []
        for flight in leg1_flights:
            if not flight.get("departure_at"):
                continue
            try:
                # Извлекаем дату вылета
                depart_date = flight["departure_at"].split("T")[0]

                # Проверяем диапазон
                if leg1_depart_from and depart_date < leg1_depart_from:
                    continue
                if leg1_depart_to and depart_date > leg1_depart_to:
                    continue

                filtered_leg1.append(flight)
            except Exception:
                continue

        leg1_flights = filtered_leg1

    # Фильтруем вторые рейсы по дате вылета, если указано
    if leg2_depart_from or leg2_depart_to:
        filtered_leg2 = []
        for flight in leg2_flights:
            if not flight.get("departure_at"):
                continue
            try:
                # Извлекаем дату вылета
                depart_date = flight["departure_at"].split("T")[0]

                # Проверяем диапазон
                if leg2_depart_from and depart_date < leg2_depart_from:
                    continue
                if leg2_depart_to and depart_date > leg2_depart_to:
                    continue

                filtered_leg2.append(flight)
            except Exception:
                continue

        leg2_flights = filtered_leg2

    combinations = []

    # Формируем информацию о фильтрах
    leg1_filter = ""
    if leg1_depart_from and leg1_depart_to:
        leg1_filter = f" (вылет: {leg1_depart_from} - {leg1_depart_to})"
    elif leg1_depart_from:
        leg1_filter = f" (вылет: от {leg1_depart_from})"
    elif leg1_depart_to:
        leg1_filter = f" (вылет: до {leg1_depart_to})"

    leg2_filter = ""
    if leg2_depart_from and leg2_depart_to:
        leg2_filter = f" (вылет: {leg2_depart_from} - {leg2_depart_to})"
    elif leg2_depart_from:
        leg2_filter = f" (вылет: от {leg2_depart_from})"
    elif leg2_depart_to:
        leg2_filter = f" (вылет: до {leg2_depart_to})"

    via_filter = f" через {via_city}" if via_city else ""
    airport_transfer_info = ""
    if airport_network:
        airport_transfer_info = f" (с учетом переходов между аэропортами до {max_airport_distance} км)"
    print(f"\nАнализ {len(leg1_flights)} рейсов первого этапа{leg1_filter} и {len(leg2_flights)} рейсов второго этапа{leg2_filter}{via_filter}{airport_transfer_info}...")

    for flight1 in leg1_flights:
        # Получаем город прибытия первого рейса
        intermediate_city = flight1.get("destination") or flight1.get("search_destination")

        # Фильтруем по конкретному промежуточному городу, если указано
        if via_city and intermediate_city != via_city:
            continue

        # Определяем набор допустимых аэропортов для второго рейса
        # (либо тот же аэропорт, либо близлежащие, если включена сеть)
        if airport_network and intermediate_city:
            allowed_airports = get_nearby_airports(intermediate_city, airport_network, max_airport_distance)
        else:
            allowed_airports = {intermediate_city} if intermediate_city else set()

        # Вычисляем дату/время прибытия первого рейса
        if flight1.get("arrival_at"):
            arrival_at = flight1["arrival_at"]
        elif flight1.get("departure_at") and flight1.get("duration"):
            # Вычисляем на основе departure_at + duration
            arrival_at = calculate_arrival(flight1["departure_at"], flight1["duration"])
        else:
            # Если нет данных, пропускаем этот рейс
            continue

        for flight2 in leg2_flights:
            # Проверяем, что второй рейс начинается из допустимого аэропорта
            leg2_origin = flight2.get("origin") or flight2.get("search_origin")

            if leg2_origin not in allowed_airports:
                continue

            # Определяем, является ли это переходом между аэропортами
            is_airport_transfer = leg2_origin != intermediate_city

            # Получаем дату/время отправления второго рейса
            if not flight2.get("departure_at"):
                continue
            departure_at = flight2["departure_at"]

            # Вычисляем длительность пребывания
            stay_days = calculate_stay_duration(arrival_at, departure_at)

            # Проверяем, что пребывание в допустимых пределах
            if stay_days < min_stay or stay_days > max_stay:
                continue

            # Вычисляем общую стоимость
            price1 = flight1.get("price") or flight1.get("value", 0)
            price2 = flight2.get("price") or flight2.get("value", 0)
            total_price = price1 + price2

            # Вычисляем время прибытия второго рейса
            if flight2.get("arrival_at"):
                leg2_arrival = flight2["arrival_at"]
            elif flight2.get("departure_at") and flight2.get("duration"):
                leg2_arrival = calculate_arrival(flight2["departure_at"], flight2["duration"])
            else:
                leg2_arrival = departure_at

            # Формируем комбинацию
            combination = {
                "total_price": total_price,
                "stay_days": stay_days,
                "intermediate_city": intermediate_city,
                "leg1": {
                    "origin": flight1.get("origin") or flight1.get("search_origin"),
                    "destination": intermediate_city,
                    "departure_at": flight1.get("departure_at"),
                    "arrival_at": arrival_at,
                    "price": price1,
                    "airline": flight1.get("airline"),
                    "flight_number": flight1.get("flight_number"),
                    "link": flight1.get("link"),
                    "duration": flight1.get("duration")
                },
                "leg2": {
                    "origin": leg2_origin,  # Используем реальный аэропорт вылета
                    "destination": flight2.get("destination") or flight2.get("search_destination"),
                    "departure_at": departure_at,
                    "arrival_at": leg2_arrival,
                    "price": price2,
                    "airline": flight2.get("airline"),
                    "flight_number": flight2.get("flight_number"),
                    "link": flight2.get("link"),
                    "duration": flight2.get("duration")
                }
            }

            # Добавляем информацию о переходе между аэропортами, если применимо
            if is_airport_transfer and airport_network:
                # Находим расстояние между аэропортами
                transfer_distance = None
                if intermediate_city in airport_network:
                    for neighbor in airport_network[intermediate_city]:
                        if neighbor["iata"] == leg2_origin:
                            transfer_distance = neighbor.get("distance_km")
                            break

                combination["airport_transfer"] = {
                    "from_airport": intermediate_city,
                    "to_airport": leg2_origin,
                    "distance_km": transfer_distance
                }

            combinations.append(combination)

    return combinations


def get_statistics(combinations: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Вычисляет статистику по найденным комбинациям.

    Args:
        combinations: Список комбинаций перелетов

    Returns:
        Словарь со статистикой
    """
    if not combinations:
        return {
            "total_combinations": 0,
            "min_price": 0,
            "max_price": 0,
            "avg_price": 0,
            "median_price": 0
        }

    prices = [c["total_price"] for c in combinations]
    prices.sort()

    # Статистика по промежуточным городам
    cities_stats = defaultdict(lambda: {"count": 0, "min_price": float('inf'), "avg_prices": []})

    for combo in combinations:
        city = combo["intermediate_city"]
        price = combo["total_price"]
        cities_stats[city]["count"] += 1
        cities_stats[city]["min_price"] = min(cities_stats[city]["min_price"], price)
        cities_stats[city]["avg_prices"].append(price)

    # Вычисляем средние цены
    for city in cities_stats:
        avg_price = sum(cities_stats[city]["avg_prices"]) / len(cities_stats[city]["avg_prices"])
        cities_stats[city]["avg_price"] = round(avg_price, 2)
        del cities_stats[city]["avg_prices"]

    return {
        "total_combinations": len(combinations),
        "min_price": min(prices),
        "max_price": max(prices),
        "avg_price": round(sum(prices) / len(prices), 2),
        "median_price": prices[len(prices) // 2],
        "by_intermediate_city": dict(cities_stats)
    }


def print_summary(combinations: List[Dict[str, Any]], stats: Dict[str, Any],
                 top_n: int = 10, unique_cities: bool = False):
    """
    Выводит сводку по найденным комбинациям.

    Args:
        combinations: Список комбинаций перелетов
        stats: Статистика
        top_n: Количество лучших вариантов для отображения
        unique_cities: Если True, показывает только уникальные промежуточные города (самые дешевые)
    """
    print(f"\n{'='*80}")
    print("РЕЗУЛЬТАТЫ АНАЛИЗА")
    print(f"{'='*80}\n")

    if not combinations:
        print("❌ Не найдено подходящих комбинаций перелетов")
        return

    print(f"✓ Найдено комбинаций: {stats['total_combinations']}")
    print(f"\nСтатистика по ценам:")
    print(f"  Минимальная: {stats['min_price']:,.0f}")
    print(f"  Средняя:     {stats['avg_price']:,.0f}")
    print(f"  Медианная:   {stats['median_price']:,.0f}")
    print(f"  Максимальная: {stats['max_price']:,.0f}")

    if stats.get("by_intermediate_city"):
        print(f"\nСтатистика по промежуточным городам:")
        for city, city_stats in sorted(stats["by_intermediate_city"].items(),
                                      key=lambda x: x[1]["min_price"]):
            print(f"  {city}:")
            print(f"    - Комбинаций: {city_stats['count']}")
            print(f"    - Мин. цена: {city_stats['min_price']:,.0f}")
            print(f"    - Сред. цена: {city_stats['avg_price']:,.0f}")

    # Сортируем по цене
    sorted_combinations = sorted(combinations, key=lambda x: x["total_price"])

    # Фильтруем по уникальным городам, если требуется
    if unique_cities:
        seen_cities = set()
        filtered_combinations = []
        for combo in sorted_combinations:
            city = combo["intermediate_city"]
            if city not in seen_cities:
                seen_cities.add(city)
                filtered_combinations.append(combo)
        sorted_combinations = filtered_combinations

    print(f"\n{'='*80}")
    title = f"ТОП-{min(top_n, len(sorted_combinations))} САМЫХ ДЕШЕВЫХ ВАРИАНТОВ"
    if unique_cities:
        title += " (уникальные города)"
    print(title)
    print(f"{'='*80}\n")

    for i, combo in enumerate(sorted_combinations[:top_n], 1):
        print(f"#{i}. Общая стоимость: {combo['total_price']:,.0f} RUB | "
              f"Пребывание: {combo['stay_days']} дней")
        print(f"    Промежуточный город: {combo['intermediate_city']}")

        leg1 = combo["leg1"]
        duration1_str = f"{leg1.get('duration', 0) // 60}ч {leg1.get('duration', 0) % 60}м" if leg1.get('duration') else ""
        print(f"\n    ✈️  Этап 1: {leg1['origin']} → {leg1['destination']}")
        print(f"        Вылет:       {leg1['departure_at']}")
        print(f"        Прибытие:    {leg1['arrival_at']}")
        if duration1_str:
            print(f"        Длительность: {duration1_str}")
        print(f"        Цена:        {leg1['price']:,.0f} RUB")
        if leg1.get('airline'):
            print(f"        Авиакомпания: {leg1['airline']}")
        if leg1.get('flight_number'):
            print(f"        Рейс: {leg1['airline']} {leg1['flight_number']}")

        # Показываем информацию о переходе между аэропортами, если есть
        if combo.get("airport_transfer"):
            transfer = combo["airport_transfer"]
            print(f"\n    🚌 Переход между аэропортами:")
            print(f"        Из: {transfer['from_airport']}")
            print(f"        В:  {transfer['to_airport']}")
            if transfer.get('distance_km'):
                print(f"        Расстояние: {transfer['distance_km']:.2f} км")

        leg2 = combo["leg2"]
        duration2_str = f"{leg2.get('duration', 0) // 60}ч {leg2.get('duration', 0) % 60}м" if leg2.get('duration') else ""
        print(f"\n    ✈️  Этап 2: {leg2['origin']} → {leg2['destination']}")
        print(f"        Вылет:       {leg2['departure_at']}")
        print(f"        Прибытие:    {leg2['arrival_at']}")
        if duration2_str:
            print(f"        Длительность: {duration2_str}")
        print(f"        Цена:        {leg2['price']:,.0f} RUB")
        if leg2.get('airline'):
            print(f"        Авиакомпания: {leg2['airline']}")
        if leg2.get('flight_number'):
            print(f"        Рейс: {leg2['airline']} {leg2['flight_number']}")

        print()


def save_results(combinations: List[Dict[str, Any]], stats: Dict[str, Any],
                output_file: str, unique_cities: bool = False):
    """
    Сохраняет результаты анализа в JSON файл.

    Args:
        combinations: Список комбинаций
        stats: Статистика
        output_file: Путь к выходному файлу
        unique_cities: Если True, сохраняет только уникальные промежуточные города (самые дешевые)
    """
    # Сортируем по цене
    sorted_combinations = sorted(combinations, key=lambda x: x["total_price"])

    # Фильтруем по уникальным городам, если требуется
    if unique_cities:
        seen_cities = set()
        filtered_combinations = []
        for combo in sorted_combinations:
            city = combo["intermediate_city"]
            if city not in seen_cities:
                seen_cities.add(city)
                filtered_combinations.append(combo)
        sorted_combinations = filtered_combinations

    result = {
        "generated_at": datetime.now().isoformat(),
        "statistics": stats,
        "combinations": sorted_combinations
    }

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Результаты сохранены в {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Агрегация и анализ данных о перелетах",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Базовый анализ
  python aggregate_flights.py data/flights_MOW_BKK_20260215_120000.json

  # С ограничениями на длительность пребывания
  python aggregate_flights.py data/flights_MOW_BKK_20260215_120000.json \\
    --min-stay 3 --max-stay 7

  # Вывод топ-20 вариантов
  python aggregate_flights.py data/flights_MOW_BKK_20260215_120000.json \\
    --top 20

  # Показать только уникальные города (по одному самому дешевому варианту на город)
  python aggregate_flights.py data/flights_MOW_BKK_20260215_120000.json \\
    --unique-cities

  # Фильтрация по конкретному промежуточному городу
  python aggregate_flights.py data/flights_MOW_BKK_20260215_120000.json \\
    --via IST

  # Фильтрация по дате вылета первого этапа
  python aggregate_flights.py data/flights_MOW_BKK_20260215_120000.json \\
    --leg1-date 2026-02-19

  # Фильтрация по дате вылета второго этапа
  python aggregate_flights.py data/flights_MOW_BKK_20260215_120000.json \\
    --leg2-date 2026-02-25

  # Диапазон дат для обоих этапов
  python aggregate_flights.py data/flights_MOW_BKK_20260215_120000.json \\
    --leg1-from 2026-02-18 --leg1-to 2026-02-20 \\
    --leg2-from 2026-02-25 --leg2-to 2026-02-28

  # Сохранение результатов в файл
  python aggregate_flights.py data/flights_MOW_BKK_20260215_120000.json \\
    --output results/best_combinations.json

  # Поиск с учетом переходов между аэропортами (например, прилет в SVO, вылет из DME)
  python aggregate_flights.py data/flights_MOW_BKK_20260215_120000.json \\
    --enable-airport-transfers --airport-distance 100
        """
    )

    parser.add_argument("input_file", help="Путь к файлу с собранными данными о перелетах")

    parser.add_argument("--min-stay", type=int, default=1,
                       help="Минимальная длительность пребывания в днях (по умолчанию 1)")

    parser.add_argument("--max-stay", type=int, default=30,
                       help="Максимальная длительность пребывания в днях (по умолчанию 30)")

    # Фильтры для первого этапа
    parser.add_argument("--leg1-date", type=str, default=None,
                       help="Конкретная дата вылета первого рейса (YYYY-MM-DD)")

    parser.add_argument("--leg1-from", type=str, default=None,
                       help="Минимальная дата вылета первого рейса (YYYY-MM-DD)")

    parser.add_argument("--leg1-to", type=str, default=None,
                       help="Максимальная дата вылета первого рейса (YYYY-MM-DD)")

    # Фильтры для второго этапа
    parser.add_argument("--leg2-date", type=str, default=None,
                       help="Конкретная дата вылета второго рейса (YYYY-MM-DD)")

    parser.add_argument("--leg2-from", type=str, default=None,
                       help="Минимальная дата вылета второго рейса (YYYY-MM-DD)")

    parser.add_argument("--leg2-to", type=str, default=None,
                       help="Максимальная дата вылета второго рейса (YYYY-MM-DD)")

    # Устаревшие параметры для обратной совместимости
    parser.add_argument("--depart-date", type=str, default=None,
                       help="(устарело, используйте --leg1-date) Дата вылета первого рейса")

    parser.add_argument("--depart-from", type=str, default=None,
                       help="(устарело, используйте --leg1-from) Мин. дата вылета первого рейса")

    parser.add_argument("--depart-to", type=str, default=None,
                       help="(устарело, используйте --leg1-to) Макс. дата вылета первого рейса")

    parser.add_argument("--top", type=int, default=10,
                       help="Количество лучших вариантов для отображения (по умолчанию 10)")

    parser.add_argument("--unique-cities", action="store_true",
                       help="Показывать только уникальные промежуточные города (самый дешевый вариант для каждого города)")

    parser.add_argument("--via", type=str, default=None,
                       help="Фильтр по конкретному промежуточному городу (IATA код, например IST или DXB)")

    parser.add_argument("--enable-airport-transfers", action="store_true",
                       help="Включить поиск с учетом переходов между близкими аэропортами (например, SVO-DME)")

    parser.add_argument("--airport-distance", type=float, default=100,
                       help="Максимальное расстояние между аэропортами для переходов в км (по умолчанию 100)")

    parser.add_argument("--airport-network", type=str, default="data/airport_network.json",
                       help="Путь к файлу с сетью аэропортов (по умолчанию data/airport_network.json)")

    parser.add_argument("--output", default=None,
                       help="Путь к файлу для сохранения результатов (опционально)")

    args = parser.parse_args()

    # Проверяем существование входного файла
    if not Path(args.input_file).exists():
        print(f"❌ Ошибка: файл {args.input_file} не найден")
        return

    # Обрабатываем параметры дат вылета для первого этапа
    leg1_from = args.leg1_from or args.depart_from
    leg1_to = args.leg1_to or args.depart_to

    # Если указана конкретная дата для leg1, используем её как диапазон из одного дня
    if args.leg1_date or args.depart_date:
        leg1_date = args.leg1_date or args.depart_date
        leg1_from = leg1_date
        leg1_to = leg1_date

    # Обрабатываем параметры дат вылета для второго этапа
    leg2_from = args.leg2_from
    leg2_to = args.leg2_to

    # Если указана конкретная дата для leg2, используем её как диапазон из одного дня
    if args.leg2_date:
        leg2_from = args.leg2_date
        leg2_to = args.leg2_date

    print(f"\n{'#'*80}")
    print("АНАЛИЗ ДАННЫХ О ПЕРЕЛЕТАХ")
    print(f"{'#'*80}")
    print(f"Входной файл: {args.input_file}")
    print(f"Диапазон пребывания: {args.min_stay}-{args.max_stay} дней")
    if args.via:
        print(f"Фильтр по промежуточному городу: {args.via}")

    # Выводим информацию о фильтрах дат
    if leg1_from and leg1_to and leg1_from == leg1_to:
        print(f"Дата вылета (этап 1): {leg1_from}")
    elif leg1_from or leg1_to:
        date_range = f"{leg1_from or 'любая'} - {leg1_to or 'любая'}"
        print(f"Диапазон дат вылета (этап 1): {date_range}")

    if leg2_from and leg2_to and leg2_from == leg2_to:
        print(f"Дата вылета (этап 2): {leg2_from}")
    elif leg2_from or leg2_to:
        date_range = f"{leg2_from or 'любая'} - {leg2_to or 'любая'}"
        print(f"Диапазон дат вылета (этап 2): {date_range}")

    # Загружаем данные
    data = load_data(args.input_file)

    # Выводим метаданные
    metadata = data.get("metadata", {})
    if metadata:
        print(f"\nМаршрут: {metadata.get('origin')} → "
              f"[{', '.join(metadata.get('intermediate_airports', []))}] → "
              f"{metadata.get('destination')}")
        print(f"Собрано рейсов: {metadata.get('total_flights')}")

    # Загружаем сеть аэропортов, если включены переходы между аэропортами
    airport_network = None
    if args.enable_airport_transfers:
        print(f"\n{'#'*80}")
        print("ЗАГРУЗКА СЕТИ АЭРОПОРТОВ")
        print(f"{'#'*80}")
        airport_network = load_airport_network(args.airport_network)
        if airport_network:
            print(f"✓ Сеть аэропортов загружена: {len(airport_network)} аэропортов")
        else:
            print("⚠ Сеть аэропортов не загружена, продолжаем без переходов между аэропортами")

    # Находим комбинации
    combinations = find_combinations(data, args.min_stay, args.max_stay,
                                    leg1_from, leg1_to, leg2_from, leg2_to, args.via,
                                    airport_network, args.airport_distance)

    # Вычисляем статистику
    stats = get_statistics(combinations)

    # Выводим результаты
    print_summary(combinations, stats, args.top, args.unique_cities)

    # Сохраняем результаты, если указан выходной файл
    if args.output:
        save_results(combinations, stats, args.output, args.unique_cities)


if __name__ == "__main__":
    main()
