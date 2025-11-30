#!/usr/bin/env python3
"""
Скрипт для сбора данных о перелетах через промежуточные города.
Собирает данные о прямых перелетах из города отправления в промежуточные города,
а затем из промежуточных городов в конечный пункт назначения.
"""

import os
import sys
import json
import requests
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any
from pathlib import Path
from dotenv import load_dotenv
import time

# Загрузка переменных окружения
load_dotenv()

API_BASE_URL = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"
API_TOKEN = os.getenv("TRAVELPAYOUTS_TOKEN")

if not API_TOKEN:
    print("Ошибка: TRAVELPAYOUTS_TOKEN не найден в .env файле")
    sys.exit(1)


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


def fetch_flights(origin: str = None, destination: str = None, departure_at: str = None,
                  currency: str = "RUB", unique: bool = True, limit: int = 1000) -> Dict[str, Any]:
    """
    Получает данные о перелетах из API.

    Args:
        origin: Код города отправления (IATA), опционально
        destination: Код города назначения (IATA), опционально
        departure_at: Дата вылета в формате YYYY-MM-DD
        currency: Валюта цен (по умолчанию RUB)
        unique: Уникальные направления (по умолчанию True)
        limit: Лимит результатов (по умолчанию 1000)

    Returns:
        Словарь с данными о перелетах
    """
    params = {
        "currency": currency,
        "token": API_TOKEN,
        "direct": "true",
        "one_way": "true",
        "limit": limit
    }

    # Добавляем unique только если True
    if unique:
        params["unique"] = "true"

    # Добавляем origin и destination только если они указаны
    if origin:
        params["origin"] = origin
    if destination:
        params["destination"] = destination
    if departure_at:
        params["departure_at"] = departure_at

    try:
        response = requests.get(API_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        origin_str = origin or "ANY"
        dest_str = destination or "ANY"
        print(f"Ошибка при запросе {origin_str} -> {dest_str} на {departure_at}: {e}")
        return {"data": []}


def collect_leg_data(origin: str = None, destination: str = None,
                     date_range: List[str] = None, leg_name: str = "") -> List[Dict[str, Any]]:
    """
    Собирает данные о перелетах для одного этапа маршрута.
    Если origin указан, а destination нет - получает все направления из origin.
    Если destination указан, а origin нет - получает все направления в destination.

    Args:
        origin: Код города отправления (IATA), опционально
        destination: Код города назначения (IATA), опционально
        date_range: Список дат для проверки
        leg_name: Название этапа (для логирования)

    Returns:
        Список всех найденных перелетов
    """
    all_flights = []
    total_requests = len(date_range) if date_range else 1
    current_request = 0

    origin_str = origin or "ANY"
    dest_str = destination or "ANY"

    print(f"\n{'='*60}")
    print(f"Сбор данных для этапа: {leg_name}")
    print(f"Маршрут: {origin_str} -> {dest_str}")
    if date_range:
        print(f"Диапазон дат: {date_range[0]} - {date_range[-1]}")
    print(f"Всего запросов: {total_requests}")
    print(f"{'='*60}\n")

    if date_range:
        for date in date_range:
            current_request += 1
            print(f"[{current_request}/{total_requests}] Запрос: {origin_str} -> {dest_str} на {date}...", end=" ")

            result = fetch_flights(origin, destination, date)

            if result.get("data"):
                flight_count = len(result["data"])
                print(f"✓ Найдено {flight_count} рейс(ов)")

                # Добавляем метаданные к каждому рейсу
                for flight in result["data"]:
                    flight["leg"] = leg_name
                    flight["search_origin"] = origin
                    flight["search_destination"] = destination
                    flight["search_date"] = date
                    all_flights.append(flight)
            else:
                print("✗ Рейсов не найдено")

            # Небольшая задержка для избежания rate limiting
            time.sleep(0.5)
    else:
        # Запрос без указания конкретной даты
        print(f"Запрос: {origin_str} -> {dest_str}...", end=" ")
        result = fetch_flights(origin, destination)

        if result.get("data"):
            flight_count = len(result["data"])
            print(f"✓ Найдено {flight_count} рейс(ов)")

            for flight in result["data"]:
                flight["leg"] = leg_name
                flight["search_origin"] = origin
                flight["search_destination"] = destination
                all_flights.append(flight)
        else:
            print("✗ Рейсов не найдено")

    print(f"\nИтого найдено {len(all_flights)} рейс(ов) для этапа {leg_name}\n")
    return all_flights


def save_data(data: Dict[str, Any], output_file: str):
    """
    Сохраняет данные в JSON файл.

    Args:
        data: Данные для сохранения
        output_file: Путь к выходному файлу
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Данные сохранены в {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Сбор данных о перелетах через промежуточные города",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # НОВЫЙ СПОСОБ: Автоматический поиск всех направлений (рекомендуется)
  python collect_flights.py MOW BKK \\
    --leg1-dates 2026-02-15 2026-02-20 \\
    --leg2-dates 2026-02-25 2026-03-05

  # Старый способ: Перелет через конкретные промежуточные города
  python collect_flights.py MOW BKK \\
    --leg1-dates 2026-02-15 2026-02-20 \\
    --leg2-dates 2026-02-25 2026-03-05 \\
    --intermediate IST DXB

  # Только сбор вылетов из города (все направления)
  python collect_flights.py MOW --leg1-dates 2026-02-15 2026-02-20
        """
    )

    parser.add_argument("origin", help="Код города отправления (IATA), например MOW для Москвы")
    parser.add_argument("destination", nargs="?", default=None,
                       help="Код конечного города (IATA), например BKK для Бангкока (опционально)")

    parser.add_argument("--leg1-dates", nargs=2, required=True,
                       metavar=("START", "END"),
                       help="Диапазон дат для первого этапа (из origin)")

    parser.add_argument("--leg2-dates", nargs=2, required=False,
                       metavar=("START", "END"),
                       help="Диапазон дат для второго этапа (в destination)")

    parser.add_argument("--intermediate", nargs="+", required=False,
                       help="Список промежуточных городов (IATA коды). Если не указано, API вернёт все доступные направления")

    parser.add_argument("--currency", default="RUB",
                       help="Валюта для цен (по умолчанию RUB)")

    parser.add_argument("--output", default=None,
                       help="Путь к выходному файлу (по умолчанию data/flights_TIMESTAMP.json)")

    args = parser.parse_args()

    # Генерируем диапазоны дат
    leg1_dates = get_date_range(args.leg1_dates[0], args.leg1_dates[1])
    leg2_dates = get_date_range(args.leg2_dates[0], args.leg2_dates[1]) if args.leg2_dates else None

    print(f"\n{'#'*60}")
    print("СБОР ДАННЫХ О ПЕРЕЛЕТАХ")
    print(f"{'#'*60}")

    if args.intermediate:
        # Старый способ с указанными промежуточными городами
        print(f"Маршрут: {args.origin} -> [{', '.join(args.intermediate)}] -> {args.destination}")
    elif args.destination:
        # Новый способ - автоматический поиск всех направлений
        print(f"Маршрут: {args.origin} -> [ВСЕ НАПРАВЛЕНИЯ] -> {args.destination}")
    else:
        # Только первый этап - все направления из origin
        print(f"Направления из: {args.origin}")

    print(f"Первый этап: {len(leg1_dates)} дней ({leg1_dates[0]} - {leg1_dates[-1]})")
    if leg2_dates:
        print(f"Второй этап: {len(leg2_dates)} дней ({leg2_dates[0]} - {leg2_dates[-1]})")
    print(f"Валюта: {args.currency}")

    # Сбор данных для первого этапа
    if args.intermediate:
        # Старый способ: перебираем указанные промежуточные города
        leg1_flights = []
        for intermediate in args.intermediate:
            flights = collect_leg_data(
                origin=args.origin,
                destination=intermediate,
                date_range=leg1_dates,
                leg_name="leg1"
            )
            leg1_flights.extend(flights)
    else:
        # Новый способ: получаем все направления из origin
        leg1_flights = collect_leg_data(
            origin=args.origin,
            destination=None,  # Не указываем destination - получим все направления
            date_range=leg1_dates,
            leg_name="leg1"
        )

    # Сбор данных для второго этапа (если нужен)
    leg2_flights = []
    if args.destination and leg2_dates:
        if args.intermediate:
            # Старый способ: из указанных промежуточных городов в destination
            for intermediate in args.intermediate:
                flights = collect_leg_data(
                    origin=intermediate,
                    destination=args.destination,
                    date_range=leg2_dates,
                    leg_name="leg2"
                )
                leg2_flights.extend(flights)
        else:
            # Новый способ: все направления в destination
            leg2_flights = collect_leg_data(
                origin=None,  # Не указываем origin - получим все направления
                destination=args.destination,
                date_range=leg2_dates,
                leg_name="leg2"
            )

    # Извлекаем уникальные промежуточные аэропорты из собранных данных
    discovered_airports = set()
    for flight in leg1_flights:
        dest = flight.get("destination")
        if dest:
            discovered_airports.add(dest)

    # Формируем итоговые данные
    result = {
        "metadata": {
            "origin": args.origin,
            "destination": args.destination,
            "intermediate_airports": args.intermediate or sorted(list(discovered_airports)),
            "leg1_date_range": {
                "start": leg1_dates[0],
                "end": leg1_dates[-1]
            },
            "currency": args.currency,
            "collected_at": datetime.now().isoformat(),
            "total_flights": len(leg1_flights) + len(leg2_flights)
        },
        "leg1_flights": leg1_flights,
        "leg2_flights": leg2_flights
    }

    if leg2_dates:
        result["metadata"]["leg2_date_range"] = {
            "start": leg2_dates[0],
            "end": leg2_dates[-1]
        }

    # Определяем имя выходного файла
    if args.output:
        output_file = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_str = args.destination if args.destination else "ALL"
        output_file = f"data/flights_{args.origin}_{dest_str}_{timestamp}.json"

    # Сохраняем данные
    save_data(result, output_file)

    print(f"\n{'='*60}")
    print("СБОР ДАННЫХ ЗАВЕРШЕН")
    print(f"{'='*60}")
    print(f"Первый этап: {len(leg1_flights)} рейсов")
    if leg2_flights:
        print(f"Второй этап: {len(leg2_flights)} рейсов")
    print(f"Всего: {len(leg1_flights) + len(leg2_flights)} рейсов")
    if discovered_airports:
        print(f"Найдено направлений: {len(discovered_airports)}")
    print(f"Файл: {output_file}")


if __name__ == "__main__":
    main()
