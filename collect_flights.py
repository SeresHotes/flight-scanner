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


def fetch_flights(origin: str, destination: str, depart_date: str,
                  currency: str = "RUB") -> Dict[str, Any]:
    """
    Получает данные о перелетах из API.

    Args:
        origin: Код города отправления (IATA)
        destination: Код города назначения (IATA)
        depart_date: Дата вылета в формате YYYY-MM-DD
        currency: Валюта цен (по умолчанию RUB)

    Returns:
        Словарь с данными о перелетах
    """
    params = {
        "origin": origin,
        "destination": destination,
        "departure_at": depart_date,
        "currency": currency,
        "token": API_TOKEN,
        "direct": "true",
        "one_way": "true"
    }

    try:
        response = requests.get(API_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе {origin} -> {destination} на {depart_date}: {e}")
        return {"data": []}


def collect_leg_data(origins: List[str], destinations: List[str],
                     date_range: List[str], leg_name: str) -> List[Dict[str, Any]]:
    """
    Собирает данные о перелетах для одного этапа маршрута.

    Args:
        origins: Список кодов городов отправления (или один город в списке)
        destinations: Список кодов городов назначения (или один город в списке)
        date_range: Список дат для проверки
        leg_name: Название этапа (для логирования)

    Returns:
        Список всех найденных перелетов
    """
    all_flights = []
    total_requests = len(origins) * len(destinations) * len(date_range)
    current_request = 0

    print(f"\n{'='*60}")
    print(f"Сбор данных для этапа: {leg_name}")
    print(f"Маршрут: {', '.join(origins)} -> {', '.join(destinations)}")
    print(f"Диапазон дат: {date_range[0]} - {date_range[-1]}")
    print(f"Всего запросов: {total_requests}")
    print(f"{'='*60}\n")

    for origin in origins:
        for destination in destinations:
            for date in date_range:
                current_request += 1
                print(f"[{current_request}/{total_requests}] Запрос: {origin} -> {destination} на {date}...", end=" ")

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

  # Перелет Москва -> Стамбул/Дубай -> Бангкок
  python collect_flights.py MOW BKK \\
    --leg1-dates 2026-02-15 2026-02-20 \\
    --leg2-dates 2026-02-25 2026-03-05 \\
    --intermediate IST DXB

  # Перелет с одним промежуточным городом
  python collect_flights.py LED BCN \\
    --leg1-dates 2026-03-01 2026-03-03 \\
    --leg2-dates 2026-03-10 2026-03-15 \\
    --intermediate IST
        """
    )

    parser.add_argument("origin", help="Код города отправления (IATA), например MOW для Москвы")
    parser.add_argument("destination", help="Код конечного города (IATA), например BKK для Бангкока")

    parser.add_argument("--leg1-dates", nargs=2, required=True,
                       metavar=("START", "END"),
                       help="Диапазон дат для первого этапа (из origin в промежуточные города)")

    parser.add_argument("--leg2-dates", nargs=2, required=True,
                       metavar=("START", "END"),
                       help="Диапазон дат для второго этапа (из промежуточных городов в destination)")

    parser.add_argument("--intermediate", nargs="+", required=True,
                       help="Список промежуточных городов (IATA коды)")

    parser.add_argument("--currency", default="RUB",
                       help="Валюта для цен (по умолчанию RUB)")

    parser.add_argument("--output", default=None,
                       help="Путь к выходному файлу (по умолчанию data/flights_TIMESTAMP.json)")

    args = parser.parse_args()

    # Генерируем диапазоны дат
    leg1_dates = get_date_range(args.leg1_dates[0], args.leg1_dates[1])
    leg2_dates = get_date_range(args.leg2_dates[0], args.leg2_dates[1])

    print(f"\n{'#'*60}")
    print("СБОР ДАННЫХ О ПЕРЕЛЕТАХ")
    print(f"{'#'*60}")
    print(f"Маршрут: {args.origin} -> [{', '.join(args.intermediate)}] -> {args.destination}")
    print(f"Первый этап: {len(leg1_dates)} дней ({leg1_dates[0]} - {leg1_dates[-1]})")
    print(f"Второй этап: {len(leg2_dates)} дней ({leg2_dates[0]} - {leg2_dates[-1]})")
    print(f"Валюта: {args.currency}")

    # Сбор данных для первого этапа (origin -> intermediate)
    leg1_flights = collect_leg_data(
        [args.origin],
        args.intermediate,
        leg1_dates,
        "leg1"
    )

    # Сбор данных для второго этапа (intermediate -> destination)
    leg2_flights = collect_leg_data(
        args.intermediate,
        [args.destination],
        leg2_dates,
        "leg2"
    )

    # Формируем итоговые данные
    result = {
        "metadata": {
            "origin": args.origin,
            "destination": args.destination,
            "intermediate_airports": args.intermediate,
            "leg1_date_range": {
                "start": leg1_dates[0],
                "end": leg1_dates[-1]
            },
            "leg2_date_range": {
                "start": leg2_dates[0],
                "end": leg2_dates[-1]
            },
            "currency": args.currency,
            "collected_at": datetime.now().isoformat(),
            "total_flights": len(leg1_flights) + len(leg2_flights)
        },
        "leg1_flights": leg1_flights,
        "leg2_flights": leg2_flights
    }

    # Определяем имя выходного файла
    if args.output:
        output_file = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"data/flights_{args.origin}_{args.destination}_{timestamp}.json"

    # Сохраняем данные
    save_data(result, output_file)

    print(f"\n{'='*60}")
    print("СБОР ДАННЫХ ЗАВЕРШЕН")
    print(f"{'='*60}")
    print(f"Первый этап: {len(leg1_flights)} рейсов")
    print(f"Второй этап: {len(leg2_flights)} рейсов")
    print(f"Всего: {len(leg1_flights) + len(leg2_flights)} рейсов")
    print(f"Файл: {output_file}")


if __name__ == "__main__":
    main()
