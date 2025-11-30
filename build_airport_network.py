#!/usr/bin/env python3
"""
Скрипт для построения сети аэропортов с вычислением расстояний между ними.
Загружает данные об аэропортах из HuggingFace датасета и определяет ближайшие аэропорты.
"""

import json
import math
from pathlib import Path
from typing import Dict, List, Tuple
from datasets import load_dataset


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Вычисляет расстояние между двумя точками на Земле по формуле гаверсинуса.

    Args:
        lat1, lon1: Координаты первой точки (широта, долгота)
        lat2, lon2: Координаты второй точки (широта, долгота)

    Returns:
        Расстояние в километрах
    """
    # Радиус Земли в километрах
    R = 6371.0

    # Конвертируем градусы в радианы
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Разница координат
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Формула гаверсинуса
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(a))

    distance = R * c
    return distance


def parse_coordinates(coord_str: str) -> Tuple[float, float]:
    """
    Парсит строку с координатами в формате "latitude, longitude".

    Args:
        coord_str: Строка с координатами

    Returns:
        Кортеж (latitude, longitude)
    """
    try:
        parts = coord_str.split(',')
        lat = float(parts[0].strip())
        lon = float(parts[1].strip())
        return lat, lon
    except (ValueError, IndexError, AttributeError):
        return None, None


def fetch_airports_from_huggingface() -> List[Dict]:
    """
    Загружает данные об аэропортах из HuggingFace датасета.

    Returns:
        Список словарей с данными об аэропортах
    """
    print("Загружаем данные об аэропортах из HuggingFace...")
    print("Это может занять некоторое время при первом запуске...")

    try:
        # Загружаем датасет
        dataset = load_dataset("ronnieaban/world-airports", split="train")

        print(f"Датасет загружен, всего записей: {len(dataset)}")

        # Фильтруем только аэропорты с IATA кодом и координатами
        airports = []
        for row in dataset:
            if row.get("iata_code") and row.get("coordinates"):
                airports.append(dict(row))

        print(f"Найдено {len(airports)} аэропортов с IATA кодами и координатами")
        return airports

    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        raise


def build_airport_network(airports: List[Dict], max_distance_km: float = 100) -> Dict[str, Dict]:
    """
    Строит сеть аэропортов, определяя для каждого аэропорта близлежащие аэропорты.

    Args:
        airports: Список аэропортов с координатами
        max_distance_km: Максимальное расстояние для считывания аэропортов близлежащими (в км)

    Returns:
        Словарь {IATA_код: {"name": название, "municipality": город, "country": страна,
                           "coordinates": "lat, lon", "nearby_airports": [список IATA кодов]}}
    """
    print(f"\nСтроим сеть аэропортов (макс. расстояние: {max_distance_km} км)...")

    # Создаем словарь аэропортов с координатами и информацией
    airport_coords = {}
    airport_info = {}

    for airport in airports:
        iata = airport.get("iata_code")
        if not iata:
            continue

        coords_str = airport.get("coordinates", "")
        lat, lon = parse_coordinates(coords_str)
        if lat is not None and lon is not None:
            airport_coords[iata] = (lat, lon)
            airport_info[iata] = {
                "name": airport.get("name", ""),
                "municipality": airport.get("municipality", ""),
                "country": airport.get("iso_country", ""),
                "coordinates": coords_str
            }

    print(f"Обрабатываем {len(airport_coords)} аэропортов с корректными координатами...")

    # Находим близлежащие аэропорты для каждого
    network = {}
    processed = 0

    for iata1, (lat1, lon1) in airport_coords.items():
        nearby_with_distances = []

        for iata2, (lat2, lon2) in airport_coords.items():
            if iata1 == iata2:
                continue

            distance = haversine_distance(lat1, lon1, lat2, lon2)

            if distance <= max_distance_km:
                nearby_with_distances.append({
                    "iata": iata2,
                    "distance_km": round(distance, 2)
                })

        # Сортируем по расстоянию
        nearby_with_distances.sort(key=lambda x: x["distance_km"])

        # Создаем запись для этого аэропорта
        network[iata1] = {
            "name": airport_info[iata1]["name"],
            "municipality": airport_info[iata1]["municipality"],
            "country": airport_info[iata1]["country"],
            "coordinates": airport_info[iata1]["coordinates"],
            "nearby_airports": nearby_with_distances
        }

        processed += 1
        if processed % 100 == 0:
            print(f"Обработано {processed}/{len(airport_coords)} аэропортов...")

    print(f"Обработано {len(network)} аэропортов")
    return network


def save_network(network: Dict, output_file: str = "data/airport_network.json"):
    """
    Сохраняет сеть аэропортов в JSON файл.

    Args:
        network: Сеть аэропортов
        output_file: Путь к выходному файлу
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(network, f, ensure_ascii=False, indent=2)

    print(f"\nСеть аэропортов сохранена в {output_file}")

    # Выводим статистику
    total_connections = sum(len(info.get("nearby_airports", [])) for info in network.values())
    avg_connections = total_connections / len(network) if network else 0

    print(f"\nСтатистика:")
    print(f"  - Всего аэропортов в сети: {len(network)}")
    print(f"  - Всего связей: {total_connections}")
    print(f"  - Среднее количество близких аэропортов: {avg_connections:.2f}")

    # Примеры
    print(f"\nПримеры близких аэропортов:")
    for i, (iata, info) in enumerate(list(network.items())[:5]):
        nearby_list = info.get("nearby_airports", [])
        if nearby_list:
            print(f"  {iata} ({info['municipality']}, {info['country']}):")
            for neighbor_item in nearby_list[:3]:
                neighbor_iata = neighbor_item.get("iata")
                neighbor_distance = neighbor_item.get("distance_km")
                if neighbor_iata in network:
                    neighbor_info = network[neighbor_iata]
                    print(f"    - {neighbor_iata} ({neighbor_info['municipality']}, {neighbor_info['country']}): {neighbor_distance} км")


def main():
    """Основная функция."""
    import argparse

    parser = argparse.ArgumentParser(description="Построение сети аэропортов")
    parser.add_argument(
        "--max-distance",
        type=float,
        default=100,
        help="Максимальное расстояние между аэропортами в км (по умолчанию: 100)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/airport_network.json",
        help="Путь к выходному файлу (по умолчанию: data/airport_network.json)"
    )

    args = parser.parse_args()

    try:
        # Загружаем данные об аэропортах
        airports = fetch_airports_from_huggingface()

        if not airports:
            print("Не удалось загрузить данные об аэропортах")
            return 1

        print(f"\nОбработка {len(airports)} аэропортов...")

        # Строим сеть
        network = build_airport_network(airports, max_distance_km=args.max_distance)

        # Сохраняем результат
        save_network(network, output_file=args.output)

        return 0

    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
