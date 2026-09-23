#!/usr/bin/env bash
# Собирает каталог ./seed для заливки в Object Storage (s3://<bucket>/seed).
# Это стартовый набор данных, который cloud-init разворачивает в /opt/flights/data
# при первом бутe VM: справочник аэропортов + исторические выгрузки (идут в SQLite:
# «доступные» маршруты и покрытие) + ранее собранные маршруты.
#
# Запуск из корня репозитория:  deploy/make-seed.sh  [SRC_DATA_DIR] [OUT_DIR]
set -euo pipefail

SRC="${1:-data}"
OUT="${2:-seed}"

if [ ! -f "$SRC/airport_network.json" ]; then
  echo "ОШИБКА: $SRC/airport_network.json не найден — нечего сеять." >&2
  exit 1
fi

mkdir -p "$OUT/collected"

# Обязательный справочник аэропортов.
cp "$SRC/airport_network.json" "$OUT/"

# Исторические выгрузки коллектора (import_data_dir читает flights_*.json + mcr_*.json).
shopt -s nullglob
for f in "$SRC"/flights_*.json "$SRC"/mcr_*.json; do
  cp "$f" "$OUT/"
done

# Ранее собранные маршруты (durable, восстанавливаются в _load_collected).
if [ -d "$SRC/collected" ]; then
  for f in "$SRC"/collected/*.json; do
    cp "$f" "$OUT/collected/"
  done
fi
shopt -u nullglob

echo "seed готов: $OUT ($(du -sh "$OUT" | cut -f1))"
find "$OUT" -type f | sed "s#^$OUT/#  #" | head -50
