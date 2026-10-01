#!/usr/bin/env bash
# Одноразово на VM: перенести Postgres склада билетов (/opt/flights/pg) на отдельный SSD-диск
# (terraform `yandex_compute_disk.pg`, device_name flights-pg → /dev/disk/by-id/virtio-flights-pg).
# Размечает диск (если пустой), монтирует его в /opt/flights/pg через fstab, останавливает
# tickets и tickets-db, переносит данные, поднимает стек. Идемпотентно: смонтированный диск
# не трогает. Простой склада — время копирования (~28 ГБ, несколько минут).
#
# Запуск с локальной машины из корня репозитория:
#   deploy/vm-pg-ssd.sh ubuntu@93.77.186.45
set -euo pipefail

HOST="${1:?usage: deploy/vm-pg-ssd.sh ubuntu@<vm_ip>}"

ssh "$HOST" 'sudo bash -s' <<'EOF'
set -euo pipefail
DEV=/dev/disk/by-id/virtio-flights-pg
MNT=/opt/flights/pg
[ -e "$DEV" ] || { echo "диск $DEV не найден — сначала terraform apply (yandex_compute_disk.pg)"; exit 1; }
if findmnt -rn "$MNT" >/dev/null; then
  echo "уже смонтирован: $(findmnt -rn -o SOURCE,FSTYPE,SIZE "$MNT")"; exit 0
fi
if ! blkid "$DEV" >/dev/null; then
  echo "размечаю $DEV (ext4)"
  mkfs.ext4 -q -L flights-pg -E lazy_itable_init=0,lazy_journal_init=0 "$DEV"
fi
cd /opt/flights
echo "останавливаю склад"
docker compose stop tickets tickets-db
mkdir -p /mnt/pg-new
mount "$DEV" /mnt/pg-new
if [ -d "$MNT" ] && [ -n "$(ls -A "$MNT" 2>/dev/null)" ]; then
  echo "копирую $MNT → $DEV"
  rsync -a --info=progress2 "$MNT/" /mnt/pg-new/
  mv "$MNT" "${MNT}.hdd-$(date +%Y%m%d-%H%M)"
fi
umount /mnt/pg-new
mkdir -p "$MNT"
UUID=$(blkid -s UUID -o value "$DEV")
grep -q "$UUID" /etc/fstab || echo "UUID=$UUID $MNT ext4 defaults,noatime,nofail 0 2" >> /etc/fstab
mount "$MNT"
echo "смонтирован: $(findmnt -rn -o SOURCE,FSTYPE,SIZE,USED "$MNT")"
docker compose up -d tickets-db tickets
sleep 5
docker compose ps tickets-db tickets
echo "старая копия на HDD оставлена рядом (${MNT}.hdd-*): удалить после проверки — rm -rf"
EOF
