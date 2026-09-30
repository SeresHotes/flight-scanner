#!/usr/bin/env bash
# Одноразово на уже созданной VM: дописать в /opt/flights/.env переменные склада билетов
# (docs/TICKETS.md) — образ и пароль Postgres. Terraform кладёт их в .env только при создании
# VM (cloud-init не перезапускается), поэтому для живой машины — этот скрипт. Идемпотентно.
#
# Запуск с локальной машины из корня репозитория:
#   deploy/vm-tickets-env.sh ubuntu@93.77.186.45 [<пароль Postgres>]
# Пароль по умолчанию — из terraform (random_password.tickets_pg):
#   terraform -chdir=infra/terraform output -raw tickets_pg_password
set -euo pipefail

HOST="${1:?usage: deploy/vm-tickets-env.sh ubuntu@<vm_ip> [pg_password]}"
PASS="${2:-}"
if [ -z "$PASS" ]; then
  PASS=$(terraform -chdir="$(cd "$(dirname "$0")/.." && pwd)/infra/terraform" output -raw tickets_pg_password)
fi

ssh "$HOST" 'sudo bash -s' <<EOF
set -euo pipefail
cd /opt/flights
set -a; source /opt/flights/.env; set +a
[ -z "\$(tail -c1 .env)" ] || echo >> .env
if [ -z "\${TICKETS_IMAGE:-}" ]; then
  REG="\${PLANNER_IMAGE%/flights-planner:*}"
  TAG="\${PLANNER_IMAGE##*:}"
  echo "TICKETS_IMAGE=\$REG/flights-tickets:\$TAG" >> .env
fi
grep -q '^TICKETS_PG_PASSWORD=' .env || echo "TICKETS_PG_PASSWORD=$PASS" >> .env
mkdir -p /opt/flights/pg
grep -E '^TICKETS_' .env | sed -E 's/(PASSWORD=).*/\1…/'
EOF
echo "готово: следующий деплой (или sudo systemctl start flights-update.service) поднимет tickets и tickets-db"
