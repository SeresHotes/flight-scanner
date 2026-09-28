#!/usr/bin/env bash
# Одноразовая миграция уже созданной VM на схему planner + collector (docs/COLLECTOR.md).
# cloud-init на существующей VM не перезапускается, поэтому то, что terraform кладёт
# при создании (run.sh, .env), здесь дописывается руками. Идемпотентно.
#
# Запуск с локальной машины из корня репозитория (после мержа в main и сборки образов):
#   deploy/vm-migrate.sh ubuntu@93.77.186.45
#
# Что делает на VM:
#   1. Дополняет /opt/flights/.env: PLANNER_IMAGE, COLLECTOR_IMAGE, LAKE_MAX_GB, RATE_PER_MINUTE
#      (по образцу API_IMAGE, который там уже есть).
#   2. Заменяет /opt/flights/run.sh версией из infra/terraform/cloud-init.yaml.tftpl
#      (compose берётся из образа planner при каждом обновлении).
#   3. Останавливает старый стек (api/web), сжимает flights.db без ticket_cache
#      (VACUUM INTO — нужен диск только под живые данные), запускает run.sh.
set -euo pipefail

HOST="${1:?usage: deploy/vm-migrate.sh ubuntu@<vm_ip>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TPL="$ROOT/infra/terraform/cloud-init.yaml.tftpl"

# run.sh из шаблона cloud-init: блок между "path: /opt/flights/run.sh" и следующим "- path:".
RUN_SH=$(awk '
  /path: \/opt\/flights\/run.sh/ {grab=1; next}
  grab && /content: \|/ {body=1; next}
  body && /^  - path:/ {exit}
  body {sub(/^      /, ""); print}
' "$TPL")
if ! grep -q "docker compose up -d" <<<"$RUN_SH"; then
  echo "не удалось вырезать run.sh из $TPL" >&2
  exit 1
fi

SHRINK_PY=$(cat "$ROOT/scripts/shrink_planner_db.py")

ssh "$HOST" 'sudo bash -s' <<EOF
set -euo pipefail
cd /opt/flights
set -a; source /opt/flights/.env; set +a

# 1. .env: образы новой схемы по образцу API_IMAGE. Terraform писал файл без
# завершающего перевода строки — иначе первая дописанная строка клеится к секрету S3.
[ -z "\$(tail -c1 .env)" ] || echo >> .env
if [ -z "\${PLANNER_IMAGE:-}" ]; then
  REG="\${API_IMAGE%/flights-api:*}"
  TAG="\${API_IMAGE##*:}"
  echo "PLANNER_IMAGE=\$REG/flights-planner:\$TAG" >> .env
  echo "COLLECTOR_IMAGE=\$REG/flights-collector:\$TAG" >> .env
fi
if [ -z "\${CRAWLER_IMAGE:-}" ]; then
  REG="\${PLANNER_IMAGE%/flights-planner:*}"
  TAG="\${PLANNER_IMAGE##*:}"
  echo "CRAWLER_IMAGE=\$REG/flights-crawler:\$TAG" >> .env
fi
grep -q '^LAKE_MAX_GB=' .env || echo "LAKE_MAX_GB=180" >> .env
grep -q '^RATE_PER_MINUTE=' .env || echo "RATE_PER_MINUTE=60" >> .env

# 2. run.sh из шаблона cloud-init.
cp -n run.sh run.sh.bak-before-collector || true
cat > run.sh.new <<'RUNSH'
$RUN_SH
RUNSH
chmod 0755 run.sh.new && mv run.sh.new run.sh

# 3. Старый стек вниз, сжать базу планировщика, поднять новый.
docker compose down --remove-orphans || true
cat > /tmp/shrink_planner_db.py <<'PYEOF'
$SHRINK_PY
PYEOF
python3 /tmp/shrink_planner_db.py /opt/flights/data/flights.db
chown -R 1000:1000 /opt/flights/data
systemctl start flights-update.service
docker ps --format '{{.Names}} {{.Image}} {{.Status}}'
EOF

echo "готово: проверь https://\$(ssh "$HOST" 'set -a; source /opt/flights/.env; echo \$SITE_DOMAIN')/api/health"
