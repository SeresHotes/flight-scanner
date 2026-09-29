#!/usr/bin/env bash
# Подключает данные flights к общей аналитической VM (ClickHouse + Grafana, проект
# market-data-fetcher) и заливает дашборды в папку Grafana «Flights». Идемпотентно.
#
#   analytics/setup.sh ubuntu@89.169.140.225
#
# Окружение (необязательно):
#   SSH_OPTS                 — ключ к аналитической VM, по умолчанию "-i ~/.ssh/mdfetcher_vm"
#   FLIGHTS_TF_DIR           — terraform этого репозитория (S3-ключи бакета flights),
#                              по умолчанию infra/terraform основного checkout
#   FLIGHTS_CH_PASSWORD      — пароль пользователя ClickHouse flights_grafana (иначе генерируется;
#                              users.d/flights.xml и datasource Grafana обновляются вместе)
# Пароль admin Grafana берётся на самой VM из /opt/analytics/.env (GRAFANA_ADMIN_PASSWORD),
# шаг Grafana выполняется там же через localhost:3000 — пароль не покидает VM.
#
# Что делает:
#   1. Рендерит named collections бакета flights → /opt/analytics/clickhouse/config.d/
#      named_collections_flights.xml и пользователя flights_grafana → users.d/flights.xml
#      (файлы Market Data не трогает), SYSTEM RELOAD CONFIG, применяет init-flights.sql.
#   2. В Grafana создаёт/обновляет источник «ClickHouse Flights» (uid flights-clickhouse),
#      папку «Flights» и заливает analytics/grafana/dashboards/*.json.
set -euo pipefail

HOST="${1:?usage: analytics/setup.sh ubuntu@<analytics_vm_ip>}"
SSH_OPTS="${SSH_OPTS:--i $HOME/.ssh/mdfetcher_vm}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FLIGHTS_TF_DIR="${FLIGHTS_TF_DIR:-$HOME/Projects/flight_scanner/infra/terraform}"
GRAFANA_URL="http://${HOST#*@}:3000"

BUCKET=$(terraform -chdir="$FLIGHTS_TF_DIR" output -raw bucket_name)
S3_KEY=$(terraform -chdir="$FLIGHTS_TF_DIR" output -raw s3_access_key)
S3_SECRET=$(terraform -chdir="$FLIGHTS_TF_DIR" output -raw s3_secret_key)
CH_PASSWORD="${FLIGHTS_CH_PASSWORD:-$(openssl rand -hex 16)}"

render() { sed -e "s#__BUCKET__#$BUCKET#g" -e "s#__S3_KEY__#$S3_KEY#g" -e "s#__S3_SECRET__#$S3_SECRET#g" \
               -e "s#__CH_PASSWORD__#$CH_PASSWORD#g" "$1"; }

echo "== ClickHouse на $HOST"
{
  echo "NAMED_B64=$(render "$ROOT/analytics/clickhouse/named_collections_flights.xml.tmpl" | base64)"
  echo "USERS_B64=$(render "$ROOT/analytics/clickhouse/users_flights.xml.tmpl" | base64)"
  echo "INIT_B64=$(base64 < "$ROOT/analytics/clickhouse/init-flights.sql")"
  cat <<'REMOTE'
set -euo pipefail
echo "$NAMED_B64" | base64 -d | sudo tee /opt/analytics/clickhouse/config.d/named_collections_flights.xml >/dev/null
sudo chmod 0644 /opt/analytics/clickhouse/config.d/named_collections_flights.xml
echo "$USERS_B64" | base64 -d | sudo tee /opt/analytics/clickhouse/users.d/flights.xml >/dev/null
sudo chmod 0644 /opt/analytics/clickhouse/users.d/flights.xml
CH=$(sudo docker ps --filter name=clickhouse --format '{{.Names}}' | head -1)
sudo docker exec "$CH" clickhouse-client -q "SYSTEM RELOAD CONFIG"
sleep 3
echo "$INIT_B64" | base64 -d | sudo docker exec -i "$CH" clickhouse-client -n
sudo docker exec "$CH" clickhouse-client -q "SELECT name FROM system.tables WHERE database='flights'"
sudo docker exec "$CH" clickhouse-client --user flights_grafana --password "$CH_PASSWORD" -q "SELECT count() FROM flights.coverage" \
  || echo "flights.coverage пока пуст/недоступен (coverage/latest.parquet появится через 10 мин после старта коллектора)"
REMOTE
} | ssh $SSH_OPTS "$HOST" "CH_PASSWORD='$CH_PASSWORD' bash -s"

echo "== Grafana (на VM, пароль admin из /opt/analytics/.env)"
DASH_TAR_B64=$(COPYFILE_DISABLE=1 tar --no-xattrs -C "$ROOT/analytics/grafana/dashboards" -czf - . 2>/dev/null | base64)
{
  echo "DASH_TAR_B64=$DASH_TAR_B64"
  cat <<'REMOTE'
set -euo pipefail
set -a; source <(sudo cat /opt/analytics/.env); set +a
TMP=$(mktemp -d); echo "$DASH_TAR_B64" | base64 -d | tar -xzf - -C "$TMP"
export GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
python3 - "$TMP" <<'PY'
import base64, glob, json, os, sys, urllib.request, urllib.error

url, pw = os.environ["GRAFANA_URL"], os.environ["GRAFANA_ADMIN_PASSWORD"]
auth = "Basic " + base64.b64encode(("admin:" + pw).encode()).decode()


def call(method, path, body=None):
    req = urllib.request.Request(url + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json", "Authorization": auth})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


ds = {"name": "ClickHouse Flights", "type": "grafana-clickhouse-datasource", "uid": "flights-clickhouse",
      "access": "proxy", "isDefault": False,
      "jsonData": {"protocol": "native", "host": "clickhouse", "port": 9000,
                   "username": "flights_grafana", "defaultDatabase": "flights"},
      "secureJsonData": {"password": os.environ["CH_PASSWORD"]}}
code, _ = call("POST", "/api/datasources", ds)
if code == 409:
    code, _ = call("PUT", "/api/datasources/uid/flights-clickhouse", ds)
print("datasource:", code)

code, folder = call("POST", "/api/folders", {"uid": "flights", "title": "Flights"})
if code in (409, 412):  # уже есть (412 — «version mismatch» у существующей папки)
    code, folder = call("GET", "/api/folders/flights")
print("folder:", code, folder.get("uid"))

for path in sorted(glob.glob(sys.argv[1] + "/*.json")):
    dash = json.load(open(path, encoding="utf-8"))
    dash["id"] = None
    code, resp = call("POST", "/api/dashboards/db", {"dashboard": dash, "folderUid": "flights", "overwrite": True})
    print(os.path.basename(path), "->", code, resp.get("url") or resp.get("message"))
PY
rm -rf "$TMP"
REMOTE
} | ssh $SSH_OPTS "$HOST" "CH_PASSWORD='$CH_PASSWORD' bash -s"

echo "готово: $GRAFANA_URL/dashboards/f/flights"
