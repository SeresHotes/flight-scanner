#!/usr/bin/env bash
# Читает секрет Lockbox по IAM-токену из YC_TOKEN (после .github/actions/yc-oidc) и раздаёт
# значения, не печатая их:
#   lockbox.sh <secret_id> key=ENV_NAME   — в $GITHUB_ENV (значение маскируется в логах)
#   lockbox.sh <secret_id> key=@/path     — в файл с правами 600 (многострочные, SSH-ключ)
set -euo pipefail
SECRET_ID=$1; shift
PAYLOAD=$(curl -sSf -H "Authorization: Bearer $YC_TOKEN" \
  "https://payload.lockbox.api.cloud.yandex.net/lockbox/v1/secrets/$SECRET_ID/payload")
for spec in "$@"; do
  key=${spec%%=*}; dest=${spec#*=}
  value=$(jq -r --arg k "$key" '.entries[] | select(.key == $k) | .textValue // empty' <<<"$PAYLOAD")
  [ -n "$value" ] || { echo "::error::В Lockbox $SECRET_ID нет ключа $key"; exit 1; }
  if [[ $dest == @* ]]; then
    install -m 600 /dev/null "${dest#@}"
    printf '%s\n' "$value" > "${dest#@}"
  else
    echo "::add-mask::$value"
    echo "$dest=$value" >> "$GITHUB_ENV"
  fi
done
