#!/usr/bin/env bash
# terraform с кредами для локального запуска: IAM-токен — вашим yc, ключ бакета состояния
# и секретные TF_VAR_* — из Lockbox flights-terraform (значения не печатаются).
#   ./tf.sh plan
#   ./tf.sh apply
set -euo pipefail
cd "$(dirname "$0")"

export TF_CLI_CONFIG_FILE="$PWD/terraform.rc"
[ -f "$TF_CLI_CONFIG_FILE" ] || cp terraform.rc.example "$TF_CLI_CONFIG_FILE"

YC_TOKEN=$(yc iam create-token)
export YC_TOKEN

lockbox() { yc lockbox payload get --name flights-terraform --key "$1"; }
AWS_ACCESS_KEY_ID=$(lockbox tfstate_access_key)
AWS_SECRET_ACCESS_KEY=$(lockbox tfstate_secret_key)
TF_VAR_travelpayouts_token=$(lockbox travelpayouts_token)
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY TF_VAR_travelpayouts_token

[ -d .terraform ] || terraform init -input=false
exec terraform "$@"
