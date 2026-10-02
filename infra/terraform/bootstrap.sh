#!/usr/bin/env bash
# Разовый переезд (README → «GitHub Actions без ключей»): запускать из основного checkout,
# где лежит локальный terraform.tfstate, после того как в main приехали state.tf/github.tf.
#   1. terraform apply при локальном state: бакет состояния + KMS, SA flights-tfstate и
#      flights-terraform, федерация GitHub, Lockbox (спросит подтверждение);
#   2. секреты → Lockbox: статический ключ flights-tfstate, токен Travelpayouts из
#      terraform.tfvars, SSH-ключ деплоя (создаётся здесь же);
#   3. публичный SSH-ключ деплоя → ~ubuntu/.ssh/authorized_keys на VM (вашим SSH);
#   4. state → бакет (terraform init -migrate-state), локальный файл → *.pre-s3;
#   5. GitHub: environment prod (только ветка main) и его переменные (ID, не секреты).
# Секреты не печатаются; шаги идемпотентны — при сбое скрипт можно перезапустить.
set -euo pipefail
cd "$(dirname "$0")"

REPO=${GITHUB_REPOSITORY:-SeresHotes/flight-scanner}
ENV_NAME=prod
CI_KEY=${CI_SSH_KEY:-$HOME/.ssh/flights_ci_deploy_ed25519}

step() { printf '\n== %s\n' "$*"; }
tfvar() { sed -n "s/^$1 *= *\"\(.*\)\"/\1/p" terraform.tfvars | head -1; }

export TF_CLI_CONFIG_FILE="$PWD/terraform.rc"
[ -f "$TF_CLI_CONFIG_FILE" ] || cp terraform.rc.example "$TF_CLI_CONFIG_FILE"
YC_TOKEN=$(yc iam create-token)
export YC_TOKEN

# --- 0. SSH-ключ деплоя: пара нужна до apply (публичный уходит в metadata VM) ---
step "0/5 SSH-ключ деплоя ($CI_KEY)"
[ -f "$CI_KEY" ] || ssh-keygen -t ed25519 -N "" -C "flights-ci-deploy" -f "$CI_KEY" -q
CI_PUB=$(cat "$CI_KEY.pub")
if ! grep -q '^ci_ssh_public_key' terraform.tfvars; then
  printf '\n# Публичный SSH-ключ деплоя для CI (приватный — в Lockbox flights-ops)\nci_ssh_public_key   = "%s"\n' "$CI_PUB" >> terraform.tfvars
fi

# --- 1. Ресурсы при локальном state (backend.tf перекрыт local-бэкендом) ---
if [ -f terraform.tfstate ]; then
  step "1/5 terraform apply при локальном state"
  printf 'terraform {\n  backend "local" {}\n}\n' > bootstrap_override.tf
  trap 'rm -f bootstrap_override.tf' EXIT
  terraform init -reconfigure -input=false >/dev/null
  terraform apply
else
  step "1/5 пропуск: локального terraform.tfstate нет (state уже в бакете?)"
fi

out() {
  if [ -f bootstrap_override.tf ]; then terraform output -raw "$1"; else ./tf.sh output -raw "$1"; fi
}
LB_TF=$(yc lockbox secret get --name flights-terraform --format json | jq -r .id)
LB_OPS=$(yc lockbox secret get --name flights-ops --format json | jq -r .id)

# --- 2. Секреты в Lockbox (мимо Terraform — в state их нет) ---
step "2/5 Lockbox"
if yc lockbox payload get --id "$LB_TF" --key tfstate_access_key >/dev/null 2>&1; then
  echo "flights-terraform: уже заполнен"
else
  TP_TOKEN=$(tfvar travelpayouts_token)
  [ -n "$TP_TOKEN" ] || { echo "нет travelpayouts_token в terraform.tfvars" >&2; exit 1; }
  yc iam access-key create --service-account-name flights-tfstate \
      --description "backend s3 Terraform (Lockbox flights-terraform)" --format json |
    jq --arg tp "$TP_TOKEN" '[
        {key: "tfstate_access_key", text_value: .access_key.key_id},
        {key: "tfstate_secret_key", text_value: .secret},
        {key: "travelpayouts_token", text_value: $tp}]' |
    yc lockbox secret add-version --id "$LB_TF" --payload - >/dev/null
  echo "flights-terraform: заполнен"
fi
if yc lockbox payload get --id "$LB_OPS" --key vm_ssh_key >/dev/null 2>&1; then
  echo "flights-ops: уже заполнен"
else
  jq -n --rawfile k "$CI_KEY" '[{key: "vm_ssh_key", text_value: $k}]' |
    yc lockbox secret add-version --id "$LB_OPS" --payload - >/dev/null
  echo "flights-ops: заполнен"
fi

# --- 3. Ключ деплоя на живую VM (metadata ssh-keys cloud-init читает только при создании) ---
VM_HOST=$(out vm_external_ip)
step "3/5 authorized_keys на $VM_HOST"
ssh "ubuntu@$VM_HOST" "grep -qxF '$CI_PUB' ~/.ssh/authorized_keys || echo '$CI_PUB' >> ~/.ssh/authorized_keys"
ssh -i "$CI_KEY" -o IdentitiesOnly=yes "ubuntu@$VM_HOST" true && echo "вход ключом деплоя: ok"

# --- 4. State в бакет ---
if [ -f bootstrap_override.tf ]; then
  step "4/5 state → s3://$(terraform output -raw tfstate_bucket)"
  rm -f bootstrap_override.tf
  AWS_ACCESS_KEY_ID=$(yc lockbox payload get --id "$LB_TF" --key tfstate_access_key)
  AWS_SECRET_ACCESS_KEY=$(yc lockbox payload get --id "$LB_TF" --key tfstate_secret_key)
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
  terraform init -migrate-state -force-copy -input=false
  mv terraform.tfstate terraform.tfstate.pre-s3
  [ -f terraform.tfstate.backup ] && mv terraform.tfstate.backup terraform.tfstate.backup.pre-s3
  ./tf.sh plan -detailed-exitcode -no-color >/dev/null && echo "plan из бакета: без изменений"
else
  step "4/5 пропуск: state уже в бакете"
fi

# --- 5. GitHub environment prod + переменные ---
step "5/5 GitHub $REPO: environment $ENV_NAME"
gh api -X PUT "repos/$REPO/environments/$ENV_NAME" --input - >/dev/null <<'EOF'
{"deployment_branch_policy": {"protected_branches": false, "custom_branch_policies": true}}
EOF
gh api "repos/$REPO/environments/$ENV_NAME/deployment-branch-policies" --jq '.branch_policies[].name' | grep -qx main ||
  gh api -X POST "repos/$REPO/environments/$ENV_NAME/deployment-branch-policies" -f name=main -f type=branch >/dev/null
KNOWN=$(ssh-keygen -F "$VM_HOST" | grep -v '^#' || true)
[ -n "$KNOWN" ] || KNOWN=$(ssh-keyscan -t ed25519 "$VM_HOST" 2>/dev/null)
setvar() { gh variable set "$1" --env "$ENV_NAME" --repo "$REPO" --body "$2"; }
setvar YC_CLOUD_ID          "$(tfvar cloud_id)"
setvar YC_FOLDER_ID         "$(tfvar folder_id)"
setvar YC_REGISTRY_ID       "$(out registry_id)"
setvar YC_CI_SA_ID          "$(out ci_sa_id)"
setvar YC_TERRAFORM_SA_ID   "$(out terraform_sa_id)"
setvar LOCKBOX_TERRAFORM_ID "$LB_TF"
setvar LOCKBOX_OPS_ID       "$LB_OPS"
setvar VM_HOST              "$VM_HOST"
setvar VM_SSH_KNOWN_HOSTS   "$KNOWN"
setvar TF_BUCKET_NAME       "$(tfvar bucket_name)"
setvar TF_SSH_PUBLIC_KEY    "$(tfvar ssh_public_key)"
setvar TF_CI_SSH_PUBLIC_KEY "$CI_PUB"

step "Готово. Проверка из GitHub: gh workflow run terraform.yml -f command=plan --repo $REPO"
