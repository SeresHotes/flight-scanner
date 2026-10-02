# ---------------------------------------------------------------------------
# Вход GitHub Actions в Yandex Cloud без постоянных ключей (Workload Identity Federation).
#
# Job берёт у GitHub OIDC-токен («я workflow репо X в environment prod») и меняет его в
# auth.yandex.cloud на IAM-токен SA (≤ 12 ч). Yandex Cloud выдаёт токен, только если
# subject совпал с привязкой ниже. В environment prod пускают только ветку main
# (deployment branch policy в GitHub), так что workflow с другой ветки — например,
# изменённый в PR — токена не получит. Секреты для CI лежат в Lockbox, читают их те же SA.
# Подробности — README.md, раздел «GitHub Actions без ключей».
# ---------------------------------------------------------------------------
locals {
  github_oidc_subject = "repo:${var.github_repository}:environment:${var.github_environment}"
}

resource "yandex_iam_workload_identity_oidc_federation" "github" {
  name        = "flights-github"
  description = "GitHub Actions ${var.github_repository}"
  issuer      = "https://token.actions.githubusercontent.com"
  jwks_url    = "https://token.actions.githubusercontent.com/.well-known/jwks"
  # Audience по умолчанию у OIDC-токена GitHub — https://github.com/<владелец>.
  audiences = ["https://github.com/${split("/", var.github_repository)[0]}"]
}

# flights-ci: push образов + SSH-ключ деплоя (deploy.yml, ops.yml).
resource "yandex_iam_workload_identity_federated_credential" "ci" {
  service_account_id  = yandex_iam_service_account.ci.id
  federation_id       = yandex_iam_workload_identity_oidc_federation.github.id
  external_subject_id = local.github_oidc_subject
}

# flights-terraform: plan/apply из terraform.yml. Terraform управляет SA и ролями каталога,
# поэтому admin на каталог (как у человека, который запускает apply руками).
resource "yandex_iam_service_account" "terraform" {
  name        = "flights-terraform"
  description = "SA для terraform.yml в GitHub Actions (вход через федерацию flights-github)"
}

resource "yandex_resourcemanager_folder_iam_member" "terraform_admin" {
  folder_id = var.folder_id
  role      = "admin"
  member    = "serviceAccount:${yandex_iam_service_account.terraform.id}"
}

resource "yandex_iam_workload_identity_federated_credential" "terraform" {
  service_account_id  = yandex_iam_service_account.terraform.id
  federation_id       = yandex_iam_workload_identity_oidc_federation.github.id
  external_subject_id = local.github_oidc_subject
}

# ---------------------------------------------------------------------------
# Lockbox: сами секреты. Terraform создаёт только «папки» и права; значения кладёт
# bootstrap-secrets.sh через yc — в state они не попадают.
#   flights-terraform: tfstate_access_key, tfstate_secret_key, travelpayouts_token
#   flights-ops:       vm_ssh_key (приватный ключ деплоя, пара — var.ci_ssh_public_key)
# ---------------------------------------------------------------------------
resource "yandex_lockbox_secret" "terraform" {
  name                = "flights-terraform"
  description         = "Ключ бакета состояния Terraform и TF_VAR_* секреты"
  deletion_protection = true
}

resource "yandex_lockbox_secret" "ops" {
  name                = "flights-ops"
  description         = "SSH-ключ деплоя на VM flights-app (пользователь ubuntu)"
  deletion_protection = true
}

resource "yandex_lockbox_secret_iam_member" "terraform_reads_terraform" {
  secret_id = yandex_lockbox_secret.terraform.id
  role      = "lockbox.payloadViewer"
  member    = "serviceAccount:${yandex_iam_service_account.terraform.id}"
}

resource "yandex_lockbox_secret_iam_member" "ci_reads_ops" {
  secret_id = yandex_lockbox_secret.ops.id
  role      = "lockbox.payloadViewer"
  member    = "serviceAccount:${yandex_iam_service_account.ci.id}"
}
