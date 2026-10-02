# ---------------------------------------------------------------------------
# Хранилище состояния Terraform (backend "s3" в backend.tf).
#
# В state лежат секреты (ключи SA, S3-ключ приложения, токен в user-data VM), поэтому
# бакет приватный, шифруется KMS-ключом, с версионированием (откат испорченного state —
# предыдущей версией объекта). Пишет в него только SA flights-tfstate: его статический
# ключ (S3 понимает только подпись SigV4) лежит в Lockbox flights-terraform, сам ключ
# создаёт bootstrap-secrets.sh мимо Terraform — чтобы его не было в этом же state.
# ---------------------------------------------------------------------------
resource "yandex_kms_symmetric_key" "tfstate" {
  name                = "flights-tfstate"
  description         = "Шифрование бакета состояния Terraform"
  default_algorithm   = "AES_256"
  rotation_period     = "8760h"
  deletion_protection = true

  lifecycle {
    prevent_destroy = true
  }
}

resource "yandex_iam_service_account" "tfstate" {
  name        = "flights-tfstate"
  description = "Доступ только к бакету состояния Terraform (статический ключ в Lockbox)"
}

resource "yandex_kms_symmetric_key_iam_member" "tfstate" {
  symmetric_key_id = yandex_kms_symmetric_key.tfstate.id
  role             = "kms.keys.encrypterDecrypter"
  member           = "serviceAccount:${yandex_iam_service_account.tfstate.id}"
}

resource "yandex_storage_bucket" "tfstate" {
  bucket = var.tfstate_bucket_name
  # Без статических ключей (IAM-токеном) провайдер не знает каталог бакета — задаём явно.
  folder_id = var.folder_id

  anonymous_access_flags {
    read        = false
    list        = false
    config_read = false
  }

  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        kms_master_key_id = yandex_kms_symmetric_key.tfstate.id
        sse_algorithm     = "aws:kms"
      }
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "yandex_storage_bucket_grant" "tfstate" {
  bucket = yandex_storage_bucket.tfstate.bucket

  grant {
    id          = yandex_iam_service_account.tfstate.id
    type        = "CanonicalUser"
    permissions = ["READ", "WRITE"]
  }
}
