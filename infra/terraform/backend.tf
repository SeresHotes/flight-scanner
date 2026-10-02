# Состояние — в бакете Object Storage (state.tf), не на ноутбуке. Ключ бакета берётся из
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY: локально их выставляет ./tf.sh из Lockbox
# flights-terraform, в GitHub Actions — terraform.yml. Блокировка — lock-файлом рядом со
# state (use_lockfile, условная запись S3), версии state хранит версионирование бакета.
terraform {
  backend "s3" {
    bucket = "sereshotes-flights-tfstate"
    key    = "flights/terraform.tfstate"
    region = "ru-central1"
    endpoints = {
      s3 = "https://storage.yandexcloud.net"
    }
    use_lockfile = true

    skip_region_validation      = true
    skip_credentials_validation = true
    skip_requesting_account_id  = true
    skip_s3_checksum            = true
  }
}
