terraform {
  required_version = ">= 1.5"
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = ">= 0.100"
    }
  }
}

# Провайдер аутентифицируется вашими кредами (yc CLI / SA-ключ / OAuth):
#   export YC_TOKEN=$(yc iam create-token)
provider "yandex" {
  cloud_id  = var.cloud_id
  folder_id = var.folder_id
  zone      = var.zone
}

# ---------------------------------------------------------------------------
# SA приложения: пишет в Object Storage (сид/бэкапы) и тянет образы (VM).
# ---------------------------------------------------------------------------
resource "yandex_iam_service_account" "app" {
  name        = "flights-app"
  description = "SA Flight Scanner: Object Storage + pull образов"
}

resource "yandex_resourcemanager_folder_iam_member" "storage_admin" {
  folder_id = var.folder_id
  role      = "storage.admin"
  member    = "serviceAccount:${yandex_iam_service_account.app.id}"
}

resource "yandex_resourcemanager_folder_iam_member" "registry_puller" {
  folder_id = var.folder_id
  role      = "container-registry.images.puller"
  member    = "serviceAccount:${yandex_iam_service_account.app.id}"
}

# Статический ключ для S3-доступа приложения (seed из бакета, будущие бэкапы озера).
resource "yandex_iam_service_account_static_access_key" "app" {
  service_account_id = yandex_iam_service_account.app.id
  description        = "S3 static key для flights-app"
}

# ---------------------------------------------------------------------------
# Object Storage: сид данных (data/seed) + место под бэкапы Parquet-озера (Фаза 4).
# ---------------------------------------------------------------------------
resource "yandex_storage_bucket" "data" {
  bucket     = var.bucket_name
  access_key = yandex_iam_service_account_static_access_key.app.access_key
  secret_key = yandex_iam_service_account_static_access_key.app.secret_key

  max_size = var.bucket_max_size_gb > 0 ? var.bucket_max_size_gb * 1024 * 1024 * 1024 : 0

  depends_on = [yandex_resourcemanager_folder_iam_member.storage_admin]
}

# ---------------------------------------------------------------------------
# Container Registry: сюда пушим flights-api и flights-web, VM тянет их.
# ---------------------------------------------------------------------------
resource "yandex_container_registry" "flights" {
  name = "flights"
}

# ---------------------------------------------------------------------------
# SA для CI (GitHub Actions): только push образов в реестр.
# ---------------------------------------------------------------------------
resource "yandex_iam_service_account" "ci" {
  name        = "flights-ci"
  description = "SA для GitHub Actions: push образов в Container Registry"
}

resource "yandex_resourcemanager_folder_iam_member" "ci_pusher" {
  folder_id = var.folder_id
  role      = "container-registry.images.pusher"
  member    = "serviceAccount:${yandex_iam_service_account.ci.id}"
}

resource "yandex_iam_service_account_key" "ci" {
  service_account_id = yandex_iam_service_account.ci.id
  description        = "Authorized key для GitHub Actions"
}

# ---------------------------------------------------------------------------
# Сеть: переиспользуем существующую подсеть (квота vpc.networks в фолдере занята
# сетями default/mdfetcher-net). VM получает публичный IP (nat=true).
# ---------------------------------------------------------------------------
data "yandex_vpc_subnet" "subnet" {
  subnet_id = var.subnet_id
}

data "yandex_compute_image" "ubuntu" {
  family = "ubuntu-2204-lts"
}

# ---------------------------------------------------------------------------
# VM (burstable). cloud-init поднимает Docker, засеивает данные из S3 и
# запускает docker compose (api + caddy). См. cloud-init.yaml.tftpl.
# ---------------------------------------------------------------------------
resource "yandex_compute_instance" "app" {
  name               = "flights-app"
  platform_id        = "standard-v3"
  zone               = var.zone
  service_account_id = yandex_iam_service_account.app.id

  resources {
    cores         = var.vm_cores
    core_fraction = var.vm_core_fraction
    memory        = var.vm_memory_gb
  }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = var.vm_disk_gb
    }
  }

  network_interface {
    subnet_id = data.yandex_vpc_subnet.subnet.id
    nat       = true
  }

  metadata = {
    ssh-keys  = "ubuntu:${var.ssh_public_key}"
    user-data = local.cloud_init
  }
}

locals {
  image_api_ref = "cr.yandex/${yandex_container_registry.flights.id}/flights-api:${var.image_tag}"
  image_web_ref = "cr.yandex/${yandex_container_registry.flights.id}/flights-web:${var.image_tag}"

  # /opt/flights/.env: и переменные подстановки compose (${API_IMAGE}...), и секреты
  # приложения (env_file для контейнера api). SITE_DOMAIN уходит в web (Caddy).
  env_file = join("\n", [
    "SITE_DOMAIN=${var.site_domain}",
    "API_IMAGE=${local.image_api_ref}",
    "WEB_IMAGE=${local.image_web_ref}",
    "TRAVELPAYOUTS_TOKEN=${var.travelpayouts_token}",
    "S3_ENDPOINT=https://storage.yandexcloud.net",
    "S3_REGION=ru-central1",
    "S3_BUCKET=${var.bucket_name}",
    "S3_ACCESS_KEY=${yandex_iam_service_account_static_access_key.app.access_key}",
    "S3_SECRET_KEY=${yandex_iam_service_account_static_access_key.app.secret_key}",
  ])

  cloud_init = templatefile("${path.module}/cloud-init.yaml.tftpl", {
    env_b64 = base64encode(local.env_file)
  })
}
