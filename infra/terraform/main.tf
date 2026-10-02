terraform {
  required_version = ">= 1.5"
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = ">= 0.100"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5"
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
# SA для CI (GitHub Actions): push образов в реестр. Ключей у него нет — входит через
# федерацию GitHub (github.tf).
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
# Статический публичный IP VM: на него смотрит A-запись site_domain. Без него
# адрес эфемерный и меняется при каждой остановке VM (смена памяти/диска).
# На проде адрес 93.77.186.45 (ранее эфемерный) зарезервирован 01.10.2026 через
# `yc vpc address update --reserved` и импортирован: terraform import yandex_vpc_address.app <id>.
# ---------------------------------------------------------------------------
resource "yandex_vpc_address" "app" {
  name = "flights-app-ip"
  external_ipv4_address {
    zone_id = var.zone
  }
}

# ---------------------------------------------------------------------------
# VM (burstable). cloud-init поднимает Docker, засеивает данные из S3 и
# запускает docker compose (planner + collector + crawler + caddy).
# См. cloud-init.yaml.tftpl. Смена ресурсов (память, диск) требует остановки VM —
# allow_stopping_for_update; данные на диске сохраняются, root-раздел cloud-init
# (growpart/resizefs) расширяет при загрузке.
# ---------------------------------------------------------------------------
resource "yandex_compute_instance" "app" {
  name                      = "flights-app"
  platform_id               = "standard-v3"
  zone                      = var.zone
  service_account_id        = yandex_iam_service_account.app.id
  allow_stopping_for_update = true

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
    subnet_id      = data.yandex_vpc_subnet.subnet.id
    nat            = true
    nat_ip_address = yandex_vpc_address.app.external_ipv4_address[0].address
  }

  metadata = {
    ssh-keys  = join("\n", [for k in [var.ssh_public_key, var.ci_ssh_public_key] : "ubuntu:${k}" if k != ""])
    user-data = local.cloud_init
  }

  # Семейство ubuntu-2204-lts обновляется — без этого каждый новый образ в семействе
  # заставляет terraform ПЕРЕСОЗДАТЬ VM (и потерять /opt/flights/data). cloud-init
  # (user-data) на живой VM тоже не перезапускается: правки шаблона применяются
  # только к новым машинам, для существующей — deploy/vm-migrate.sh.
  # Размер загрузочного диска в initialize_params провайдер меняет только пересозданием
  # VM, поэтому на живой машине диск растим руками (`yc compute disk update <id> --size N`,
  # root-раздел расширяет cloud-init growpart при следующей загрузке), а здесь размер
  # игнорируем; vm_disk_gb действует для новых VM. Прод: 20 → 100 ГБ 01.10.2026.
  lifecycle {
    ignore_changes = [boot_disk[0].initialize_params[0].image_id, boot_disk[0].initialize_params[0].size]
  }
}

locals {
  image_planner_ref   = "cr.yandex/${yandex_container_registry.flights.id}/flights-planner:${var.image_tag}"
  image_collector_ref = "cr.yandex/${yandex_container_registry.flights.id}/flights-collector:${var.image_tag}"
  image_crawler_ref   = "cr.yandex/${yandex_container_registry.flights.id}/flights-crawler:${var.image_tag}"
  image_web_ref       = "cr.yandex/${yandex_container_registry.flights.id}/flights-web:${var.image_tag}"

  # /opt/flights/.env: и переменные подстановки compose (${PLANNER_IMAGE}...), и секреты
  # (env_file для planner и collector). SITE_DOMAIN уходит в web (Caddy).
  # На уже созданной VM файл дополняет deploy/vm-migrate.sh (cloud-init не перезапускается).
  env_file = join("\n", [
    "SITE_DOMAIN=${var.site_domain}",
    "PLANNER_IMAGE=${local.image_planner_ref}",
    "COLLECTOR_IMAGE=${local.image_collector_ref}",
    "CRAWLER_IMAGE=${local.image_crawler_ref}",
    "WEB_IMAGE=${local.image_web_ref}",
    "TRAVELPAYOUTS_TOKEN=${var.travelpayouts_token}",
    "S3_ENDPOINT=https://storage.yandexcloud.net",
    "S3_REGION=ru-central1",
    "S3_BUCKET=${var.bucket_name}",
    "S3_ACCESS_KEY=${yandex_iam_service_account_static_access_key.app.access_key}",
    "S3_SECRET_KEY=${yandex_iam_service_account_static_access_key.app.secret_key}",
    "LAKE_MAX_GB=${var.lake_max_gb}",
    "RATE_PER_MINUTE=${var.rate_per_minute}",
    "", # завершающий перевод строки: иначе `echo >> .env` клеится к последней строке
  ])

  cloud_init = templatefile("${path.module}/cloud-init.yaml.tftpl", {
    env_b64 = base64encode(local.env_file)
  })
}
