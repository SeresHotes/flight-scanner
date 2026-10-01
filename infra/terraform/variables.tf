variable "cloud_id" {
  type        = string
  description = "ID облака Yandex Cloud"
}

variable "folder_id" {
  type        = string
  description = "ID каталога, где создаются ресурсы"
}

variable "zone" {
  type        = string
  description = "Зона доступности"
  default     = "ru-central1-a"
}

variable "subnet_id" {
  type        = string
  description = "ID существующей подсети в var.zone (переиспользуем — квота на сети занята)"
  default     = "e9bc77jr01u7rgjpp5k8" # default-ru-central1-a
}

variable "bucket_name" {
  type        = string
  description = "Глобально уникальное имя бакета Object Storage (сид данных + бэкапы озера)"
}

# --- Домен / TLS ---

variable "site_domain" {
  type        = string
  description = "Домен сайта (A-запись → внешний IP VM). Caddy берёт по нему Let's Encrypt-серт"
  default     = "flights.sereshotes.dev"
}

# --- Секреты приложения ---

variable "travelpayouts_token" {
  type        = string
  description = "Токен Travelpayouts/Aviasales для сбора (serving из кэша не требует)"
  sensitive   = true
}

# --- Параметры VM ---

variable "vm_cores" {
  type        = number
  description = "Число vCPU"
  default     = 2
}

variable "vm_core_fraction" {
  type        = number
  description = "Гарантированная доля vCPU, % (20 = burstable, дёшево)"
  default     = 20
}

variable "vm_memory_gb" {
  type        = number
  description = "RAM, ГБ (8 с 01.10.2026: склад билетов в Postgres + планировщик)"
  default     = 8
}

variable "vm_disk_gb" {
  type        = number
  description = "Размер загрузочного диска, ГБ (данные + образы + SQLite + Postgres склада билетов ~30 ГБ)"
  default     = 100
}

variable "pg_disk_gb" {
  type        = number
  description = "Диск под Postgres склада билетов (network-ssd-nonreplicated, кратно 93 ГБ)"
  default     = 93
}

variable "ssh_public_key" {
  type        = string
  description = "Публичный SSH-ключ для доступа к VM (пользователь ubuntu)"
}

# --- Образ приложения ---

variable "image_tag" {
  type        = string
  description = "Тег образов flights-api / flights-web в Container Registry"
  default     = "latest"
}

# --- Контроль расходов на Object Storage ---

variable "bucket_max_size_gb" {
  type        = number
  description = "Жёсткий потолок размера бакета в ГБ (0 = без лимита). Ретеншн озера (LAKE_MAX_GB) держится ниже него"
  default     = 200
}

variable "lake_max_gb" {
  type        = number
  description = "Порог объёма Parquet-озера tickets/ в ГБ: выше — коллектор удаляет самые старые файлы"
  default     = 180
}

variable "rate_per_minute" {
  type        = number
  description = "Темп запросов коллектора к GraphQL Data API (лимит источника — 60/мин)"
  default     = 60
}
