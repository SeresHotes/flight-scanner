output "vm_external_ip" {
  description = "Внешний IP VM. Заведите A-запись site_domain -> этот IP в Cloudflare"
  value       = yandex_compute_instance.app.network_interface[0].nat_ip_address
}

output "site_url" {
  description = "URL сайта после настройки DNS и получения TLS-сертификата"
  value       = "https://${var.site_domain}"
}

output "registry_id" {
  description = "ID Container Registry (в имени образов и в GitHub Secret YC_REGISTRY_ID)"
  value       = yandex_container_registry.flights.id
}

output "image_planner_ref" {
  description = "Полное имя образа планировщика (FastAPI /api), который тянет VM"
  value       = local.image_planner_ref
}

output "image_collector_ref" {
  description = "Полное имя образа коллектора (очередь к GraphQL + Parquet-озеро)"
  value       = local.image_collector_ref
}

output "image_crawler_ref" {
  description = "Полное имя образа фонового сборщика (покрытие город × день)"
  value       = local.image_crawler_ref
}

output "image_web_ref" {
  description = "Полное имя web-образа (Caddy + фронт), который тянет VM"
  value       = local.image_web_ref
}

output "bucket_name" {
  description = "Бакет Object Storage (сид данных в s3://<bucket>/seed + бэкапы)"
  value       = yandex_storage_bucket.data.bucket
}

output "s3_access_key" {
  description = "S3 access key SA приложения (для заливки сида и бэкапов)"
  value       = yandex_iam_service_account_static_access_key.app.access_key
  sensitive   = true
}

output "s3_secret_key" {
  description = "S3 secret key SA приложения"
  value       = yandex_iam_service_account_static_access_key.app.secret_key
  sensitive   = true
}

# --- Для GitHub Actions (переменные environment prod, см. README) ---

output "tfstate_bucket" {
  description = "Бакет состояния Terraform"
  value       = yandex_storage_bucket.tfstate.bucket
}

output "github_oidc_subject" {
  description = "subject OIDC-токена GitHub, которому федерация выдаёт IAM-токены"
  value       = local.github_oidc_subject
}

output "ci_sa_id" {
  description = "SA flights-ci (vars.YC_CI_SA_ID)"
  value       = yandex_iam_service_account.ci.id
}

output "terraform_sa_id" {
  description = "SA flights-terraform (vars.YC_TERRAFORM_SA_ID)"
  value       = yandex_iam_service_account.terraform.id
}

output "lockbox_terraform_id" {
  description = "Lockbox flights-terraform (vars.LOCKBOX_TERRAFORM_ID)"
  value       = yandex_lockbox_secret.terraform.id
}

output "lockbox_ops_id" {
  description = "Lockbox flights-ops (vars.LOCKBOX_OPS_ID)"
  value       = yandex_lockbox_secret.ops.id
}

output "seed_and_push_hint" {
  description = "Что сделать сразу после apply: залить сид и собрать/запушить образы"
  value       = <<-EOT
    # 1) Залить сид данных (нужен для serving и справочника аэропортов):
    AWS_ACCESS_KEY_ID=<s3_access_key> AWS_SECRET_ACCESS_KEY=<s3_secret_key> \
      aws --endpoint-url https://storage.yandexcloud.net \
      s3 sync ./seed s3://${var.bucket_name}/seed

    # 2) Собрать и запушить образы (VM подхватит таймером flights-update):
    yc container registry configure-docker
    docker build -t ${local.image_planner_ref} -f deploy/Dockerfile.planner .
    docker build -t ${local.image_collector_ref} -f deploy/Dockerfile.collector .
    docker build -t ${local.image_crawler_ref} -f deploy/Dockerfile.crawler .
    docker build -t ${local.image_web_ref} -f deploy/Dockerfile.web .
    docker push ${local.image_planner_ref}
    docker push ${local.image_collector_ref}
    docker push ${local.image_crawler_ref}
    docker push ${local.image_web_ref}

    # 3) Завести A-запись ${var.site_domain} -> vm_external_ip в Cloudflare (proxy off).
  EOT
}
