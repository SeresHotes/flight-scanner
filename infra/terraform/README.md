# Деплой Flight Scanner в Yandex Cloud (Фаза 3)

Одна burstable-VM + Docker Compose (`api` FastAPI + `caddy` статика/reverse-proxy/TLS),
Object Storage (сид данных + бэкапы озера), Container Registry (образы). Terraform
поднимает всю инфру, cloud-init на VM ставит Docker, засеивает данные из бакета и
поднимает стек; systemd-таймер каждые 2 минуты подтягивает свежие образы.

```
Cloudflare DNS: flights.sereshotes.dev ──A──> VM external IP
Интернет ──443──> Caddy ──/api──> FastAPI(api) ──> SQLite + Parquet (том /opt/flights/data)
                    └──/──> статика фронта (/srv)     ▲ сид из Object Storage при первом бутe
Container Registry: cr.yandex/<reg>/flights-api|flights-web:latest  (VM тянет по IAM SA)
```

## Ресурсы (Terraform)

- SA `flights-app` — `storage.admin` + `container-registry.images.puller`, статический S3-ключ; это SA самой VM.
- SA `flights-ci` — `container-registry.images.pusher`, authorized-key для GitHub Actions.
- Bucket `bucket_name` (Object Storage), Registry `flights`, сеть `flights-net` + подсеть.
- VM `flights-app` (standard-v3, 2 vCPU / 20% / 4 ГБ, диск 20 ГБ, публичный IP) + cloud-init.

## Разовый провижининг

```sh
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # заполнить cloud_id/folder_id/токен/ssh-ключ
export YC_TOKEN=$(yc iam create-token)
terraform init
terraform apply

# 1) Залить сид данных в бакет (без него нет справочника аэропортов и «доступных» маршрутов):
#    seed собирается скриптом deploy/make-seed.sh из локального data/ (запускать из корня репо).
(cd ../.. && deploy/make-seed.sh)               # → ./seed (~49 МБ)
export AWS_ACCESS_KEY_ID=$(terraform output -raw s3_access_key)
export AWS_SECRET_ACCESS_KEY=$(terraform output -raw s3_secret_key)
aws --endpoint-url https://storage.yandexcloud.net \
    s3 sync ../../seed "s3://$(terraform output -raw bucket_name)/seed"

# 2) Собрать и запушить образы (VM подхватит таймером через ~2 мин, либо сразу — см. ниже):
yc container registry configure-docker
docker build -t "$(terraform output -raw image_api_ref)" -f deploy/Dockerfile .
docker build -t "$(terraform output -raw image_web_ref)" -f deploy/Dockerfile.web .
docker push "$(terraform output -raw image_api_ref)"
docker push "$(terraform output -raw image_web_ref)"

# 3) DNS: A-запись site_domain -> vm_external_ip (Cloudflare, proxy OFF на время выпуска TLS).
terraform output vm_external_ip
```

После DNS Caddy автоматически берёт Let's Encrypt-сертификат (HTTP-01). Проверка:
`curl -fsS https://flights.sereshotes.dev/api/health`.

## Авто-деплой из GitHub

Задать секреты репозитория (Settings → Secrets and variables → Actions):

```sh
terraform output -raw registry_id     # → секрет YC_REGISTRY_ID
echo "$YC_FOLDER_ID"                   # → секрет YC_FOLDER_ID
terraform output -raw ci_sa_key_json   # → секрет YC_SA_KEY_JSON
```

Дальше push в `main` (пути api/core/storage/frontend/deploy) → GitHub Actions собирает
и пушит образы → VM подхватывает их таймером `flights-update` без ручного вмешательства.

## Эксплуатация

- SSH: `ssh ubuntu@<vm_external_ip>`.
- Логи стека: `sudo journalctl -u flights -f` и `cd /opt/flights && sudo docker compose logs -f`.
- Форсировать апдейт: `sudo systemctl start flights-update`.
- Старые образы `run.sh` удаляет сам (`docker image prune -f` до pull и после up).
  Симптом, если диск всё же забит: CI зелёный, а прод на старой версии; в
  `sudo journalctl -u flights-update -n 25` — `no space left on device`. Проверка: `df -h /`,
  `sudo docker system df`. cloud-init применяется только при создании VM — правки
  `run.sh` в шаблоне на живую машину доносить вручную.
- Данные (SQLite + collected + Parquet-озеро) переживают перезапуски: том `/opt/flights/data`.
- Секреты бакета для ручной заливки: `terraform output -raw s3_access_key` / `s3_secret_key`.

## Снос

```sh
terraform destroy    # удалит VM/сеть/registry/бакет (бакет должен быть пуст — очистить сид)
```
