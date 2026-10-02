# Деплой Flight Scanner в Yandex Cloud (Фаза 3)

Одна burstable-VM + Docker Compose (`api` FastAPI + `caddy` статика/reverse-proxy/TLS),
Object Storage (сид данных + бэкапы озера), Container Registry (образы). Terraform
поднимает всю инфру, cloud-init на VM ставит Docker, засеивает данные из бакета и
поднимает стек; systemd-таймер каждые 2 минуты подтягивает свежие образы.

```
Cloudflare DNS: flights.sereshotes.dev ──A──> VM external IP
Интернет ──443──> Caddy ──/api──> FastAPI(api) ──> SQLite + Parquet (том /opt/flights/data)
                    └──/──> статика фронта (/srv)     ▲ сид из Object Storage при первом бутe
Container Registry: cr.yandex/<reg>/flights-planner|flights-collector|flights-crawler|flights-web:latest  (VM тянет по IAM SA)
```

## Ресурсы (Terraform)

- SA `flights-app` — `storage.admin` + `container-registry.images.puller`, статический S3-ключ; это SA самой VM.
- SA `flights-ci` — `container-registry.images.pusher`; GitHub Actions входит им через федерацию (ниже).
- SA `flights-terraform` — `admin` на каталог, для `terraform.yml` (тоже через федерацию).
- Федерация `flights-github` (Workload Identity, `github.tf`), Lockbox `flights-terraform` и `flights-ops`.
- Состояние Terraform — бакет `sereshotes-flights-tfstate` (`state.tf`, `backend.tf`): приватный,
  KMS-ключ `flights-tfstate`, версионирование, блокировка lock-файлом; пишет SA `flights-tfstate`.
- Bucket `bucket_name` (Object Storage), Registry `flights`, сеть `flights-net` + подсеть.
- VM `flights-app` (standard-v3, 2 vCPU / 100% / 12 ГБ, диск 100 ГБ, статический публичный IP `yandex_vpc_address.app`) + cloud-init. Память меняется на месте с остановкой VM (`allow_stopping_for_update`); размер диска на живой VM — `yc compute disk update <id> --size N` (в Terraform игнорируется).

## Разовый провижининг

С нуля (бакета состояния ещё нет): первый `apply` — с локальным state (как в `bootstrap.sh`:
`bootstrap_override.tf` с `backend "local" {}`), затем `./bootstrap.sh` переносит его в бакет.
Ниже — исторический порядок первого развёртывания.

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
docker build -t "$(terraform output -raw image_planner_ref)" -f deploy/Dockerfile.planner .
docker build -t "$(terraform output -raw image_collector_ref)" -f deploy/Dockerfile.collector .
docker build -t "$(terraform output -raw image_crawler_ref)" -f deploy/Dockerfile.crawler .
docker build -t "$(terraform output -raw image_web_ref)" -f deploy/Dockerfile.web .
docker push "$(terraform output -raw image_planner_ref)"
docker push "$(terraform output -raw image_collector_ref)"
docker push "$(terraform output -raw image_crawler_ref)"
docker push "$(terraform output -raw image_web_ref)"

# 3) DNS: A-запись site_domain -> vm_external_ip (Cloudflare, proxy OFF на время выпуска TLS).
terraform output vm_external_ip
```

После DNS Caddy автоматически берёт Let's Encrypt-сертификат (HTTP-01). Проверка:
`curl -fsS https://flights.sereshotes.dev/api/health`.

## Состояние Terraform и локальный запуск

State лежит не на ноутбуке, а в бакете `sereshotes-flights-tfstate` (`backend.tf`). В нём
секреты (ключи SA, S3-ключ приложения, токен в user-data VM — Terraform хранит все
атрибуты ресурсов, `sensitive` прячет их только из вывода), поэтому бакет приватный и
шифруется KMS. Предыдущие версии state — в версиях объекта (`yc storage s3api
list-object-versions --bucket sereshotes-flights-tfstate`). Одновременный `apply`
(ваш и из CI) не испортит state: второй ждёт lock-файл (`use_lockfile`, Object Storage
поддерживает условную запись — проверено 02.10.2026).

Локально — через обёртку: она берёт IAM-токен вашего `yc`, а ключ бакета и токен
Travelpayouts — из Lockbox `flights-terraform`:

```sh
cd infra/terraform
./tf.sh plan
./tf.sh apply
```

## GitHub Actions без ключей

В GitHub нет ни одного долгоживущего секрета Yandex Cloud. Job берёт у GitHub OIDC-токен
(«workflow репо `SeresHotes/flight-scanner` в environment `prod`») и меняет его в
`auth.yandex.cloud` на IAM-токен SA (≤ 12 ч) — `.github/actions/yc-oidc`. Федерация
`flights-github` выдаёт токен только subject `repo:SeresHotes/flight-scanner:environment:prod`,
а в environment `prod` GitHub пускает только ветку `main`: workflow с другой ветки
(изменённый в PR, запущенный с `--ref`) токена не получит. Секреты для CI — в Lockbox,
читают их те же SA по IAM-токену (`.github/scripts/lockbox.sh`):

| Lockbox | ключи | кто читает |
|---|---|---|
| `flights-terraform` | `tfstate_access_key`, `tfstate_secret_key`, `travelpayouts_token` | `flights-terraform` |
| `flights-ops` | `vm_ssh_key` (ключ деплоя, пара — `ci_ssh_public_key`) | `flights-ci` |

В environment `prod` — только переменные (ID, не секреты): `YC_CLOUD_ID`, `YC_FOLDER_ID`,
`YC_REGISTRY_ID`, `YC_CI_SA_ID`, `YC_TERRAFORM_SA_ID`, `LOCKBOX_TERRAFORM_ID`,
`LOCKBOX_OPS_ID`, `VM_HOST`, `VM_SSH_KNOWN_HOSTS`, `TF_BUCKET_NAME`, `TF_SSH_PUBLIC_KEY`,
`TF_CI_SSH_PUBLIC_KEY`.

Workflow:

- `deploy.yml` — push в `main` → сборка и push образов (`flights-ci`) → `systemctl start
  flights-update` на VM по SSH (иначе таймер, ~2 мин).
- `terraform.yml` — push в `main` с правками `infra/terraform/**` → `plan`;
  `gh workflow run terraform.yml -f command=apply` → plan + apply смёрженного кода.
- `ops.yml` — команда на VM по SSH: `gh workflow run ops.yml -f command='…'`, вывод —
  `gh run view <id> --log`. Для облачных сессий Claude: у них нет SSH (только HTTP через
  прокси) и ключей, а `gh` работает. **Репозиторий публичный — логи Actions видны всем**:
  не печатать `.env`, `docker inspect`, переменные контейнеров и персональные данные.

### Разовый переезд (02.10.2026)

Из основного checkout, где лежит локальный `terraform.tfstate`, после мержа этих файлов в main:

```sh
cd infra/terraform && git pull && ./bootstrap.sh
```

Скрипт: `terraform apply` при локальном state (бакет состояния, SA, федерация, Lockbox —
спросит подтверждение) → секреты в Lockbox (статический ключ `flights-tfstate`, токен из
`terraform.tfvars`, новый SSH-ключ деплоя `~/.ssh/flights_ci_deploy_ed25519`) → ключ деплоя
в `authorized_keys` VM → `terraform init -migrate-state`, локальный файл →
`terraform.tfstate.pre-s3` → environment `prod` (только `main`) и переменные. Повторный
запуск безопасен. Проверка: `gh workflow run terraform.yml -f command=plan` → «No changes».

После первого зелёного `deploy.yml` через OIDC — убрать старый ключ: удалить ресурс
`yandex_iam_service_account_key.ci` и выход `ci_sa_key_json`, `apply` (ключ отзывается), удалить
секреты репозитория `YC_SA_KEY_JSON`, `YC_FOLDER_ID`, `YC_REGISTRY_ID` и legacy-шаг в
`deploy.yml`. Локальные `terraform.tfstate.pre-s3*` (в них секреты) — удалить.

## Эксплуатация

- SSH: `ssh ubuntu@<vm_external_ip>`; без SSH (облачная сессия) — `ops.yml` выше.
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
