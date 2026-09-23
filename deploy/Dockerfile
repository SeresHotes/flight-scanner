# API-образ Flight Scanner: FastAPI + встроенный asyncio-воркер сбора.
# Раздача статики и TLS — на соседнем контейнере caddy (deploy/Dockerfile.web),
# сюда приходит уже только /api/* через reverse-proxy.
FROM python:3.12-slim

WORKDIR /app

# Рантайм-зависимости serving+сбора. Тяжёлый `datasets` (HuggingFace) НЕ ставим —
# он нужен только офлайн-скрипту build_airport_network.py, не в проде.
# requests — коллектор (Travelpayouts); pyarrow — Parquet-озеро; python-dotenv — .env.
RUN pip install --no-cache-dir \
      "fastapi>=0.115,<0.116" \
      "uvicorn[standard]>=0.34,<0.35" \
      "pyarrow>=22,<23" \
      "requests>=2.32,<3" \
      "python-dotenv>=1.2,<2"

# Только рантайм-код ядра/хранилища/API. Данные (data/) НЕ пекём в образ —
# они приезжают на персистентный том (сид из Object Storage на первом бутe).
COPY core ./core
COPY storage ./storage
COPY api ./api

# Всё горячее состояние — на смонтированном томе /app/data (SQLite + collected + lake).
# Пути совпадают с дефолтами модулей (cwd=/app → data/... == /app/data/...).
ENV FLIGHT_DB=/app/data/flights.db \
    COLLECTED_DIR=/app/data/collected \
    PYTHONUNBUFFERED=1

# Непривилегированный пользователь с фиксированным uid 1000 — том /app/data
# монтируется снаружи и chown-ится под этот uid в cloud-init (run.sh).
RUN useradd --create-home --uid 1000 app && mkdir -p /app/data && chown -R app /app
USER app

EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
