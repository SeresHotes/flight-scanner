-- База `flights` в ClickHouse аналитической VM: вьюхи поверх Parquet в бакете
-- flights через named collections (config.d/named_collections_flights.xml, без
-- секретов в SQL). Применяет analytics/setup.sh через clickhouse-client; идемпотентно.
CREATE DATABASE IF NOT EXISTS flights;

-- Серии билетов: tickets/date=<день вылета>/origin=<город>/<A>-<B>[__params]__<время>.parquet
-- (collector/lake.py). Каждый файл — одна выборка серии, история цен = несколько файлов.
CREATE OR REPLACE VIEW flights.tickets AS
    SELECT * FROM s3(flights_tickets);

-- Ops-метрики коллектора, строка в минуту (collector/metrics.py).
CREATE OR REPLACE VIEW flights.ops_metrics AS
    SELECT * FROM s3(flights_ops_metrics);

-- Снимок покрытия «город × день вылета» (перезаписывается раз в 10 мин).
CREATE OR REPLACE VIEW flights.coverage AS
    SELECT * FROM s3(flights_coverage);
