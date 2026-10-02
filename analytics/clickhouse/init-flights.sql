-- База `flights` в ClickHouse аналитической VM: вьюхи поверх Parquet в бакете
-- flights через named collections (config.d/named_collections_flights.xml, без
-- секретов в SQL). Применяет analytics/setup.sh через clickhouse-client; идемпотентно.
--
-- Структура задана явно (схемы — collector/lake.py и collector/metrics.py): вьюхи
-- создаются и до появления файлов, а запросы не тратят время на вывод схемы по
-- сотням файлов. При смене схемы Parquet править здесь.
CREATE DATABASE IF NOT EXISTS flights;

-- Серии билетов: tickets/fetched=<день загрузки>/origin=<город>/<A>-<B>__<дни вылета>[__params]__<время>.parquet
-- (маска tickets/*/origin=* читает и прежнюю раскладку date=<день вылета>).
-- (collector/lake.py). Каждый файл — одна выборка серии, история цен = несколько файлов.
CREATE OR REPLACE VIEW flights.tickets AS
    SELECT * FROM s3(flights_tickets, structure='
        series_id Int64, observed_at String,
        search_origin Nullable(String), search_destination Nullable(String), search_date Nullable(String),
        origin Nullable(String), destination Nullable(String),
        origin_airport Nullable(String), destination_airport Nullable(String),
        departure_at Nullable(String), arrival_at Nullable(String),
        duration Nullable(Int32), duration_to Nullable(Int32), transfers Nullable(Int16),
        airline Nullable(String), flight_number Nullable(String), price Nullable(Float64),
        currency Nullable(String), link Nullable(String),
        chain_json Nullable(String), legs_json Nullable(String), transfer_points_json Nullable(String),
        baggage_code Nullable(String), baggage_known Bool, baggage_included Bool,
        baggage_pieces Nullable(Int16), baggage_kg Nullable(Int16), source Nullable(String)');

-- Ops-метрики коллектора, строка в минуту (collector/metrics.py): файл на час,
-- прошедшие дни склеены в day.parquet. Колонки, добавленные позже (с 02.10.2026), в
-- старых файлах отсутствуют — читаются как NULL.
CREATE OR REPLACE VIEW flights.ops_metrics AS
    SELECT * FROM s3(flights_ops_metrics, structure='
        ts DateTime(''UTC''),
        cpu_pct Nullable(Float64), mem_pct Nullable(Float64), disk_used_pct Nullable(Float64), load1 Nullable(Float64),
        lake_bytes Nullable(Int64), lake_files Nullable(Int64), lake_max_bytes Nullable(Int64),
        bucket_bytes Nullable(Int64), bucket_objects Nullable(Int64),
        queued_app Nullable(Int64), queued_crawl Nullable(Int64), running Nullable(Int8), rate_per_minute Nullable(Float64),
        pages_app_1m Nullable(Int64), pages_crawl_1m Nullable(Int64), series_app_1m Nullable(Int64),
        series_crawl_1m Nullable(Int64), failed_app_1m Nullable(Int64), failed_crawl_1m Nullable(Int64),
        cache_hit_app_1m Nullable(Int64), cache_hit_crawl_1m Nullable(Int64),
        submitted_app_1m Nullable(Int64), submitted_crawl_1m Nullable(Int64),
        http_429_1m Nullable(Int64), source_errors_1m Nullable(Int64), network_errors_1m Nullable(Int64),
        lake_write_errors_1m Nullable(Int64), tickets_1m Nullable(Int64), files_deleted_1m Nullable(Int64),
        app_latency_avg_s Nullable(Float64),
        series_total Nullable(Int64), series_age_p50_h Nullable(Float64), series_age_max_h Nullable(Float64),
        cities Nullable(Int64),
        crawler_pairs Nullable(Int64), crawler_fresh Nullable(Int64), crawler_stale Nullable(Int64),
        crawler_missing Nullable(Int64), crawler_errors Nullable(Int64),
        crawler_pass_progress Nullable(Float64), crawler_submitted Nullable(Int64),
        crawler_quarantined_cities Nullable(Int64),
        series_age_p95_h Nullable(Float64), horizon_pairs Nullable(Int64),
        crawler_refresh Nullable(Int64), crawler_queued_pages_est Nullable(Int64)')
    SETTINGS input_format_parquet_allow_missing_columns = 1;

-- Снимок покрытия «город × день вылета» (перезаписывается раз в 10 мин).
CREATE OR REPLACE VIEW flights.coverage AS
    SELECT * FROM s3(flights_coverage, structure='
        snapshot_at DateTime(''UTC''), origin String, day Date, fetched_at DateTime(''UTC''),
        age_h Float64, pages Int32, tickets Int32, exhausted Bool, error Bool,
        error_msg Nullable(String)')
    SETTINGS input_format_parquet_allow_missing_columns = 1;
