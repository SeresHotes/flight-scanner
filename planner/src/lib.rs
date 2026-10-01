//! Планировщик Flight Scanner на Rust: HTTP `/api/*` (axum), сбор серий у коллектора,
//! стыковка цепочек (A*), наборы городов, SQLite-джобы и Parquet-рейсы джобы.
//! Контракт и семантика — docs/PLANNER_V2.md (прежняя реализация — Python `api/` + `core/`).

/// Аллокатор: сбор и разбор билетов — миллионы мелких строк, mimalloc заметно быстрее glibc.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod airports;
pub mod api;
pub mod collect;
pub mod collector;
pub mod dates;
pub mod dynamics;
pub mod flightcols;
pub mod graphql;
pub mod hot;
pub mod lakestore;
pub mod lakesync;
pub mod nearby;
pub mod overview;
pub mod planquery;
pub mod search;
pub mod segments;
pub mod stops;
pub mod ticket;
pub mod tickets;
pub mod worker;
