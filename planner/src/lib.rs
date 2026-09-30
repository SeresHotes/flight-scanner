//! Планировщик Flight Scanner на Rust: HTTP `/api/*` (axum), сбор серий у коллектора,
//! стыковка цепочек (A*), наборы городов, SQLite-джобы и Parquet-рейсы джобы.
//! Контракт и семантика — docs/PLANNER_V2.md (прежняя реализация — Python `api/` + `core/`).

pub mod airports;
pub mod api;
pub mod collect;
pub mod collector;
pub mod dates;
pub mod flightcols;
pub mod graphql;
pub mod hot;
pub mod nearby;
pub mod overview;
pub mod planquery;
pub mod search;
pub mod segments;
pub mod stops;
pub mod ticket;
pub mod worker;
