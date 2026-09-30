//! Строки склада → Arrow IPC-поток в схеме озера (`collector/lake.SCHEMA`) — ровно тот
//! формат, что коллектор отдаёт планировщику (`/v1/requests/{id}/result?format=arrow`),
//! поэтому планировщик читает ответ склада тем же `tickets_from_ipc`.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::lake::Row;

pub const ARROW_MEDIA_TYPE: &str = "application/vnd.apache.arrow.stream";

pub fn lake_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("series_id", DataType::Int64, true),
        Field::new("observed_at", DataType::Utf8, true),
        Field::new("search_origin", DataType::Utf8, true),
        Field::new("search_destination", DataType::Utf8, true),
        Field::new("search_date", DataType::Utf8, true),
        Field::new("origin", DataType::Utf8, true),
        Field::new("destination", DataType::Utf8, true),
        Field::new("origin_airport", DataType::Utf8, true),
        Field::new("destination_airport", DataType::Utf8, true),
        Field::new("departure_at", DataType::Utf8, true),
        Field::new("arrival_at", DataType::Utf8, true),
        Field::new("duration", DataType::Int32, true),
        Field::new("duration_to", DataType::Int32, true),
        Field::new("transfers", DataType::Int16, true),
        Field::new("airline", DataType::Utf8, true),
        Field::new("flight_number", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
        Field::new("currency", DataType::Utf8, true),
        Field::new("link", DataType::Utf8, true),
        Field::new("chain_json", DataType::Utf8, true),
        Field::new("legs_json", DataType::Utf8, true),
        Field::new("transfer_points_json", DataType::Utf8, true),
        Field::new("baggage_code", DataType::Utf8, true),
        Field::new("baggage_known", DataType::Boolean, true),
        Field::new("baggage_included", DataType::Boolean, true),
        Field::new("baggage_pieces", DataType::Int16, true),
        Field::new("baggage_kg", DataType::Int16, true),
        Field::new("source", DataType::Utf8, true),
    ]))
}

pub fn batch_from_rows(rows: &[Row]) -> RecordBatch {
    let s = |f: &dyn Fn(&Row) -> Option<&str>| -> ArrayRef { Arc::new(StringArray::from(rows.iter().map(f).collect::<Vec<_>>())) };
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(rows.iter().map(|r| r.series_id).collect::<Vec<_>>())),
        s(&|r| r.observed_at.as_deref()),
        s(&|r| r.search_origin.as_deref()),
        s(&|r| r.search_destination.as_deref()),
        s(&|r| r.search_date.as_deref()),
        s(&|r| r.origin.as_deref()),
        s(&|r| r.destination.as_deref()),
        s(&|r| r.origin_airport.as_deref()),
        s(&|r| r.destination_airport.as_deref()),
        s(&|r| r.departure_at.as_deref()),
        s(&|r| r.arrival_at.as_deref()),
        Arc::new(Int32Array::from(rows.iter().map(|r| r.duration).collect::<Vec<_>>())),
        Arc::new(Int32Array::from(rows.iter().map(|r| r.duration_to).collect::<Vec<_>>())),
        Arc::new(Int16Array::from(rows.iter().map(|r| r.transfers).collect::<Vec<_>>())),
        s(&|r| r.airline.as_deref()),
        s(&|r| r.flight_number.as_deref()),
        Arc::new(Float64Array::from(rows.iter().map(|r| r.price).collect::<Vec<_>>())),
        s(&|r| r.currency.as_deref()),
        s(&|r| r.link.as_deref()),
        s(&|r| r.chain_json.as_deref()),
        s(&|r| r.legs_json.as_deref()),
        s(&|r| r.transfer_points_json.as_deref()),
        s(&|r| r.baggage_code.as_deref()),
        Arc::new(BooleanArray::from(rows.iter().map(|r| r.baggage_known).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(rows.iter().map(|r| r.baggage_included).collect::<Vec<_>>())),
        Arc::new(Int16Array::from(rows.iter().map(|r| r.baggage_pieces).collect::<Vec<_>>())),
        Arc::new(Int16Array::from(rows.iter().map(|r| r.baggage_kg).collect::<Vec<_>>())),
        s(&|r| r.source.as_deref()),
    ];
    RecordBatch::try_new(lake_schema(), cols).expect("lake batch")
}

/// IPC-поток (одна батча; пустой список — поток только со схемой).
pub fn ipc_from_rows(rows: &[Row]) -> Vec<u8> {
    let batch = batch_from_rows(rows);
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &batch.schema()).expect("ipc writer");
        w.write(&batch).expect("ipc write");
        w.finish().expect("ipc finish");
    }
    buf
}

/// Parquet-файл в схеме озера из строк (для тестов и локальных прогонов).
pub fn parquet_from_rows(rows: &[Row]) -> Vec<u8> {
    let batch = batch_from_rows(rows);
    let mut buf = Vec::new();
    let mut w = parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None).expect("parquet writer");
    w.write(&batch).expect("parquet write");
    w.close().expect("parquet close");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lake::{read_parquet, rows_from_batch};
    use arrow::ipc::reader::StreamReader;

    #[test]
    fn roundtrip_ipc_and_parquet() {
        let r = Row { search_origin: Some("MOW".into()), search_date: Some("2026-10-01".into()), origin: Some("MOW".into()), destination: Some("IST".into()), price: Some(123.0), transfers: Some(1), baggage_known: Some(true), legs_json: Some("[]".into()), ..Default::default() };
        let rows = vec![r.clone(), Row::default()];
        let ipc = ipc_from_rows(&rows);
        let reader = StreamReader::try_new(std::io::Cursor::new(ipc), None).unwrap();
        let back: Vec<Row> = reader.map(|b| rows_from_batch(&b.unwrap())).flatten().collect();
        assert_eq!(back, rows);
        let pq = parquet_from_rows(&rows);
        assert_eq!(read_parquet(bytes::Bytes::from(pq)).unwrap(), rows);
        assert!(!ipc_from_rows(&[]).is_empty());
    }
}
