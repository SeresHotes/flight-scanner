//! Рейсы джобы колонками (FlightCols): все плечи одной таблицей, поля стыковки уже
//! числами. Из неё строят свои структуры перебор A* (search) и наборы городов
//! (overview). Полный рейс лежит JSON-колонкой `seg` (только поля сегмента) и
//! разбирается лениво — для рейсов из результата.
//!
//! Хранение — Parquet-файл на джобу (`plan_flights/<job>.parquet` рядом с БД) в той же
//! схеме, что писал Python-планировщик (файлы прежних джоб читаются как есть).

use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::dates::{naive_seconds, ordinal, parse_naive};
use crate::ticket::Ticket;

pub const STR_COLS: [&str; 6] = ["orig_city", "orig_airport", "dest", "dest_airport", "dep_iso", "arr_iso"];
pub const NUM_COLS: [&str; 12] =
    ["leg", "price", "transfers", "duration", "hidden", "bag_incl", "pts_min", "layover", "dep_ts", "arr_ts", "dep_ord", "arr_ord"];

pub struct FlightCols {
    pub n: usize,
    pub leg: Vec<i16>,
    pub orig_city: Vec<String>,
    pub orig_airport: Vec<String>,
    pub dest: Vec<String>,
    pub dest_airport: Vec<String>,
    pub dep_iso: Vec<Option<String>>,
    pub arr_iso: Vec<Option<String>>,
    pub price: Vec<f64>,
    pub transfers: Vec<i64>,
    pub duration: Vec<i64>,
    pub hidden: Vec<bool>,
    pub bag_incl: Vec<bool>,
    pub pts_min: Vec<Option<i64>>,
    pub layover: Vec<Option<i64>>,
    pub dep_ts: Vec<f64>,
    pub arr_ts: Vec<f64>,
    pub dep_ord: Vec<i64>,
    pub arr_ord: Vec<i64>,
    /// Рейсы целиком (из сбора) или JSON `seg` (из файла), разбираемый лениво.
    raw: Mutex<Raw>,
    parsed: Mutex<HashMap<usize, Arc<Ticket>>>,
}

enum Raw {
    Tickets(Vec<Arc<Ticket>>),
    Json(Vec<Option<String>>),
    Lazy(String), // путь Parquet: колонка seg дочитывается при первом flight(i)
}

fn upper(v: &Option<String>) -> String {
    v.as_deref().map(|s| s.to_uppercase()).unwrap_or_default()
}

impl FlightCols {
    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Из собранных рейсов по плечам.
    pub fn from_collected(collected: &[Vec<Arc<Ticket>>]) -> FlightCols {
        let n: usize = collected.iter().map(|v| v.len()).sum();
        let mut c = FlightCols::with_capacity(n);
        let mut raw = Vec::with_capacity(n);
        for (leg, flights) in collected.iter().enumerate() {
            for f in flights {
                c.push(leg as i16, f);
                raw.push(f.clone());
            }
        }
        c.n = c.leg.len();
        c.raw = Mutex::new(Raw::Tickets(raw));
        c
    }

    fn with_capacity(n: usize) -> FlightCols {
        FlightCols {
            n: 0,
            leg: Vec::with_capacity(n),
            orig_city: Vec::with_capacity(n),
            orig_airport: Vec::with_capacity(n),
            dest: Vec::with_capacity(n),
            dest_airport: Vec::with_capacity(n),
            dep_iso: Vec::with_capacity(n),
            arr_iso: Vec::with_capacity(n),
            price: Vec::with_capacity(n),
            transfers: Vec::with_capacity(n),
            duration: Vec::with_capacity(n),
            hidden: Vec::with_capacity(n),
            bag_incl: Vec::with_capacity(n),
            pts_min: Vec::with_capacity(n),
            layover: Vec::with_capacity(n),
            dep_ts: Vec::with_capacity(n),
            arr_ts: Vec::with_capacity(n),
            dep_ord: Vec::with_capacity(n),
            arr_ord: Vec::with_capacity(n),
            raw: Mutex::new(Raw::Tickets(Vec::new())),
            parsed: Mutex::new(HashMap::new()),
        }
    }

    fn push(&mut self, leg: i16, f: &Ticket) {
        let dep = f.departure_at.clone().filter(|s| !s.is_empty());
        let arr = if dep.is_some() { f.arrival() } else { None };
        let dep_dt = dep.as_deref().and_then(parse_naive);
        let arr_dt = arr.as_deref().and_then(parse_naive);
        self.leg.push(leg);
        self.orig_city.push(f.origin_city().map(|s| s.to_uppercase()).unwrap_or_default());
        self.orig_airport.push(upper(&f.origin_airport));
        self.dest.push(f.dest_city().map(|s| s.to_uppercase()).unwrap_or_default());
        self.dest_airport.push(upper(&f.destination_airport));
        self.dep_iso.push(dep);
        self.arr_iso.push(arr);
        self.price.push(f.price());
        self.transfers.push(f.transfers);
        self.duration.push(f.duration.unwrap_or(0));
        self.hidden.push(f.hidden_city.is_some());
        self.bag_incl.push(f.baggage.as_ref().map(|b| b.included).unwrap_or(false));
        self.pts_min.push(f.min_transfer_minutes());
        self.layover.push(f.layover_minutes);
        self.dep_ts.push(dep_dt.map(naive_seconds).unwrap_or(f64::NAN));
        self.arr_ts.push(arr_dt.map(naive_seconds).unwrap_or(f64::NAN));
        self.dep_ord.push(dep_dt.map(|d| ordinal(d.date())).unwrap_or(-1));
        self.arr_ord.push(arr_dt.map(|d| ordinal(d.date())).unwrap_or(-1));
    }

    /// Полный рейс строки i — для сегментов результата.
    pub fn flight(&self, i: usize) -> Arc<Ticket> {
        {
            let raw = self.raw.lock().unwrap();
            if let Raw::Tickets(v) = &*raw {
                return v[i].clone();
            }
        }
        if let Some(got) = self.parsed.lock().unwrap().get(&i) {
            return got.clone();
        }
        let json = self.seg_json(i);
        let ticket: Ticket = json.as_deref().and_then(|s| serde_json::from_str(s).ok()).unwrap_or_default();
        let ticket = Arc::new(ticket);
        self.parsed.lock().unwrap().insert(i, ticket.clone());
        ticket
    }

    fn seg_json(&self, i: usize) -> Option<String> {
        let mut raw = self.raw.lock().unwrap();
        if let Raw::Lazy(path) = &*raw {
            let col = read_seg_column(path).unwrap_or_else(|e| {
                eprintln!("[flightcols] {path}: колонка seg не прочитана: {e}");
                Vec::new()
            });
            *raw = Raw::Json(col);
        }
        match &*raw {
            Raw::Json(v) => v.get(i).cloned().flatten(),
            _ => None,
        }
    }

    pub fn rows(&self, leg: usize) -> Vec<usize> {
        (0..self.n).filter(|&r| self.leg[r] as usize == leg).collect()
    }

    pub fn origin_codes(&self, r: usize) -> (&str, &str) {
        (&self.orig_city[r], &self.orig_airport[r])
    }

    /// Город или аэропорт прилёта строки r в allow (None — любой).
    pub fn dest_in(&self, r: usize, allow: Option<&std::collections::HashSet<String>>) -> bool {
        match allow {
            None => true,
            Some(a) => a.contains(&self.dest[r]) || a.contains(&self.dest_airport[r]),
        }
    }

    /// Строки плеча по коду вылета — и городу, и аэропорту.
    pub fn by_origin(&self, rows: &[usize]) -> HashMap<String, Vec<usize>> {
        let mut out: HashMap<String, Vec<usize>> = HashMap::new();
        for &r in rows {
            let (c, a) = (&self.orig_city[r], &self.orig_airport[r]);
            if !c.is_empty() {
                out.entry(c.clone()).or_default().push(r);
            }
            if !a.is_empty() && a != c {
                out.entry(a.clone()).or_default().push(r);
            }
        }
        out
    }

    // ------------------------------ хранение ---------------------------------

    fn schema() -> Arc<Schema> {
        let mut fields: Vec<Field> = STR_COLS.iter().map(|c| Field::new(*c, DataType::Utf8, true)).collect();
        fields.extend([
            Field::new("leg", DataType::Int16, true),
            Field::new("price", DataType::Float64, true),
            Field::new("transfers", DataType::Int32, true),
            Field::new("duration", DataType::Int64, true),
            Field::new("hidden", DataType::Boolean, true),
            Field::new("bag_incl", DataType::Boolean, true),
            Field::new("pts_min", DataType::Float64, true),
            Field::new("layover", DataType::Float64, true),
            Field::new("dep_ts", DataType::Float64, true),
            Field::new("arr_ts", DataType::Float64, true),
            Field::new("dep_ord", DataType::Int64, true),
            Field::new("arr_ord", DataType::Int64, true),
            Field::new("seg", DataType::Utf8, true),
        ]);
        Arc::new(Schema::new(fields))
    }

    fn to_batch(&self) -> Result<RecordBatch, String> {
        let seg: Vec<Option<String>> = (0..self.n)
            .map(|i| serde_json::to_string(&self.flight(i).segment_source()).ok())
            .collect();
        let opt = |v: &Vec<String>| -> ArrayRef { Arc::new(StringArray::from(v.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>())) };
        let cols: Vec<ArrayRef> = vec![
            opt(&self.orig_city),
            opt(&self.orig_airport),
            opt(&self.dest),
            opt(&self.dest_airport),
            Arc::new(StringArray::from(self.dep_iso.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
            Arc::new(StringArray::from(self.arr_iso.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
            Arc::new(Int16Array::from(self.leg.clone())),
            Arc::new(Float64Array::from(self.price.clone())),
            Arc::new(Int32Array::from(self.transfers.iter().map(|t| *t as i32).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(self.duration.clone())),
            Arc::new(BooleanArray::from(self.hidden.clone())),
            Arc::new(BooleanArray::from(self.bag_incl.clone())),
            Arc::new(Float64Array::from(self.pts_min.iter().map(|v| v.map(|x| x as f64).unwrap_or(f64::NAN)).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(self.layover.iter().map(|v| v.map(|x| x as f64).unwrap_or(f64::NAN)).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(self.dep_ts.clone())),
            Arc::new(Float64Array::from(self.arr_ts.clone())),
            Arc::new(Int64Array::from(self.dep_ord.clone())),
            Arc::new(Int64Array::from(self.arr_ord.clone())),
            Arc::new(StringArray::from(seg.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
        ];
        RecordBatch::try_new(Self::schema(), cols).map_err(|e| e.to_string())
    }

    pub fn write(&self, path: &str) -> Result<(), String> {
        let batch = self.to_batch()?;
        let file = File::create(path).map_err(|e| e.to_string())?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_column_compression("seg".into(), Compression::LZ4_RAW)
            .build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
        writer.close().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Числовые и строковые колонки из Parquet; `seg` — лениво при первом flight(i).
    pub fn read(path: &str) -> Result<FlightCols, String> {
        let file = File::open(path).map_err(|e| e.to_string())?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;
        let schema = builder.schema().clone();
        let wanted: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| STR_COLS.contains(&f.name().as_str()) || NUM_COLS.contains(&f.name().as_str()))
            .map(|(i, _)| i)
            .collect();
        let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), wanted);
        let reader = builder.with_projection(mask).with_batch_size(1 << 16).build().map_err(|e| e.to_string())?;
        let mut batches = Vec::new();
        for b in reader {
            batches.push(b.map_err(|e| e.to_string())?);
        }
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        let mut c = FlightCols::with_capacity(n);
        for b in &batches {
            let s = |name: &str| -> Result<Vec<Option<String>>, String> { str_col(b, name) };
            let f = |name: &str| -> Result<Vec<Option<f64>>, String> { f64_col(b, name) };
            let i = |name: &str| -> Result<Vec<Option<i64>>, String> { i64_col(b, name) };
            let bl = |name: &str| -> Result<Vec<bool>, String> { bool_col(b, name) };
            c.orig_city.extend(s("orig_city")?.into_iter().map(|v| v.unwrap_or_default()));
            c.orig_airport.extend(s("orig_airport")?.into_iter().map(|v| v.unwrap_or_default()));
            c.dest.extend(s("dest")?.into_iter().map(|v| v.unwrap_or_default()));
            c.dest_airport.extend(s("dest_airport")?.into_iter().map(|v| v.unwrap_or_default()));
            c.dep_iso.extend(s("dep_iso")?);
            c.arr_iso.extend(s("arr_iso")?);
            c.leg.extend(i("leg")?.into_iter().map(|v| v.unwrap_or(0) as i16));
            c.price.extend(f("price")?.into_iter().map(|v| v.unwrap_or(0.0)));
            c.transfers.extend(i("transfers")?.into_iter().map(|v| v.unwrap_or(0)));
            c.duration.extend(i("duration")?.into_iter().map(|v| v.unwrap_or(0)));
            c.hidden.extend(bl("hidden")?);
            c.bag_incl.extend(bl("bag_incl")?);
            c.pts_min.extend(f("pts_min")?.into_iter().map(|v| v.filter(|x| !x.is_nan()).map(|x| x as i64)));
            c.layover.extend(f("layover")?.into_iter().map(|v| v.filter(|x| !x.is_nan()).map(|x| x as i64)));
            c.dep_ts.extend(f("dep_ts")?.into_iter().map(|v| v.unwrap_or(f64::NAN)));
            c.arr_ts.extend(f("arr_ts")?.into_iter().map(|v| v.unwrap_or(f64::NAN)));
            c.dep_ord.extend(i("dep_ord")?.into_iter().map(|v| v.unwrap_or(-1)));
            c.arr_ord.extend(i("arr_ord")?.into_iter().map(|v| v.unwrap_or(-1)));
        }
        c.n = n;
        c.raw = Mutex::new(Raw::Lazy(path.to_string()));
        Ok(c)
    }
}

fn read_seg_column(path: &str) -> Result<Vec<Option<String>>, String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;
    let schema = builder.schema().clone();
    let name = if schema.fields().iter().any(|f| f.name() == "seg") { "seg" } else { "raw" };
    let idx = schema.fields().iter().position(|f| f.name() == name).ok_or("нет колонки seg/raw")?;
    let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), [idx]);
    let reader = builder.with_projection(mask).with_batch_size(1 << 16).build().map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for b in reader {
        let b = b.map_err(|e| e.to_string())?;
        out.extend(str_col(&b, name)?);
    }
    Ok(out)
}

fn column<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, String> {
    b.column_by_name(name).ok_or_else(|| format!("нет колонки {name}"))
}

pub fn str_col(b: &RecordBatch, name: &str) -> Result<Vec<Option<String>>, String> {
    let Some(col) = b.column_by_name(name) else { return Ok(vec![None; b.num_rows()]) };
    let arr = cast(col, &DataType::Utf8).map_err(|e| e.to_string())?;
    let arr = arr.as_any().downcast_ref::<StringArray>().ok_or("не строка")?;
    Ok((0..arr.len()).map(|i| if arr.is_null(i) { None } else { Some(arr.value(i).to_string()) }).collect())
}

pub fn f64_col(b: &RecordBatch, name: &str) -> Result<Vec<Option<f64>>, String> {
    let Some(col) = b.column_by_name(name) else { return Ok(vec![None; b.num_rows()]) };
    let arr = cast(col, &DataType::Float64).map_err(|e| e.to_string())?;
    let arr = arr.as_any().downcast_ref::<Float64Array>().ok_or("не число")?;
    Ok((0..arr.len()).map(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }).collect())
}

pub fn i64_col(b: &RecordBatch, name: &str) -> Result<Vec<Option<i64>>, String> {
    let Some(col) = b.column_by_name(name) else { return Ok(vec![None; b.num_rows()]) };
    let arr = cast(col, &DataType::Int64).map_err(|e| e.to_string())?;
    let arr = arr.as_any().downcast_ref::<Int64Array>().ok_or("не целое")?;
    Ok((0..arr.len()).map(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }).collect())
}

pub fn bool_col(b: &RecordBatch, name: &str) -> Result<Vec<bool>, String> {
    let col = column(b, name)?;
    let arr = cast(col, &DataType::Boolean).map_err(|e| e.to_string())?;
    let arr = arr.as_any().downcast_ref::<BooleanArray>().ok_or("не bool")?;
    Ok((0..arr.len()).map(|i| !arr.is_null(i) && arr.value(i)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(o: &str, d: &str, dep: &str, price: f64) -> Arc<Ticket> {
        Arc::new(Ticket {
            origin: Some(o.into()),
            destination: Some(d.into()),
            origin_airport: Some(o.into()),
            destination_airport: Some(d.into()),
            departure_at: Some(dep.into()),
            duration: Some(300),
            price: Some(price),
            transfers: 1,
            transfer_points: Some(vec![crate::ticket::TransferPoint { code: Some("X".into()), minutes: Some(80), ..Default::default() }]),
            ..Default::default()
        })
    }

    #[test]
    fn builds_and_roundtrips_parquet() {
        let collected = vec![vec![t("MOW", "IST", "2026-11-01T10:00:00+03:00", 100.0)], vec![t("IST", "SEL", "2026-11-03T08:00:00+03:00", 200.0)]];
        let cols = FlightCols::from_collected(&collected);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols.arr_iso[0].as_deref(), Some("2026-11-01T15:00:00"));
        assert_eq!(cols.pts_min[0], Some(80));
        assert_eq!(cols.rows(1), vec![1]);
        assert_eq!(cols.by_origin(&[0, 1]).get("MOW"), Some(&vec![0]));
        let dir = std::env::temp_dir().join(format!("fc-{}.parquet", std::process::id()));
        let path = dir.to_string_lossy().to_string();
        cols.write(&path).unwrap();
        let back = FlightCols::read(&path).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back.dep_ord, cols.dep_ord);
        assert_eq!(back.dest[1], "SEL");
        assert_eq!(back.pts_min[0], Some(80));
        assert_eq!(back.flight(1).price(), 200.0);
        assert_eq!(back.flight(1).transfer_points.as_ref().unwrap()[0].minutes, Some(80));
        std::fs::remove_file(&path).ok();
    }
}
