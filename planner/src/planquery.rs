//! Единый запрос планировщика (PlanQuery): скелет + все фильтры одним объектом
//! (docs/PLANNER_V2.md, «PlanQuery»).
//!
//! ```text
//! stops[]:      {kind: cities|any, codes[], window[start, end], radiusKm?}
//! cities[]:     {minStay, maxStay, mustCover: [a, b] | null, requireWeekend}   # == stops
//! legs[]:       {maxTransfers, minLayoverMin, travelMin: [lo, hi],
//!                baggage: any|included|none, hiddenCity}                       # == stops - 1
//! tripLength:   [lo, hi]      maxResults: number
//! ```
//!
//! Все фильтры необязательны: отсутствующие = «без ограничений». Фильтры плеча
//! применяются к рейсам ДО построения, городов — внутри перебора, длина поездки —
//! при выдаче. `collect_key()` — хэш остановок: по нему джобы дедуплицируются;
//! `view_key()` — хэш фильтров: ключ результата стыковки внутри джобы.

use serde_json::{json, Map, Value};
use sha1::{Digest, Sha1};

use crate::nearby::clamp_radius;

pub const BAGGAGE_MODES: [&str; 3] = ["any", "included", "none"];

#[derive(Debug, Clone, PartialEq)]
pub struct StopSpec {
    pub kind: String,
    pub codes: Vec<String>,
    pub window: [String; 2],
    pub radius_km: i64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct CityFilter {
    pub min_stay: i64,
    pub max_stay: Option<i64>,
    pub must_cover: Option<[String; 2]>,
    pub require_weekend: bool,
}

impl CityFilter {
    pub fn from_value(d: Option<&Value>) -> Result<CityFilter, String> {
        let d = d.filter(|v| v.is_object()).cloned().unwrap_or(Value::Null);
        let cover = d.get("mustCover").and_then(|v| v.as_array()).and_then(|a| {
            if a.len() == 2 {
                let a0 = str_of(&a[0]);
                let a1 = str_of(&a[1]);
                if !a0.is_empty() && !a1.is_empty() {
                    return Some([a0, a1]);
                }
            }
            None
        });
        Ok(CityFilter {
            min_stay: int_of(d.get("minStay"))?.unwrap_or(0),
            max_stay: int_of(d.get("maxStay"))?,
            must_cover: cover,
            require_weekend: bool_of(d.get("requireWeekend")),
        })
    }

    pub fn is_open(&self) -> bool {
        self.min_stay <= 0 && self.max_stay.is_none() && self.must_cover.is_none() && !self.require_weekend
    }

    pub fn as_value(&self) -> Value {
        json!({
            "minStay": self.min_stay,
            "maxStay": self.max_stay,
            "mustCover": self.must_cover.as_ref().map(|c| json!([c[0], c[1]])),
            "requireWeekend": self.require_weekend,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LegFilter {
    pub max_transfers: i64,
    pub min_layover_min: i64,
    pub travel_min: (i64, Option<i64>),
    pub baggage: String,
    pub hidden_city: bool,
}

impl Default for LegFilter {
    fn default() -> Self {
        LegFilter { max_transfers: -1, min_layover_min: 0, travel_min: (0, None), baggage: "any".into(), hidden_city: true }
    }
}

impl LegFilter {
    pub fn from_value(d: Option<&Value>) -> Result<LegFilter, String> {
        let d = d.filter(|v| v.is_object()).cloned().unwrap_or(Value::Null);
        let travel = d.get("travelMin").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let baggage = d
            .get("baggage")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("any")
            .to_string();
        if !BAGGAGE_MODES.contains(&baggage.as_str()) {
            return Err(format!("baggage: {baggage:?}"));
        }
        let lo = travel.first().map(|v| int_of(Some(v))).transpose()?.flatten().unwrap_or(0);
        let hi = travel.get(1).map(|v| int_of(Some(v))).transpose()?.flatten();
        Ok(LegFilter {
            max_transfers: int_of(d.get("maxTransfers"))?.unwrap_or(-1),
            min_layover_min: int_of(d.get("minLayoverMin"))?.unwrap_or(0),
            travel_min: (lo, hi),
            baggage,
            hidden_city: d.get("hiddenCity").map(bool_of_v).unwrap_or(true),
        })
    }

    pub fn is_open(&self) -> bool {
        self.max_transfers < 0
            && self.min_layover_min <= 0
            && self.travel_min.0 <= 0
            && self.travel_min.1.is_none()
            && self.baggage == "any"
            && self.hidden_city
    }

    pub fn as_value(&self) -> Value {
        json!({
            "maxTransfers": self.max_transfers,
            "minLayoverMin": self.min_layover_min,
            "travelMin": [self.travel_min.0, self.travel_min.1],
            "baggage": self.baggage,
            "hiddenCity": self.hidden_city,
        })
    }

    /// Проходит ли рейс фильтр плеча (по уже разобранным полям колонок).
    pub fn accepts(&self, hidden: bool, transfers: i64, duration: i64, pts_min: Option<i64>, layover: Option<i64>, bag_incl: bool) -> bool {
        if !self.hidden_city && hidden {
            return false;
        }
        if self.max_transfers >= 0 && transfers > self.max_transfers {
            return false;
        }
        if duration < self.travel_min.0 {
            return false;
        }
        if let Some(hi) = self.travel_min.1 {
            if duration > hi {
                return false;
            }
        }
        if self.min_layover_min > 0 && transfers > 0 {
            if let Some(m) = pts_min {
                if m < self.min_layover_min {
                    return false;
                }
            } else if let (Some(l), 1) = (layover, transfers) {
                if l < self.min_layover_min {
                    return false;
                }
            }
        }
        if self.baggage == "included" && !bag_incl {
            return false;
        }
        if self.baggage == "none" && bag_incl {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanQuery {
    pub stops: Vec<StopSpec>,
    pub cities: Vec<CityFilter>,
    pub legs: Vec<LegFilter>,
    pub trip_length: (i64, Option<i64>),
    pub max_results: Option<i64>,
}

fn str_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn bool_of_v(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Null => false,
        Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn bool_of(v: Option<&Value>) -> bool {
    v.map(bool_of_v).unwrap_or(false)
}

/// Целое из JSON (`int(x)` в Python): null → None, число → усечение, строка → разбор.
fn int_of(v: Option<&Value>) -> Result<Option<i64>, String> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => Ok(Some(n.as_i64().or_else(|| n.as_f64().map(|f| f.trunc() as i64)).unwrap_or(0))),
        Some(Value::Bool(b)) => Ok(Some(*b as i64)),
        Some(Value::String(s)) => s.trim().parse::<i64>().map(Some).map_err(|_| format!("не число: {s:?}")),
        Some(other) => Err(format!("не число: {other}")),
    }
}

impl PlanQuery {
    pub fn from_value(d: &Value) -> Result<PlanQuery, String> {
        let mut stops = Vec::new();
        for s in d.get("stops").and_then(|v| v.as_array()).cloned().unwrap_or_default() {
            let kind = s.get("kind").and_then(|v| v.as_str()).unwrap_or("cities").to_string();
            let raw_codes: Vec<String> = match s.get("codes").and_then(|v| v.as_array()) {
                Some(a) => a.iter().map(str_of).collect(),
                None => s
                    .get("airports")
                    .and_then(|v| v.as_array())
                    .map(|a| a.iter().map(|x| x.get("code").map(str_of).unwrap_or_default()).collect())
                    .unwrap_or_default(),
            };
            let codes = raw_codes.into_iter().filter(|c| !c.is_empty()).map(|c| c.to_uppercase()).collect();
            let win = s.get("window").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            let window = [
                win.first().map(str_of).unwrap_or_default(),
                win.get(1).map(str_of).unwrap_or_default(),
            ];
            let radius_km = clamp_radius(s.get("radiusKm"));
            stops.push(StopSpec { kind, codes, window, radius_km });
        }
        let n = stops.len();
        let cities_raw = d.get("cities").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let legs_raw = d.get("legs").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let mut cities = Vec::with_capacity(n);
        for k in 0..n {
            cities.push(CityFilter::from_value(cities_raw.get(k))?);
        }
        let mut legs = Vec::new();
        for k in 0..n.saturating_sub(1) {
            legs.push(LegFilter::from_value(legs_raw.get(k))?);
        }
        let trip = d.get("tripLength").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let trip_lo = trip.first().map(|v| int_of(Some(v))).transpose()?.flatten().unwrap_or(0);
        let trip_hi = trip.get(1).map(|v| int_of(Some(v))).transpose()?.flatten();
        // maxCost (бюджет поездки) удалён 01.10.2026: в старых запросах/URL поле игнорируется.
        let max_results = int_of(d.get("maxResults").or(d.get("max_results")))?;
        Ok(PlanQuery { stops, cities, legs, trip_length: (trip_lo, trip_hi), max_results })
    }

    pub fn stops_value(&self) -> Value {
        Value::Array(
            self.stops
                .iter()
                .map(|s| {
                    let mut m = Map::new();
                    m.insert("kind".into(), json!(s.kind));
                    m.insert("codes".into(), json!(s.codes));
                    m.insert("window".into(), json!([s.window[0], s.window[1]]));
                    if s.radius_km > 0 {
                        m.insert("radiusKm".into(), json!(s.radius_km));
                    }
                    Value::Object(m)
                })
                .collect(),
        )
    }

    pub fn as_value(&self) -> Value {
        json!({
            "stops": self.stops_value(),
            "cities": self.cities.iter().map(|c| c.as_value()).collect::<Vec<_>>(),
            "legs": self.legs.iter().map(|l| l.as_value()).collect::<Vec<_>>(),
            "tripLength": [self.trip_length.0, self.trip_length.1],
            "maxResults": self.max_results,
        })
    }

    /// Хэш всего запроса (сбор + фильтры).
    pub fn key(&self) -> String {
        hash_value(&self.as_value())
    }

    /// Ключ джобы: только то, что решает, КАКИЕ серии «направление × день» нужны, —
    /// виды остановок, города, окна дат, радиус соседей.
    pub fn collect_key(&self) -> String {
        let items: Vec<Value> = self
            .stops
            .iter()
            .map(|s| {
                let mut codes = s.codes.clone();
                codes.sort();
                json!({"kind": s.kind, "codes": codes, "window": [s.window[0], s.window[1]], "radiusKm": s.radius_km})
            })
            .collect();
        hash_value(&Value::Array(items))
    }

    /// Всё, кроме остановок: фильтры и потолок цепочек.
    pub fn filters(&self) -> Value {
        let mut v = self.as_value();
        if let Some(m) = v.as_object_mut() {
            m.remove("stops");
        }
        v
    }

    pub fn view_key(&self) -> String {
        hash_value(&self.filters())
    }

    /// Остановки этого запроса (джобы) + фильтры из `filters`.
    pub fn with_filters(&self, filters: &Value) -> Result<PlanQuery, String> {
        let mut m = filters.as_object().cloned().unwrap_or_default();
        m.insert("stops".into(), self.stops_value());
        if !m.contains_key("maxResults") {
            m.insert("maxResults".into(), json!(self.max_results));
        }
        PlanQuery::from_value(&Value::Object(m))
    }

    /// Режим показа: наборы городов, если где-то «любой» или несколько городов.
    pub fn mode(&self) -> &'static str {
        if self.stops.iter().any(|s| s.kind == "any" || s.codes.len() > 1) {
            "combos"
        } else {
            "routes"
        }
    }

    pub fn city_filter(&self, i: usize) -> Option<&CityFilter> {
        self.cities.get(i).filter(|c| !c.is_open())
    }
}

/// sha1 канонического JSON (ключи по алфавиту, без пробелов)[:16] — как `planquery._hash`.
pub fn hash_value(v: &Value) -> String {
    let canon = serde_json::to_string(v).unwrap_or_default();
    let digest = Sha1::digest(canon.as_bytes());
    hex::encode(digest)[..16].to_string()
}

pub fn trip_length_ok(days: i64, trip: (i64, Option<i64>)) -> bool {
    days >= trip.0 && trip.1.map(|hi| days <= hi).unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_hashes_like_python() {
        let q = PlanQuery::from_value(&json!({
            "stops": [{"kind": "cities", "codes": ["mow"], "window": ["", ""]},
                      {"kind": "any", "codes": [], "window": ["2026-11-01", "2026-11-07"], "radiusKm": 0}],
            "cities": [{}, {"minStay": 2}],
            "legs": [{"baggage": "included", "travelMin": [0, null]}],
            "tripLength": [3, null],
            "maxCost": 50000,   // удалённое поле — игнорируется
            "maxResults": 5000
        }))
        .unwrap();
        assert_eq!(q.stops[0].codes, vec!["MOW"]);
        assert_eq!(q.cities[1].min_stay, 2);
        assert_eq!(q.legs[0].baggage, "included");
        assert_eq!(q.mode(), "combos");
        assert!(!q.as_value().as_object().unwrap().contains_key("maxCost"));
        // ключ сбора — те же данные и порядок ключей, что у Python (sha1 канонического JSON)
        let canon = serde_json::to_string(&json!([
            {"codes": ["MOW"], "kind": "cities", "radiusKm": 0, "window": ["", ""]},
            {"codes": [], "kind": "any", "radiusKm": 0, "window": ["2026-11-01", "2026-11-07"]}
        ]))
        .unwrap();
        assert_eq!(canon, r#"[{"codes":["MOW"],"kind":"cities","radiusKm":0,"window":["",""]},{"codes":[],"kind":"any","radiusKm":0,"window":["2026-11-01","2026-11-07"]}]"#);
        assert_eq!(q.collect_key(), hash_value(&serde_json::from_str::<Value>(&canon).unwrap()));
        assert_eq!(q.collect_key().len(), 16);
        // фильтры не меняют ключ сбора, но меняют ключ вида
        let q2 = q.with_filters(&json!({"tripLength": [5, 9]})).unwrap();
        assert_eq!(q2.collect_key(), q.collect_key());
        assert_ne!(q2.view_key(), q.view_key());
        assert_eq!(q2.max_results, Some(5000));
        assert!(PlanQuery::from_value(&json!({"stops": [{}, {}], "legs": [{"baggage": "x"}]})).is_err());
    }

    #[test]
    fn leg_filter_accepts() {
        let f = LegFilter { max_transfers: 1, min_layover_min: 60, ..Default::default() };
        assert!(f.accepts(false, 1, 300, Some(90), None, false));
        assert!(!f.accepts(false, 1, 300, Some(30), None, false));
        assert!(!f.accepts(false, 2, 300, Some(90), None, false));
        assert!(f.accepts(false, 1, 300, None, None, false)); // ожидание неизвестно — не режем
        assert!(!f.accepts(false, 1, 300, None, Some(30), false));
    }
}
