//! Переезд в соседний город внутри остановки планировщика.
//!
//! У остановки может быть радиус: прилетели в город a, а дальше летим из города d,
//! если d == a или между ними не больше радиуса (по прямой между центрами городов).
//! Хотя бы один из двух — город самой остановки. Координаты — core/geo.json
//! (scripts/build_geo.py): коды городов там те же, что в билетах GraphQL.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use serde::Deserialize;
use serde_json::Value;

use crate::stops::Stop;

/// Вылет из соседнего города — не раньше, чем через столько минут после прилёта.
pub const HOP_MIN_GAP_MIN: i64 = 240;
pub const MAX_RADIUS_KM: i64 = 500;

#[derive(Debug, Clone, Deserialize)]
pub struct GeoCity(pub f64, pub f64, pub String, pub String); // lat, lon, ISO2, имя

#[derive(Debug, Clone, Deserialize)]
pub struct GeoAirport(pub f64, pub f64, pub String); // lat, lon, код города

#[derive(Debug, Default, Deserialize)]
pub struct Geo {
    #[serde(default)]
    pub cities: HashMap<String, GeoCity>,
    #[serde(default)]
    pub airports: HashMap<String, GeoAirport>,
}

static GEO: OnceLock<Geo> = OnceLock::new();

pub fn geo_path() -> String {
    std::env::var("GEO_PATH").unwrap_or_else(|_| "core/geo.json".into())
}

pub fn geo() -> &'static Geo {
    GEO.get_or_init(|| match std::fs::read_to_string(geo_path()) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|e| {
            eprintln!("[nearby] geo.json не разбирается: {e}");
            Geo::default()
        }),
        Err(e) => {
            eprintln!("[nearby] geo.json не прочитан ({}): {e}", geo_path());
            Geo::default()
        }
    })
}

/// [lat, lon, ISO2, имя] города; для кода аэропорта — его города.
pub fn city_entry(code: &str) -> Option<&'static GeoCity> {
    let g = geo();
    let code = code.to_uppercase();
    g.cities.get(&code).or_else(|| g.airports.get(&code).and_then(|a| g.cities.get(&a.2)))
}

fn point(code: &str) -> Option<(f64, f64)> {
    let g = geo();
    let code = code.to_uppercase();
    if let Some(c) = g.cities.get(&code) {
        return Some((c.0, c.1));
    }
    g.airports.get(&code).map(|a| (a.0, a.1))
}

fn haversine(p: (f64, f64), q: (f64, f64)) -> f64 {
    let (lat1, lon1, lat2, lon2) = (p.0.to_radians(), p.1.to_radians(), q.0.to_radians(), q.1.to_radians());
    let h = ((lat2 - lat1) / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * ((lon2 - lon1) / 2.0).sin().powi(2);
    6371.0 * 2.0 * h.sqrt().asin()
}

pub fn distance_km(a: &str, b: &str) -> Option<f64> {
    Some(haversine(point(a)?, point(b)?))
}

static NEIGHBORS: OnceLock<Mutex<HashMap<(String, i64), Vec<String>>>> = OnceLock::new();

/// Города в радиусе от code (сам город и город его аэропорта не входят), по
/// возрастанию расстояния.
pub fn neighbors(code: &str, radius_km: f64) -> Vec<String> {
    if radius_km <= 0.0 {
        return Vec::new();
    }
    let code = code.to_uppercase();
    let key = (code.clone(), radius_km as i64);
    let cache = NEIGHBORS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(got) = cache.lock().unwrap().get(&key) {
        return got.clone();
    }
    let out = compute_neighbors(&code, radius_km);
    cache.lock().unwrap().insert(key, out.clone());
    out
}

fn compute_neighbors(code: &str, radius_km: f64) -> Vec<String> {
    let Some(p) = point(code) else { return Vec::new() };
    let g = geo();
    let mut own: HashSet<&str> = HashSet::new();
    own.insert(code);
    if let Some(a) = g.airports.get(code) {
        own.insert(a.2.as_str());
    }
    let mut out: Vec<(f64, &str)> = Vec::new();
    for (other, c) in &g.cities {
        if own.contains(other.as_str()) {
            continue;
        }
        if (c.0 - p.0).abs() * 111.0 > radius_km {
            continue;
        }
        let d = haversine(p, (c.0, c.1));
        if d <= radius_km {
            out.push((d, other.as_str()));
        }
    }
    out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then_with(|| a.1.cmp(b.1)));
    out.into_iter().map(|(_, c)| c.to_string()).collect()
}

/// Радиус из JSON: 0 при мусоре, не больше MAX_RADIUS_KM.
pub fn clamp_radius(v: Option<&Value>) -> i64 {
    let r = match v {
        Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f.trunc() as i64)).unwrap_or(0),
        Some(Value::String(s)) => s.trim().parse::<f64>().map(|f| f.trunc() as i64).unwrap_or(0),
        Some(Value::Bool(b)) => *b as i64,
        _ => 0,
    };
    r.clamp(0, MAX_RADIUS_KM)
}

/// Правила переезда по остановкам запроса.
pub struct Hops<'a> {
    pub stops: &'a [Stop],
    pub radius: Vec<f64>,
    departs_cache: Mutex<HashMap<(usize, String), Vec<String>>>,
}

impl<'a> Hops<'a> {
    pub fn new(stops: &'a [Stop]) -> Self {
        Hops {
            stops,
            radius: stops.iter().map(|s| s.radius_km as f64).collect(),
            departs_cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn active(&self) -> bool {
        self.radius.iter().any(|r| *r > 0.0)
    }

    /// Коды остановки + их соседи в радиусе (порядок стабильный).
    pub fn expanded(&self, i: usize) -> Vec<String> {
        let s = &self.stops[i];
        let mut out = s.codes.clone();
        if self.radius[i] > 0.0 {
            let mut seen: HashSet<String> = out.iter().cloned().collect();
            for c in &s.codes {
                for n in neighbors(c, self.radius[i]) {
                    if seen.insert(n.clone()) {
                        out.push(n);
                    }
                }
            }
        }
        out
    }

    /// В какие города можно прилететь в остановку i (None — любой).
    pub fn arrive_allowed(&self, i: usize) -> Option<HashSet<String>> {
        let s = &self.stops[i];
        if s.kind != "cities" {
            return None;
        }
        if s.exact {
            return Some(s.codes.iter().cloned().collect());
        }
        Some(self.expanded(i).into_iter().collect())
    }

    /// Из каких городов можно улететь дальше, прилетев в a (a первым).
    pub fn departs(&self, i: usize, a: &str) -> Vec<String> {
        let key = (i, a.to_string());
        if let Some(got) = self.departs_cache.lock().unwrap().get(&key) {
            return got.clone();
        }
        let got = self.compute_departs(i, a);
        self.departs_cache.lock().unwrap().insert(key, got.clone());
        got
    }

    fn compute_departs(&self, i: usize, a: &str) -> Vec<String> {
        let r = self.radius[i];
        let s = &self.stops[i];
        if r <= 0.0 || (i == 0 && s.exact) {
            return vec![a.to_string()];
        }
        let own: Option<HashSet<&str>> = if s.kind == "cities" { Some(s.codes.iter().map(|c| c.as_str()).collect()) } else { None };
        let mut near = neighbors(a, r);
        if let Some(own) = &own {
            if !own.contains(a) {
                near.retain(|n| own.contains(n.as_str()));
            }
        }
        let mut out = vec![a.to_string()];
        out.extend(near);
        out
    }

    pub fn collect_codes(&self, i: usize) -> Vec<String> {
        if self.stops[i].kind == "cities" { self.expanded(i) } else { Vec::new() }
    }
}
