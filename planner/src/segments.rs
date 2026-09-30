//! Сегмент маршрута из нормализованного билета — контракт карточки на фронте
//! (frontend/src/types.ts Segment): концы (город и аэропорт), времена, длительность,
//! пересадки с ожиданием, багаж, hidden-city, ссылка на покупку. Плюс справочник
//! имён городов (CityLookup: core/city_names.json + курируемые агломерации +
//! core/geo.json).

use std::collections::HashMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::airports::city_by_code;
use crate::dates::parse_naive;
use crate::nearby::city_entry;
use crate::ticket::{Baggage, Ticket};

pub const AVIASALES_BASE: &str = "https://www.aviasales.ru";

pub fn flag_emoji(iso2: &str) -> String {
    if iso2.len() != 2 || !iso2.chars().all(|c| c.is_ascii_alphabetic()) {
        return String::new();
    }
    iso2.chars()
        .map(|c| char::from_u32(0x1F1E6 + (c.to_ascii_uppercase() as u32 - 'A' as u32)).unwrap_or(' '))
        .collect()
}

#[derive(Debug, Clone)]
pub struct CityInfo {
    pub city: String,
    pub country: String,
    pub flag: String,
}

static CITY_NAMES: OnceLock<HashMap<String, (String, String)>> = OnceLock::new();

pub fn city_names_path() -> String {
    std::env::var("CITY_NAMES_PATH").unwrap_or_else(|_| "core/city_names.json".into())
}

/// core/city_names.json ({код: [имя, ISO2]}) — один раз на процесс.
pub fn city_names() -> &'static HashMap<String, (String, String)> {
    CITY_NAMES.get_or_init(|| match std::fs::read_to_string(city_names_path()) {
        Ok(text) => serde_json::from_str::<HashMap<String, (String, String)>>(&text).unwrap_or_else(|e| {
            eprintln!("[segments] city_names.json не разбирается: {e}");
            HashMap::new()
        }),
        Err(e) => {
            eprintln!("[segments] city_names.json не прочитан ({}): {e}", city_names_path());
            HashMap::new()
        }
    })
}

/// Имя/страна/флаг города по коду: справочник имён, затем курируемые агломерации
/// (BJS, LON…), затем core/geo.json (соседние города и пр.), иначе — сам код.
pub fn city_info(code: &str) -> CityInfo {
    let mut name = String::new();
    let mut country = String::new();
    if let Some((n, c)) = city_names().get(code) {
        name = n.clone();
        country = c.clone();
    }
    if name.is_empty() {
        if let Some(entry) = city_by_code(code) {
            name = entry.2.to_string();
            country = entry.3.to_string();
        }
    }
    if name.is_empty() && !code.is_empty() {
        if let Some(geo) = city_entry(code) {
            name = geo.3.clone();
            if country.is_empty() {
                country = geo.2.clone();
            }
        }
    }
    let city = if name.is_empty() { code.to_string() } else { name };
    let flag = flag_emoji(&country);
    CityInfo { city, country, flag }
}

/// Пара [имя, флаг] для словарей `cities` результата.
pub fn city_pair(code: &str) -> (String, String) {
    let ci = city_info(code);
    (ci.city, ci.flag)
}

pub fn ddmm(iso_dt: &str) -> String {
    match parse_naive(iso_dt) {
        Some(dt) => dt.format("%d%m").to_string(),
        None => String::new(),
    }
}

pub fn booking_link(origin: &str, dest: &str, depart_at: &str) -> String {
    format!("{AVIASALES_BASE}/search/{origin}{}{dest}1", ddmm(depart_at))
}

/// Относительный `link` из API → абсолютная ссылка на Aviasales.
pub fn booking_url(link: Option<&str>) -> Option<String> {
    let link = link?;
    if link.is_empty() {
        return None;
    }
    Some(if link.starts_with("http") { link.to_string() } else { format!("{AVIASALES_BASE}{link}") })
}

/// Суммарное время на земле на пересадках, мин: duration − duration_to.
pub fn layover_minutes(flight: &Ticket, transfers: i64) -> Option<i64> {
    let (total, air) = (flight.duration?, flight.duration_to?);
    if transfers == 0 || total == 0 || air == 0 || total <= air {
        return None;
    }
    Some(total - air)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SegmentPoint {
    pub code: Option<String>,
    pub city: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minutes: Option<Option<i64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub night: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visa: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub country: Option<Option<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Segment {
    pub origin: Option<String>,
    pub destination: Option<String>,
    pub origin_airport: Option<String>,
    pub destination_airport: Option<String>,
    pub origin_city: String,
    pub destination_city: String,
    pub departure_at: Option<String>,
    pub arrival_at: Option<String>,
    pub duration: Option<i64>,
    pub transfers: i64,
    pub direct: bool,
    pub transfer_points: Vec<SegmentPoint>,
    pub layover_minutes: Option<i64>,
    pub airline: Option<String>,
    pub flight_number: Option<String>,
    pub price: f64,
    pub link: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baggage: Option<Baggage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hidden_city: Option<Value>,
}

/// Сегмент из билета (зеркало `segments.Builder.make_segment`).
pub fn make_segment(flight: &Ticket) -> Segment {
    let origin = flight.origin_city().map(str::to_string);
    let dest = flight.dest_city().map(str::to_string);
    let dep = flight.departure_at.clone().filter(|s| !s.is_empty());
    let arr = flight.arrival();
    let transfers = flight.transfers;
    let origin_s = origin.clone().unwrap_or_default();
    let dest_s = dest.clone().unwrap_or_default();
    let transfer_points = match &flight.transfer_points {
        Some(points) => points
            .iter()
            .map(|p| SegmentPoint {
                code: p.code.clone(),
                city: city_info(p.code.as_deref().unwrap_or("")).city,
                minutes: Some(p.minutes),
                night: Some(p.night),
                visa: Some(p.visa),
                country: Some(p.country.clone()),
            })
            .collect(),
        None => points_from_link(flight.link.as_deref(), transfers),
    };
    let mut link = dep.as_deref().map(|d| booking_link(&origin_s, &dest_s, d));
    let mut hidden = None;
    if let Some(h) = &flight.hidden_city {
        let mut v = serde_json::to_value(h).unwrap_or(Value::Null);
        if let Some(m) = v.as_object_mut() {
            m.insert("final_city".into(), Value::String(city_info(h.final_.as_deref().unwrap_or("")).city));
        }
        hidden = Some(v);
        link = booking_url(flight.link.as_deref()).or(link);
    }
    Segment {
        origin_airport: flight.origin_airport.clone().filter(|s| !s.is_empty()).or_else(|| origin.clone()),
        destination_airport: flight.destination_airport.clone().filter(|s| !s.is_empty()).or_else(|| dest.clone()),
        origin_city: city_info(&origin_s).city,
        destination_city: city_info(&dest_s).city,
        origin,
        destination: dest,
        departure_at: dep,
        arrival_at: arr,
        duration: flight.duration,
        transfers,
        direct: transfers == 0,
        transfer_points,
        layover_minutes: layover_minutes(flight, transfers),
        airline: flight.airline.clone(),
        flight_number: flight.flight_number.clone(),
        price: flight.price(),
        link,
        baggage: flight.baggage.clone(),
        hidden_city: hidden,
    }
}

/// Города пересадок из токена ссылки Aviasales (`t=…SVOTJMNYA_…`) — для рейсов без
/// `transfer_points` (данные REST). Промежуточные точки.
fn points_from_link(link: Option<&str>, transfers: i64) -> Vec<SegmentPoint> {
    let Some(link) = link else { return Vec::new() };
    if transfers <= 0 {
        return Vec::new();
    }
    let Some(t) = link.split(['?', '&']).skip(1).find_map(|kv| kv.strip_prefix("t=")) else { return Vec::new() };
    // цепочка ≥6 заглавных букв перед '_'
    let bytes = t.as_bytes();
    let mut best: Option<&str> = None;
    let mut start = None;
    for (i, b) in bytes.iter().enumerate() {
        if b.is_ascii_uppercase() {
            if start.is_none() {
                start = Some(i);
            }
        } else {
            if let Some(s) = start {
                if *b == b'_' && i - s >= 6 {
                    best = Some(&t[s..i]);
                    break;
                }
            }
            start = None;
        }
    }
    let Some(chain) = best else { return Vec::new() };
    let codes: Vec<&str> = (0..chain.len()).step_by(3).map(|i| &chain[i..(i + 3).min(chain.len())]).collect();
    if codes.len() as i64 != transfers + 2 {
        return Vec::new();
    }
    codes[1..codes.len() - 1]
        .iter()
        .map(|c| SegmentPoint { code: Some(c.to_string()), city: city_info(c).city, minutes: None, night: None, visa: None, country: None })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_and_links() {
        assert_eq!(flag_emoji("RU"), "🇷🇺");
        assert_eq!(flag_emoji(""), "");
        assert_eq!(booking_link("MOW", "SEL", "2026-10-15T08:25:00+03:00"), "https://www.aviasales.ru/search/MOW1510SEL1");
        assert_eq!(booking_url(Some("/search/x")).as_deref(), Some("https://www.aviasales.ru/search/x"));
        assert_eq!(booking_url(Some("")), None);
    }

    #[test]
    fn segment_from_ticket() {
        let t = Ticket {
            origin: Some("MOW".into()),
            destination: Some("SEL".into()),
            departure_at: Some("2026-10-15T08:25:00+03:00".into()),
            duration: Some(600),
            duration_to: Some(500),
            transfers: 1,
            price: Some(100.0),
            transfer_points: Some(vec![crate::ticket::TransferPoint { code: Some("SVX".into()), minutes: Some(100), ..Default::default() }]),
            ..Default::default()
        };
        let s = make_segment(&t);
        assert_eq!(s.origin_airport.as_deref(), Some("MOW"));
        assert_eq!(s.layover_minutes, Some(100));
        assert!(!s.direct);
        assert_eq!(s.transfer_points[0].code.as_deref(), Some("SVX"));
        assert_eq!(s.arrival_at.as_deref(), Some("2026-10-15T18:25:00"));
        let json = serde_json::to_value(&s).unwrap();
        assert!(json.get("baggage").is_none());
        assert!(json.get("hidden_city").is_none());
        assert_eq!(json["transfer_points"][0]["minutes"], 100);
    }

    #[test]
    fn points_from_rest_link() {
        let pts = points_from_link(Some("/search/MOW1510SEL1?t=U617920527001792263300003150DMESVXICN_59cc_36127&x=1"), 1);
        assert_eq!(pts.len(), 1);
        assert_eq!(pts[0].code.as_deref(), Some("SVX"));
        assert!(points_from_link(Some("/search/MOW1510SEL1?t=U617920527001792263300003150DMESVXICN_59cc_36127"), 2).is_empty());
    }
}
