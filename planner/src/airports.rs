//! Справочник аэропортов и автокомплит точек A/B.
//!
//! Основа — data/airport_network.json (физические аэропорты: SVO, DME, ICN, …).
//! Проект работает на уровне ГОРОДОВ (MOW, SEL, BJS…), таких кодов в сети нет, поэтому
//! сверху подмешивается курируемый список агломераций с русскими и английскими именами.

use std::sync::OnceLock;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::segments::flag_emoji;

/// (IATA города, русское имя, английское имя, ISO2 страны). Ранжируются выше аэропортов.
pub const CITY_CODES: &[(&str, &str, &str, &str)] = &[
    ("MOW", "Москва", "Moscow", "RU"),
    ("LED", "Санкт-Петербург", "Saint Petersburg", "RU"),
    ("SVX", "Екатеринбург", "Yekaterinburg", "RU"),
    ("OVB", "Новосибирск", "Novosibirsk", "RU"),
    ("KZN", "Казань", "Kazan", "RU"),
    ("AER", "Сочи", "Sochi", "RU"),
    ("SEL", "Сеул", "Seoul", "KR"),
    ("ICN", "Сеул (Инчхон)", "Seoul Incheon", "KR"),
    ("BJS", "Пекин", "Beijing", "CN"),
    ("SHA", "Шанхай", "Shanghai", "CN"),
    ("CAN", "Гуанчжоу", "Guangzhou", "CN"),
    ("HKG", "Гонконг", "Hong Kong", "HK"),
    ("TYO", "Токио", "Tokyo", "JP"),
    ("OSA", "Осака", "Osaka", "JP"),
    ("BKK", "Бангкок", "Bangkok", "TH"),
    ("SGN", "Хошимин", "Ho Chi Minh City", "VN"),
    ("DXB", "Дубай", "Dubai", "AE"),
    ("AUH", "Абу-Даби", "Abu Dhabi", "AE"),
    ("IST", "Стамбул", "Istanbul", "TR"),
    ("LON", "Лондон", "London", "GB"),
    ("PAR", "Париж", "Paris", "FR"),
    ("MIL", "Милан", "Milan", "IT"),
    ("ROM", "Рим", "Rome", "IT"),
    ("BER", "Берлин", "Berlin", "DE"),
    ("BCN", "Барселона", "Barcelona", "ES"),
    ("MAD", "Мадрид", "Madrid", "ES"),
    ("NYC", "Нью-Йорк", "New York", "US"),
    ("ALA", "Алматы", "Almaty", "KZ"),
    ("TAS", "Ташкент", "Tashkent", "UZ"),
    ("DEL", "Дели", "Delhi", "IN"),
    ("BAK", "Баку", "Baku", "AZ"),
    ("JKT", "Джакарта", "Jakarta", "ID"),
    ("SAO", "Сан-Паулу", "São Paulo", "BR"),
    ("SPK", "Саппоро", "Sapporo", "JP"),
    ("YTO", "Торонто", "Toronto", "CA"),
    ("NHA", "Нячанг", "Nha Trang", "VN"),
    ("RTW", "Саратов", "Saratov", "RU"),
    ("BSZ", "Бишкек", "Bishkek", "KG"),
];

pub fn city_by_code(code: &str) -> Option<&'static (&'static str, &'static str, &'static str, &'static str)> {
    CITY_CODES.iter().find(|c| c.0 == code)
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NetworkEntry {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub municipality: Option<String>,
    #[serde(default)]
    pub country: Option<String>,
    #[serde(default)]
    pub iso_country: Option<String>,
}

static NETWORK: OnceLock<IndexMap<String, NetworkEntry>> = OnceLock::new();

pub fn network_path() -> String {
    std::env::var("AIRPORT_NETWORK_PATH").unwrap_or_else(|_| "data/airport_network.json".into())
}

/// Сеть аэропортов; пусто, если файла нет или он битый (порядок — как в файле).
pub fn network() -> &'static IndexMap<String, NetworkEntry> {
    NETWORK.get_or_init(|| {
        let path = network_path();
        match std::fs::read_to_string(&path) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_else(|e| {
                eprintln!("[network] не удалось разобрать {path}: {e}");
                IndexMap::new()
            }),
            Err(_) => IndexMap::new(),
        }
    })
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AirportOption {
    pub code: String,
    pub city: String,
    pub country: String,
    pub flag: String,
    pub label: String,
}

fn city_option(entry: &(&str, &str, &str, &str)) -> AirportOption {
    let (code, _ru, en, country) = *entry;
    AirportOption { code: code.into(), city: en.into(), country: country.into(), flag: flag_emoji(country), label: format!("{en} ({code})") }
}

fn airport_option(code: &str, entry: &NetworkEntry) -> AirportOption {
    let city = entry
        .municipality
        .clone()
        .filter(|s| !s.is_empty())
        .or_else(|| entry.name.clone().filter(|s| !s.is_empty()))
        .unwrap_or_else(|| code.to_string());
    let country = entry.country.clone().unwrap_or_default();
    AirportOption { code: code.into(), city: city.clone(), flag: flag_emoji(&country), label: format!("{city} ({code})"), country }
}

/// Совпадение по началу слова, а не по произвольной подстроке ('pek' не находит 'toPEKa').
fn matches_word_start(text: &str, ql: &str) -> bool {
    text.replace('-', " ").split_whitespace().any(|w| w.starts_with(ql))
}

pub fn search_airports(q: &str, limit: usize) -> Vec<AirportOption> {
    let q = q.trim();
    if q.is_empty() {
        return Vec::new();
    }
    let (qu, ql) = (q.to_uppercase(), q.to_lowercase());
    let mut results = Vec::new();
    let mut used: Vec<&str> = Vec::new();
    for entry in CITY_CODES {
        let (code, ru, en, _) = *entry;
        if code == qu || code.starts_with(&qu) || matches_word_start(&ru.to_lowercase(), &ql) || matches_word_start(&en.to_lowercase(), &ql) {
            results.push(city_option(entry));
            used.push(code);
        }
    }
    let net = network();
    let (mut exact, mut prefix, mut contains) = (Vec::new(), Vec::new(), Vec::new());
    for (code, entry) in net {
        if used.contains(&code.as_str()) {
            continue;
        }
        let muni = entry.municipality.as_deref().unwrap_or("").to_lowercase();
        let name = entry.name.as_deref().unwrap_or("").to_lowercase();
        if *code == qu {
            exact.push((code, entry));
        } else if code.starts_with(&qu) {
            prefix.push((code, entry));
        } else if matches_word_start(&muni, &ql) || matches_word_start(&name, &ql) {
            contains.push((code, entry));
        }
    }
    for (code, entry) in exact.into_iter().chain(prefix).chain(contains) {
        results.push(airport_option(code, entry));
    }
    results.truncate(limit);
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn curated_cities_first() {
        let got = search_airports("mos", 5);
        assert_eq!(got[0].code, "MOW");
        assert_eq!(got[0].flag, "🇷🇺");
        assert!(search_airports("", 5).is_empty());
        assert!(matches_word_start("beijing capital", "bei"));
        assert!(!matches_word_start("topeka", "pek"));
    }
}
