//! Даты и время планировщика: разбор ISO как «наивного» локального времени
//! (смещение срезается, не пересчитывается: `2026-10-18T10:20:00+09:00` → 10:20),
//! секунды от эпохи, порядковый номер дня (как `date.toordinal()` в Python),
//! дни пребывания, выходные внутри пребывания, список дат окна.

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime};

pub const DAY_SECONDS: f64 = 86400.0;

/// «Наивное» локальное время из ISO-строки: берём дату и время, зону отбрасываем.
/// Понимает `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM[:SS[.fff]][±HH:MM|Z]` и вариант с пробелом.
pub fn parse_naive(s: &str) -> Option<NaiveDateTime> {
    let s = s.trim();
    if s.len() < 10 {
        return None;
    }
    let date = NaiveDate::parse_from_str(&s[..10], "%Y-%m-%d").ok()?;
    if s.len() == 10 {
        return Some(date.and_time(NaiveTime::default()));
    }
    let rest = &s[11..];
    // время до зоны: до '+', 'Z' или '-' (после позиции времени)
    let end = rest
        .find(|c: char| c == '+' || c == 'Z' || c == 'z' || c == '-')
        .unwrap_or(rest.len());
    let time_part = &rest[..end];
    let time = NaiveTime::parse_from_str(time_part, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(time_part, "%H:%M:%S"))
        .or_else(|_| NaiveTime::parse_from_str(time_part, "%H:%M"))
        .ok()?;
    Some(date.and_time(time))
}

/// Секунды от 1970-01-01 наивного времени (как `(dt - epoch).total_seconds()`).
pub fn naive_seconds(dt: NaiveDateTime) -> f64 {
    dt.and_utc().timestamp() as f64 + dt.and_utc().timestamp_subsec_micros() as f64 / 1e6
}

/// Порядковый номер дня: 0001-01-01 → 1 (совпадает с `date.toordinal()`).
pub fn ordinal(date: NaiveDate) -> i64 {
    date.num_days_from_ce() as i64
}

pub fn date_from_ordinal(ord: i64) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(ord as i32)
}

/// День недели по порядковому номеру: 0 — понедельник … 5 — суббота, 6 — воскресенье.
pub fn weekday_of_ordinal(ord: i64) -> i64 {
    (ord + 6).rem_euclid(7)
}

/// `YYYY-MM-DD` из ISO-строки (дата локального времени).
pub fn date_only(iso: &str) -> String {
    match parse_naive(iso) {
        Some(dt) => dt.date().format("%Y-%m-%d").to_string(),
        None => iso.get(..10).unwrap_or(iso).to_string(),
    }
}

/// Дата, сдвинутая на `days` дней, в `YYYY-MM-DD`.
pub fn shift_date(date_str: &str, days: i64) -> String {
    match NaiveDate::parse_from_str(date_str.get(..10).unwrap_or(date_str), "%Y-%m-%d") {
        Ok(d) => (d + Duration::days(days)).format("%Y-%m-%d").to_string(),
        Err(_) => date_str.to_string(),
    }
}

/// Прилёт по вылету и длительности (ISO без зоны, как `datetime.isoformat()`).
pub fn calculate_arrival(departure_at: &str, duration_min: i64) -> String {
    match parse_naive(departure_at) {
        Some(dt) => (dt + Duration::minutes(duration_min))
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string(),
        None => departure_at.to_string(),
    }
}

/// Дней между прилётом и вылетом: floor((вылет − прилёт) / сутки), как `timedelta.days`.
pub fn stay_between(arrive_iso: &str, depart_iso: &str) -> i64 {
    match (parse_naive(arrive_iso), parse_naive(depart_iso)) {
        (Some(a), Some(d)) => stay_days_secs(naive_seconds(a), naive_seconds(d)),
        _ => 0,
    }
}

pub fn stay_days_secs(arrive_secs: f64, depart_secs: f64) -> i64 {
    ((depart_secs - arrive_secs) / DAY_SECONDS).floor() as i64
}

/// Оба выходных (сб И вс) попадают в пребывание `[arrive..depart]` (по датам);
/// false, если вылет раньше прилёта.
pub fn has_both_weekend_days(arrive_iso: &str, depart_iso: &str) -> bool {
    match (parse_naive(arrive_iso), parse_naive(depart_iso)) {
        (Some(a), Some(d)) => {
            weekend_covered(naive_seconds(a), ordinal(a.date()), naive_seconds(d), ordinal(d.date()))
        }
        _ => false,
    }
}

/// То же по уже разобранным значениям (секунды и порядковые номера дней).
pub fn weekend_covered(arrive_secs: f64, arrive_ord: i64, depart_secs: f64, depart_ord: i64) -> bool {
    if depart_secs < arrive_secs {
        return false;
    }
    let (mut sat, mut sun) = (false, false);
    let mut cur = arrive_ord;
    while cur <= depart_ord {
        let wd = weekday_of_ordinal(cur);
        sat = sat || wd == 5;
        sun = sun || wd == 6;
        if sat && sun {
            return true;
        }
        cur += 1;
    }
    false
}

/// Первый день D ≥ arr, при котором [arr, D] содержит и сб, и вс.
pub fn weekend_deadline(arr_ord: i64) -> i64 {
    let wd = weekday_of_ordinal(arr_ord);
    arr_ord
        + if wd == 6 {
            6
        } else if wd == 5 {
            1
        } else {
            6 - wd
        }
}

/// Даты от start до end включительно (`YYYY-MM-DD`); пусто, если строки не даты.
pub fn date_range(start: &str, end: &str) -> Vec<String> {
    let (Ok(s), Ok(e)) = (
        NaiveDate::parse_from_str(start, "%Y-%m-%d"),
        NaiveDate::parse_from_str(end, "%Y-%m-%d"),
    ) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut cur = s;
    while cur <= e {
        out.push(cur.format("%Y-%m-%d").to_string());
        cur += Duration::days(1);
    }
    out
}

/// Порядковый номер дня строки `YYYY-MM-DD`.
pub fn date_ordinal(date_str: &str) -> Option<i64> {
    NaiveDate::parse_from_str(date_str.get(..10)?, "%Y-%m-%d")
        .ok()
        .map(ordinal)
}

/// Разница двух ISO-времён с зонами в минутах (пояса разные — учитываем); None,
/// если у одного из них зоны нет, а у другого есть, или строка не разбирается.
pub fn minutes_between_aware(start: &str, end: &str) -> Option<i64> {
    let a = parse_aware(start)?;
    let b = parse_aware(end)?;
    match (a, b) {
        (Aware::Zoned(a), Aware::Zoned(b)) => Some(((b - a).num_seconds() as f64 / 60.0).round() as i64),
        (Aware::Naive(a), Aware::Naive(b)) => Some(((b - a).num_seconds() as f64 / 60.0).round() as i64),
        _ => None,
    }
}

enum Aware {
    Zoned(chrono::DateTime<chrono::FixedOffset>),
    Naive(NaiveDateTime),
}

fn parse_aware(s: &str) -> Option<Aware> {
    let s = s.trim();
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(Aware::Zoned(dt));
    }
    // Python fromisoformat понимает и смещение без секунд (+03), и без двоеточия
    if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%z") {
        return Some(Aware::Zoned(dt));
    }
    parse_naive(s).map(Aware::Naive)
}

/// Локальное время сейчас в формате `datetime.now().isoformat()`.
pub fn now_iso() -> String {
    chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
}

pub fn now_naive() -> NaiveDateTime {
    chrono::Local::now().naive_local()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_offsets_and_dates() {
        let dt = parse_naive("2026-10-18T10:20:00+09:00").unwrap();
        assert_eq!(dt.format("%H:%M").to_string(), "10:20");
        assert_eq!(parse_naive("2026-10-18").unwrap().format("%Y-%m-%dT%H:%M").to_string(), "2026-10-18T00:00");
        assert_eq!(parse_naive("2026-10-18T10:20:00Z").unwrap().hour(), 10);
        assert_eq!(parse_naive("2026-10-18 10:20:00").unwrap().hour(), 10);
        assert!(parse_naive("nope").is_none());
    }

    #[test]
    fn stay_and_weekend() {
        assert_eq!(stay_between("2026-11-01T10:00:00", "2026-11-03T09:00:00"), 1);
        assert_eq!(stay_between("2026-11-01T10:00:00", "2026-11-01T09:00:00"), -1);
        // 2026-11-07 — суббота, 2026-11-08 — воскресенье
        assert!(has_both_weekend_days("2026-11-06T10:00:00", "2026-11-08T10:00:00"));
        assert!(!has_both_weekend_days("2026-11-06T10:00:00", "2026-11-07T23:00:00"));
        let sat = date_ordinal("2026-11-07").unwrap();
        assert_eq!(weekday_of_ordinal(sat), 5);
        assert_eq!(weekend_deadline(sat), sat + 1);
        assert_eq!(weekend_deadline(sat + 1), sat + 7);
        assert_eq!(weekend_deadline(sat - 1), sat + 1);
    }

    #[test]
    fn ranges_and_minutes() {
        assert_eq!(date_range("2026-11-01", "2026-11-03").len(), 3);
        assert_eq!(minutes_between_aware("2026-10-15T08:25:00+03:00", "2026-10-15T12:55:00+05:00"), Some(150));
        assert_eq!(shift_date("2026-11-30", 5), "2026-12-05");
        assert_eq!(calculate_arrival("2026-11-01T23:00:00+03:00", 120), "2026-11-02T01:00:00");
    }

    use chrono::Timelike;
}
