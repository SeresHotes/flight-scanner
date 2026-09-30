//! Нормализованный билет (тот же словарь, что `core.graphql_api.normalize_ticket` и
//! `collector.lake.from_row`): концы на уровне города плюс аэропорты, времена,
//! длительности, сегменты, пересадки, багаж, ссылка. Виртуальный рейс hidden-city —
//! тот же билет, обрезанный до хаба, с блоком `hidden_city`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::dates::minutes_between_aware;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Leg {
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub destination: Option<String>,
    #[serde(default)]
    pub departure_at: Option<String>,
    #[serde(default)]
    pub arrival_at: Option<String>,
    #[serde(default)]
    pub flight_number: Option<String>,
    #[serde(default)]
    pub carrier: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct TransferPoint {
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
    #[serde(default)]
    pub country: Option<String>,
    #[serde(default)]
    pub minutes: Option<i64>,
    #[serde(default)]
    pub night: bool,
    #[serde(default)]
    pub visa: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Baggage {
    #[serde(default)]
    pub known: bool,
    #[serde(default)]
    pub included: bool,
    #[serde(default)]
    pub pieces: Option<i64>,
    #[serde(default)]
    pub kg: Option<i64>,
}

/// Что за билет стоит за виртуальным рейсом hidden-city.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct HiddenCity {
    #[serde(default, rename = "final")]
    pub final_: Option<String>,
    #[serde(default)]
    pub final_airport: Option<String>,
    #[serde(default)]
    pub chain: Vec<String>,
    #[serde(default)]
    pub full_duration: Option<i64>,
    #[serde(default)]
    pub full_transfers: i64,
    #[serde(default)]
    pub baggage: Option<Baggage>,
    #[serde(default)]
    pub arrival_estimated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Ticket {
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub destination: Option<String>,
    #[serde(default)]
    pub origin_airport: Option<String>,
    #[serde(default)]
    pub destination_airport: Option<String>,
    #[serde(default)]
    pub departure_at: Option<String>,
    #[serde(default)]
    pub arrival_at: Option<String>,
    #[serde(default)]
    pub duration: Option<i64>,
    #[serde(default)]
    pub duration_to: Option<i64>,
    #[serde(default, deserialize_with = "de_int_or_null")]
    pub transfers: i64,
    #[serde(default)]
    pub airline: Option<String>,
    #[serde(default)]
    pub flight_number: Option<String>,
    #[serde(default)]
    pub price: Option<f64>,
    #[serde(default)]
    pub currency: Option<String>,
    #[serde(default)]
    pub link: Option<String>,
    #[serde(default)]
    pub chain: Vec<String>,
    #[serde(default)]
    pub legs: Vec<Leg>,
    /// None — пересадки неизвестны (данные REST): сегмент достаёт их из ссылки.
    #[serde(default)]
    pub transfer_points: Option<Vec<TransferPoint>>,
    #[serde(default)]
    pub baggage: Option<Baggage>,
    #[serde(default)]
    pub baggage_code: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub search_origin: Option<String>,
    #[serde(default)]
    pub search_destination: Option<String>,
    #[serde(default)]
    pub search_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hidden_city: Option<HiddenCity>,
    /// Наследие REST (`layover_minutes`): у билетов GraphQL нет.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layover_minutes: Option<i64>,
}

fn de_int_or_null<'de, D: serde::Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    let v = Option::<Value>::deserialize(d)?;
    Ok(match v {
        Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)).unwrap_or(0),
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        _ => 0,
    })
}

fn upper(v: &Option<String>) -> Option<String> {
    v.as_deref().filter(|s| !s.is_empty()).map(|s| s.to_uppercase())
}

impl Ticket {
    /// Цена билета (`price`, иначе 0).
    pub fn price(&self) -> f64 {
        self.price.unwrap_or(0.0)
    }

    /// Код города вылета (или города запроса) и аэропорта — как в билете.
    pub fn origin_city(&self) -> Option<&str> {
        self.origin
            .as_deref()
            .filter(|s| !s.is_empty())
            .or(self.search_origin.as_deref().filter(|s| !s.is_empty()))
    }

    pub fn dest_city(&self) -> Option<&str> {
        self.destination
            .as_deref()
            .filter(|s| !s.is_empty())
            .or(self.search_destination.as_deref().filter(|s| !s.is_empty()))
    }

    /// Коды стороны (город И аэропорт, UPPER): остановку пользователь может задать
    /// любым из них, поэтому стыковка и фильтры матчат по обоим.
    pub fn side_codes(&self, dest: bool) -> Vec<String> {
        let (city, airport) = if dest {
            (self.dest_city().map(str::to_string), upper(&self.destination_airport))
        } else {
            (self.origin_city().map(str::to_string), upper(&self.origin_airport))
        };
        let mut out = Vec::with_capacity(2);
        if let Some(c) = city {
            let c = c.to_uppercase();
            if !c.is_empty() {
                out.push(c);
            }
        }
        if let Some(a) = airport {
            if !out.contains(&a) {
                out.push(a);
            }
        }
        out
    }

    /// Реальное время прилёта: `arrival_at`, иначе вылет + длительность, иначе вылет.
    pub fn arrival(&self) -> Option<String> {
        if let Some(a) = self.arrival_at.as_deref().filter(|s| !s.is_empty()) {
            return Some(a.to_string());
        }
        match (&self.departure_at, self.duration) {
            (Some(dep), Some(dur)) if !dep.is_empty() && dur != 0 => {
                Some(crate::dates::calculate_arrival(dep, dur))
            }
            _ => self.departure_at.clone(),
        }
    }

    /// Ключ дедупликации билета между сериями (тот же рейс приходит из A→B и A→ANY):
    /// цепочка аэропортов + вылет + номера рейсов + цена.
    pub fn flight_key(&self) -> String {
        let mut key = String::new();
        for c in &self.chain {
            key.push_str(c);
            key.push('>');
        }
        key.push('|');
        key.push_str(self.departure_at.as_deref().unwrap_or(""));
        key.push('|');
        for l in &self.legs {
            key.push_str(l.flight_number.as_deref().unwrap_or("-"));
            key.push(',');
        }
        key.push('|');
        match self.price {
            Some(p) => key.push_str(&format!("{p}")),
            None => key.push_str("null"),
        }
        key
    }

    /// Минимальное ожидание на пересадках (мин), если известно у всех.
    pub fn min_transfer_minutes(&self) -> Option<i64> {
        let pts = self.transfer_points.as_ref()?;
        if pts.is_empty() {
            return None;
        }
        let mut best = i64::MAX;
        for p in pts {
            best = best.min(p.minutes?);
        }
        Some(best)
    }

    /// Виртуальный рейс «выходим на k-й пересадке»: сегменты до хаба, реальное время
    /// прилёта в хаб, transfers = k, цена всего билета; hidden_city — что это за билет.
    pub fn virtual_flight(&self, k: usize, hub_city: &str) -> Ticket {
        let points = self.transfer_points.as_deref().unwrap_or(&[]);
        let tp = &points[k];
        let hub = tp.code.clone().unwrap_or_default();
        let legs: Vec<Leg> = self.legs[..k + 1].to_vec();
        let arrival = legs.last().and_then(|l| l.arrival_at.clone());
        let air: Vec<Option<i64>> = legs
            .iter()
            .map(|l| minutes_between_aware(l.departure_at.as_deref().unwrap_or(""), l.arrival_at.as_deref().unwrap_or("")))
            .collect();
        let duration_to = if air.iter().all(|m| m.is_some()) {
            Some(air.iter().map(|m| m.unwrap()).sum())
        } else {
            None
        };
        let cut = self
            .chain
            .iter()
            .enumerate()
            .find(|(j, code)| *j >= 1 && **code == hub)
            .map(|(j, _)| j)
            .unwrap_or(self.chain.len().saturating_sub(1));
        let mut v = self.clone();
        v.destination = Some(hub_city.to_string());
        v.destination_airport = Some(hub.clone());
        v.duration = minutes_between_aware(
            self.departure_at.as_deref().unwrap_or(""),
            arrival.as_deref().unwrap_or(""),
        );
        v.arrival_at = arrival;
        v.duration_to = duration_to;
        v.transfers = k as i64;
        v.transfer_points = Some(points[..k].to_vec());
        v.legs = legs;
        v.chain = self.chain[..(cut + 1).min(self.chain.len())].to_vec();
        v.hidden_city = Some(HiddenCity {
            final_: self.destination.clone(),
            final_airport: self.destination_airport.clone(),
            chain: self.chain.clone(),
            full_duration: self.duration,
            full_transfers: self.transfers,
            baggage: self.baggage.clone(),
            arrival_estimated: false,
        });
        v
    }

    /// Урезанная копия для колонки `seg` файла джобы: только поля сегмента результата
    /// (`legs`/`chain` сегменту не нужны; ссылка — только у hidden-city и рейсов без пересадок).
    pub fn segment_source(&self) -> Ticket {
        let mut out = self.clone();
        out.legs = Vec::new();
        out.chain = Vec::new();
        out.currency = None;
        out.baggage_code = None;
        out.source = None;
        out.search_date = None;
        if !(self.hidden_city.is_some() || self.transfer_points.is_none()) {
            out.link = None;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticket() -> Ticket {
        Ticket {
            origin: Some("MOW".into()),
            destination: Some("SEL".into()),
            origin_airport: Some("SVO".into()),
            destination_airport: Some("ICN".into()),
            departure_at: Some("2026-10-15T08:25:00+03:00".into()),
            arrival_at: Some("2026-10-17T18:55:00+09:00".into()),
            duration: Some(3150),
            transfers: 2,
            price: Some(36127.0),
            chain: vec!["SVO".into(), "PEK".into(), "HRB".into(), "ICN".into()],
            legs: vec![
                Leg { origin: Some("SVO".into()), destination: Some("PEK".into()), departure_at: Some("2026-10-15T08:25:00+03:00".into()), arrival_at: Some("2026-10-15T20:00:00+08:00".into()), flight_number: Some("1".into()), carrier: None },
                Leg { origin: Some("PEK".into()), destination: Some("HRB".into()), departure_at: Some("2026-10-16T08:00:00+08:00".into()), arrival_at: Some("2026-10-16T10:00:00+08:00".into()), flight_number: Some("2".into()), carrier: None },
                Leg { origin: Some("HRB".into()), destination: Some("ICN".into()), departure_at: Some("2026-10-17T15:00:00+08:00".into()), arrival_at: Some("2026-10-17T18:55:00+09:00".into()), flight_number: Some("3".into()), carrier: None },
            ],
            transfer_points: Some(vec![
                TransferPoint { code: Some("PEK".into()), minutes: Some(720), ..Default::default() },
                TransferPoint { code: Some("HRB".into()), minutes: Some(1740), ..Default::default() },
            ]),
            ..Default::default()
        }
    }

    #[test]
    fn virtual_flight_cuts_at_hub() {
        let v = ticket().virtual_flight(0, "BJS");
        assert_eq!(v.destination.as_deref(), Some("BJS"));
        assert_eq!(v.destination_airport.as_deref(), Some("PEK"));
        assert_eq!(v.arrival_at.as_deref(), Some("2026-10-15T20:00:00+08:00"));
        assert_eq!(v.transfers, 0);
        assert_eq!(v.chain, vec!["SVO", "PEK"]);
        assert_eq!(v.legs.len(), 1);
        assert_eq!(v.duration, Some(6 * 60 + 35));
        assert_eq!(v.duration_to, Some(6 * 60 + 35));
        assert_eq!(v.price(), 36127.0);
        let h = v.hidden_city.unwrap();
        assert_eq!(h.final_.as_deref(), Some("SEL"));
        assert_eq!(h.full_transfers, 2);
    }

    #[test]
    fn side_codes_and_key() {
        let t = ticket();
        assert_eq!(t.side_codes(true), vec!["SEL", "ICN"]);
        assert_eq!(t.side_codes(false), vec!["MOW", "SVO"]);
        assert_ne!(t.flight_key(), t.virtual_flight(0, "BJS").flight_key());
        let json = serde_json::to_string(&t).unwrap();
        let back: Ticket = serde_json::from_str(&json).unwrap();
        assert_eq!(back, t);
    }
}
