# Travelpayouts / Aviasales API — справочник всех ручек

Снимок справки https://support.travelpayouts.com (категория «API и данные»),
документации для разработчиков https://travelpayouts.github.io/slate/ и
GraphQL-схемы, снятой introspection-запросом. Сохранено 2026-09-27.

- `articles/` — статьи справки в Markdown (русский, актуальнее slate).
- `slate/` — исходники developer-доков (английский, старее; есть ручки, которых нет в справке).
- `graphql-schema.graphql` — полная схема GraphQL Data API (SDL из introspection).

Справка закрыта Cloudflare, но статьи отдаёт Zendesk API:
`https://support.travelpayouts.com/api/v2/help_center/ru/articles/<id>.json`
(список статей категории — `…/ru/categories/200358578/articles.json`).

**Что использует проект:** только GraphQL `prices_one_way` (`core/graphql_api.py`).
Поле `link` в формате REST `prices_for_dates` разбирает `core/linkinfo.py`.

## Авторизация и общие правила

- Токен: заголовок `X-Access-Token` или параметр `token=` (профиль → API-ключ). →
  [faq-api-token](articles/faq-api-token.md)
- Данные REST/GraphQL Data API берутся **из кеша** поисков пользователей Aviasales (хранится 7 дней),
  параметр `market` задаёт маркет кеша (по умолчанию — по `origin`, иначе `ru`). →
  [data-api-rest](articles/data-api-rest.md), [условия доступа](articles/data-api-access-terms.md)
- Лимиты — запросов в минуту на метод, превышение → HTTP 429, остаток — заголовки
  `X-Rate-Limit`, `X-Rate-Limit-Remaining`, `X-Rate-Limit-Reset`. → [limits](articles/limits.md)
- Рекомендуется `Accept-Encoding: gzip, deflate`.

## 1. GraphQL Data API

`POST https://api.travelpayouts.com/graphql/v1/query` (лимит 60/мин), песочница —
`http://api.travelpayouts.com/graphql/v1/playground`. → [graphql-api](articles/graphql-api.md),
[схема](graphql-schema.graphql)

| Запрос | Назначение |
|---|---|
| `prices_one_way(params: ParamsOneWay!, paging, sorting, grouping, currency, market, brand)` | Билеты в одну сторону (используем) |
| `prices_round_trip(params: ParamsRoundTrip!, …)` | Билеты туда-обратно |
| `special_offers_one_way(params: ParamsSpecialOfferOneWay!, …, experimental)` | Спецпредложения в одну сторону |
| `special_offers_round_trip(params: ParamsSpecialOfferRoundTrip!, …, experimental)` | Спецпредложения туда-обратно |
| `weekend_prices_round_trip(params: ParamsWeekendRoundTrip!, …, experimental)` | Цены на поездки на выходные |

## 2. REST Data API (кеш цен)

Все `GET`, хост `https://api.travelpayouts.com`. Лимит — запросов в минуту.

| Ручка | Лимит | Назначение | Документ |
|---|---|---|---|
| `/aviasales/v3/prices_for_dates` | 600 | Самые дешёвые билеты на даты/месяцы (по одному на группу дат) | [data-api-rest](articles/data-api-rest.md) |
| `/aviasales/v3/grouped_prices` | 600 | Дешёвые билеты, сгруппированные (`group_by`: дата вылета/возврата, месяц) | [data-api-rest](articles/data-api-rest.md) |
| `/aviasales/v3/get_latest_prices` | 300 | Цены, найденные за период (`period_type`) | [data-api-rest](articles/data-api-rest.md) |
| `/aviasales/v3/get_special_offers` | 600 | Спецпредложения | [data-api-rest](articles/data-api-rest.md) |
| `/aviasales/v3/get_popular_directions` | 600 | Дешёвые билеты на популярные направления из/в город | [data-api-rest](articles/data-api-rest.md) |
| `/aviasales/v3/search_by_price_range` | 600 | Билеты в ценовом коридоре `value_min`/`value_max` | [data-api-rest](articles/data-api-rest.md) |
| `/v2/prices/month-matrix` | 300 | Календарь цен на месяц | [data-api-rest](articles/data-api-rest.md) |
| `/v2/prices/week-matrix` | 60 | Календарь цен на неделю | [data-api-rest](articles/data-api-rest.md) |
| `/v2/prices/nearest-places-matrix` | 60 | Цены по альтернативным (соседним) направлениям | [data-api-rest](articles/data-api-rest.md) |
| `/v2/prices/latest` | 300 | Цены за период (старый аналог `get_latest_prices`) | [slate/dataapiv2](slate/dataapiv2.md) |
| `/v2/prices/special-offers` | 600 | Спецпредложения (старый аналог, XML) | [slate/dataapiv2](slate/dataapiv2.md) |
| `/v1/prices/cheap` | 300 | Самые дешёвые билеты (0/1/2 пересадки) | [data-api-rest](articles/data-api-rest.md) |
| `/v1/prices/direct` | 180 | Самый дешёвый билет без пересадок | [data-api-rest](articles/data-api-rest.md) |
| `/v1/prices/calendar` | 300 | Билеты на каждый день месяца | [data-api-rest](articles/data-api-rest.md) |
| `/v1/prices/monthly` | 60 | Самые дешёвые билеты по месяцам | [slate/dataapiv1](slate/dataapiv1.md) |
| `/v1/airline-directions` | — | Популярные направления авиакомпании | [data-api-rest](articles/data-api-rest.md) |
| `/v1/city-directions` | 600 | Популярные направления из города | [data-api-rest](articles/data-api-rest.md) |

## 3. Справочники (статические JSON)

`GET https://api.travelpayouts.com/data/<lang>/<file>.json` (лимит 600/мин), `<lang>` — `ru`, `en` и др.
→ [data-dictionaries](articles/data-dictionaries.md), [iata-databases](articles/iata-databases.md),
[slate/dataapijson](slate/dataapijson.md)

| Файл | Содержимое |
|---|---|
| `data/<lang>/countries.json` | Страны (ISO-код, валюта, названия) |
| `data/<lang>/cities.json` | Города (IATA, страна, координаты, часовой пояс) |
| `data/<lang>/airports.json` | Аэропорты (IATA, город, тип, `flightable`) |
| `data/<lang>/airlines.json` | Авиакомпании |
| `data/<lang>/alliances.json` (в slate — `airlines_alliances.json`) | Альянсы |
| `data/planes.json` | Самолёты (не обновляется) |
| `data/routes.json` | Маршруты авиакомпаний (не обновляется) |

## 4. Поиск в реальном времени (Flight Search API)

**Доступ только проектам с подтверждёнными 50 000+ MAU.** Нужна подпись md5
(`signature`), `marker`, реальные IP и User-Agent пользователя. →
[search-api](articles/search-api.md), [доступ](articles/search-api-access.md),
[правила](articles/search-api-rules.md), [подпись](articles/search-api-signature.md)

| Ручка | Метод | Назначение |
|---|---|---|
| `https://tickets-api.travelpayouts.com/search/affiliate/start` | POST | Старт поиска (в т. ч. сложные маршруты) → `search_id`, `results_url` |
| `<results_url>/search/affiliate/results` | POST | Результаты, опрашивать до `is_over = true` (ссылка живёт 15 мин) |
| `<results_url>/searches/<search_id>/clicks/<proposal_id>` | GET | Ссылка перехода на сайт агентства (`click_id`) |
| `//yasen.aviasales.ru/adaptors/pixel_click.png?click_id=…&gate_id=…` | GET | Пиксель учёта перехода |

Старая версия (v1) → [search-api-v1-old](articles/search-api-v1-old.md), [slate/searchapi](slate/searchapi.md):

| Ручка | Метод | Назначение |
|---|---|---|
| `https://api.travelpayouts.com/v1/flight_search` | POST | Старт поиска → `search_id` |
| `https://api.travelpayouts.com/v1/flight_search_results?uuid=<search_id>` | GET | Результаты |
| `https://api.travelpayouts.com/v1/flight_searches/<search_id>/clicks/<terms.url>.json?marker=…` | GET | Ссылка на покупку |

## 5. Вспомогательные сервисы Aviasales

| Ручка | Назначение | Документ |
|---|---|---|
| `GET https://autocomplete.travelpayouts.com/places2?term=&locale=&types[]=` | Автокомплит стран/городов/аэропортов | [autocomplete-api](articles/autocomplete-api.md), [search-api-autocomplete](articles/search-api-autocomplete.md) |
| `GET https://www.travelpayouts.com/widgets_suggest_params?q=` | IATA-коды из поисковой фразы («Из Москвы в Лондон») | [iata-from-phrase-api](articles/iata-from-phrase-api.md), [howto](articles/iata-from-phrase-howto.md) |
| `GET http://www.travelpayouts.com/whereami?locale=&ip=` | Ближайший город по IP | [whereami-api](articles/whereami-api.md) |
| `GET http://map.aviasales.ru/supported_directions.json?origin_iata=&one_way=&locale=` | Направления для карты цен | [price-map-api](articles/price-map-api.md) |
| `GET http://map.aviasales.ru/prices.json?origin_iata=&period=&direct=&one_way=&…` | Цены для карты (фильтры виз, Шенгена, цены) | [price-map-api](articles/price-map-api.md), [mobile-apps-api](articles/mobile-apps-api.md) |
| `GET https://suggest.travelpayouts.com/api_flight_schedule?origin=&destination=&airline=&locale=&service=api_flight_schedule` | Расписание рейсов по маршруту | [slate/additional](slate/additional.md) |
| `GET http://yasen.aviasales.ru/adaptors/currency.json` | Курсы валют к рублю | [data-api-rest](articles/data-api-rest.md) |
| `GET http://pics.avs.io/<w>/<h>/<iata>.png` | Логотип авиакомпании | [logos-and-flags](articles/logos-and-flags.md) |
| `GET http://img.wway.io/pics/root/<iata>@png?exar=1&rs=fit:<w>:<h>` | Логотип авиакомпании (новый CDN) | [logos-and-flags](articles/logos-and-flags.md) |
| `GET http://img.wway.io/pics/as_gates/<gate_id>@png?exar=1&rs=fit:<w>:<h>` | Логотип агентства (гейта) | [search-api](articles/search-api.md) |
| `GET http://ios.aviasales.ru/logos/<density>/<iata>.png` | Логотипы для мобильных | [mobile-apps-api](articles/mobile-apps-api.md) |
| `https://cdn.travelpayouts.com/support/flags.zip` | Флаги стран (eps) | [logos-and-flags](articles/logos-and-flags.md) |

## 6. Партнёрский кабинет Travelpayouts

| Ручка | Метод | Назначение | Документ |
|---|---|---|---|
| `https://api.travelpayouts.com/statistics/v1/get_fields_list` | GET | Поля статистики бронирований | [statistics-api](articles/statistics-api.md) |
| `https://api.travelpayouts.com/statistics/v1/execute_query` | POST | Выборка статистики бронирований (30/мин) | [statistics-api](articles/statistics-api.md) |
| `https://api.travelpayouts.com/finance/v2/get_user_balance` | GET | Текущий баланс | [finance-api](articles/finance-api.md) |
| `https://api.travelpayouts.com/finance/v2/get_user_actions_affecting_balance` | GET | Действия, влияющие на баланс | [finance-api](articles/finance-api.md) |
| `https://api.travelpayouts.com/finance/v2/get_user_next_payout` | GET | Сумма к выплате | [finance-api](articles/finance-api.md) |
| `https://api.travelpayouts.com/finance/v2/get_user_payments` | GET | Список выплат | [finance-api](articles/finance-api.md) |
| `https://api.travelpayouts.com/finance/v2/get_user_actions_affecting_payment?payment_uuid=` | GET | Действия конкретной выплаты | [finance-api](articles/finance-api.md) |
| `https://api.travelpayouts.com/finance/v2/get_action_details?action_id=` | GET | Детали действия | [finance-api](articles/finance-api.md) |
| `https://api.travelpayouts.com/links/v1/create` | POST | Партнёрские ссылки из обычных | [links-api](articles/links-api.md) |
| `/v2/statistics/{sales,detailed-sales,balance,payments}` | GET | Старая статистика (deprecated) | [statistics-api-deprecated](articles/statistics-api-deprecated.md) |

## 7. FAQ и прочее

[faq-aviasales-api](articles/faq-aviasales-api.md) (большой FAQ),
[faq-api-kinds](articles/faq-api-kinds.md), [faq-request-limits](articles/faq-request-limits.md),
[faq-currencies](articles/faq-currencies.md), [faq-languages](articles/faq-languages.md),
[faq-signature](articles/faq-signature.md), [faq-site-requirements](articles/faq-site-requirements.md),
[faq-iata-code](articles/faq-iata-code.md), [faq-what-is-api](articles/faq-what-is-api.md),
FAQ поиска: `articles/search-api-faq-*.md`, клиентские библиотеки — [useful-libraries](articles/useful-libraries.md).

## Не сохранено (API сторонних брендов)

В той же категории есть API/фиды партнёров — к авиабилетам не относятся, при необходимости
брать по id через Zendesk API: отели (Яндекс Путешествия 19677424987026, Суточно.ру 4407224907282),
трансферы и ж/д (intui.travel 360016804119, GetTransfer 360016375920, Tutu.ru 360020147791 / 115001440551),
туры (Tezeks, Level.travel, Travelata), экскурсии (YouTravel.me, Большая Страна, Tiqets, Sputnik8,
WeGoTrip, Трипстер), Omio 360024389872, eSIM Airalo 17131439719826; общий список брендов — 20384016664594.
