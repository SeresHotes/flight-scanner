# API карты цен

> Источник: https://support.travelpayouts.com/hc/ru/articles/203755406 (обновлено 2024-02-03, сохранено 2026-09-27)

В данной статье находится информация о поддерживаемых направлениях и ценах в API карты цен.         

**Важно!** С 30 мая 2022 прекращается поддержка описанных в статье методов. Рекомендуем заранее перейти на альтернативные варианты.

Вместо **/supported_directions.json** используйте GQL-запрос **prices_one_way** ([документация](https://support.travelpayouts.com/hc/ru/articles/4417975783314-GraphQL-API-%D0%B4%D0%BB%D1%8F-%D0%B4%D0%BE%D1%81%D1%82%D1%83%D0%BF%D0%B0-%D0%BA-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%BC-%D0%BF%D0%BE-%D0%B0%D0%B2%D0%B8%D0%B0%D0%B1%D0%B8%D0%BB%D0%B5%D1%82%D0%B0%D0%BC)). Пример запроса:

    {
      prices_one_way(
        params: {
          origin: "MOW"
        }
        grouping: DIRECTIONS
        paging: {
          offset: 0
          limit: 30
        }
      ) {
        destination_city_iata
      }
    }

В ответе нет полей **country** и **coordinates**, вы можете воспользоваться методом /cities.json ([документация](https://support.travelpayouts.com/hc/ru/articles/360018907280-%D0%94%D0%B0%D0%BD%D0%BD%D1%8B%D0%B5-%D0%B2-JSON-%D1%84%D0%BE%D1%80%D0%BC%D0%B0%D1%82%D0%B5#chapter_1)) для получениях этих данных. Максимально возможное значение limit — 30 записей, используйте offset, чтобы получить все данные.

Вместо **/prices.json** используйте **/aviasales/v3/grouped_prices** ([документация](https://support.travelpayouts.com/hc/ru/articles/203956163-Aviasales-API-%D0%B4%D0%BE%D1%81%D1%82%D1%83%D0%BF%D0%B0-%D0%BA-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%BC-%D0%B4%D0%BB%D1%8F-%D1%83%D1%87%D0%B0%D1%81%D1%82%D0%BD%D0%B8%D0%BA%D0%BE%D0%B2-%D0%BF%D0%B0%D1%80%D1%82%D0%BD%D1%91%D1%80%D1%81%D0%BA%D0%BE%D0%B9-%D0%BF%D1%80%D0%BE%D0%B3%D1%80%D0%B0%D0%BC%D0%BC%D1%8B#chapter_2)). Чтобы получить значения расстояний, используйте GraphQL-запроc с указанием поля *distance*:

    {
      prices_one_way(
        params: {
          origin: "MOW"
        }
        grouping: DIRECTIONS
        paging: {
          offset: 0
          limit: 30
        }
      ) {
        destination_city_iata
        distance
      }
    }

## Поддерживаемые направления

Запрос

Метод: GET.

URL: <http://map.aviasales.ru/supported_directions.json?origin_iata=LED&one_way=false&locale=ru>

**Параметры запроса**

- **origin_iata** — IATA-код аэропорта/города, из которого ищутся билеты. IATA-код указывается буквами верхнего регистра, например MOW.
- **one_way** — «true» для перелетов в одну сторону, «false» — для туда-обратно.
- **locale** — язык поиска.

Ответ

    {
    "origin":{
        "iata":"LED",
        "name":"Санкт-Петербург",
        "country":"RU",
        "coordinates":[30.315785,59.939039]
        },
    "directions":[{
        "direct":false,
        "iata":"AAL",
        "name":"Ольборг",
        "country":"DK",
        "coordinates":[9.917771,57.028811]
        },
        {
        "direct":true,
        "iata":"AAQ",
        "name":"Анапа",
        "country":"RU",
        "coordinates":[37.316666,44.9]
        }   
    }

**Параметры ответа**

- **iata** — IATA-код города отправления.
- **name** — название города отправления.
- **country** — буквенный код страны.
- **coordinates** — координаты города отправления.
- **directions** — информация о направлении.
  - **direct** — указывает на наличие перелётов без пересадок.
  - **name** — название города прибытия.
  - **country** — страна прибытия.
  - **coordinates** — координаты.

## Цены

Запрос

Метод: GET.

URL: <http://map.aviasales.ru/prices.json?origin_iata=LED&period=2017-02-01:season&direct=true&one_way=false&no_visa=true&schengen=true&need_visa=true&locale=ru&min_trip_duration_in_days=13&max_trip_duration_in_days=15>

**Параметры запроса**

- **origin_iata** — IATA-код пункта отправления. IATA-код указывается буквами верхнего регистра, например, MOW.
- **period** — период дат для поиска:
  - **month** — отображать только перелёты в указанном месяце (необходимо указывать дату начала месяца, например, 2017-01-01);
  - **season** — отображать перелёты, входящие в сезон, указанного месяца (зима, весна, лето, осень);
  - **year** — весь год (дату можно не указывать).
- **direct** — указывает на наличие перелётов без пересадок.
- **one_way** — «true» для перелетов в одну сторону, «false» для туда-обратно.
- **no_visa** — виза для русских не нужна.
- **schengen** — нужна виза в шенген.
- **need_visa** — нужна виза для русских.
- **locale** — язык поиска.
- **min_trip_duration_in_days** — минимальная продолжительность поездки (дней).
- **max_trip_duration_in_days** — максимальная продолжительность поездки (дней).

Ответ

    [{
       "show_to_affiliates":false,
       "trip_class":0,
       "destination":"PSA",
       "depart_date":"2014-05-30",
       "return_date":"2014-06-12",
       "number_of_changes":0,
       "value":43785,
       "created_at": 1407716354,
       "ttl": 1407889154,
       "distance":2247,
       "actual":false
    }]

**Параметры ответа**

- **show_to_affiliates** — true: цена доступна партнёрам, false: цена недоступна партнёрам.
- **trip_class** — класс путешествия (1 — бизнес-класс, 0 — стандарт).
- **destination** — IATA-код пункта назначения.
- **depart_date** — дата отправления.
- **return_date** — дата возвращения.
- **number_of_changes** — количество пересадок.
- **value** — стоимость перелёта.
- **created_at** — время, когда цена была найдена (unix timestamp)\*.
- **ttl** — время, до которого действует предложение (unix timestamp)\*.
- **distance** — расстояние между пунктом отправления и прибытия.
- **actual** — принимает значения 0 или 1. 0 — цена была найдена давно и могла устареть. 1 — цена была найдена недавно и является актуальной.

\* — При переводе с помощью php date(«c», «значение UNIX-time») берётся время сервера, и поэтому не учитываются часовые пояса. Поэтому требуется учитывать время сервера или принудительно приводить его к времени Гринвича с помощью функции date_default_timezone_set('GMT').
