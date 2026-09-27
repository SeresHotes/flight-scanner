# API Авиасейлс для поиска авиабилетов: старая версия

> Источник: https://support.travelpayouts.com/hc/ru/articles/203956173 (обновлено 2026-06-10, сохранено 2026-09-27)

Всё о старой версии API поиска, который позволяет получать ответы в реальном времени.                                                                                                                                

Вы читаете статью о старой версии API поиска.

С 1 ноября 2025 года доступна [новая версия](https://support.travelpayouts.com/hc/ru/articles/30565016140434), которую мы рекомендуем для всех новых интеграций. Подробнее — здесь. Старая версия перестанет работать 15 июня 2026 года.

API поиска билетов от Aviasales позволяет получать результаты запросов в режиме **реального времени**, а также строить сложные маршруты.

Уважаемые партнёры! Обратите внимание, что по умолчанию установлено ограничение: **200 запросов в час** с одного пользовательского IP-адреса. Если требуется обрабатывать больше запросов, напишите на <support@travelpayouts.com>.

## Условия использования API для поиска авиабилетов

1.  Каждый поисковый запрос должен быть инициирован пользователем, результат запроса должен быть показан пользователю в полном объёме. В результатах каждого запроса должна быть кнопка «купить» рядом с каждым найденным вариантом.
2.  Ссылка для перехода на агентство может быть сгенерирована ТОЛЬКО после нажатия на кнопку «Купить» пользователем. Автоматическая генерация данной ссылки запрещена.
3.  Коэффициент конверсии поисков в клики по кнопке «Купить» не должен быть ниже 9%. Конверсия кликнувших «Купить» в покупателей — не ниже 5%.
4.  Запрещено использовать запросы с localhost IP адресами (127.0.0.1-127.255.255.255).
5.  **Ajax-запросы к API не работают**, так как в открытом виде передаётся ключ доступа. Необходимо делать запросы к API поискового сервиса авиабилетов с сервера, на клиенте сделать это не получится.

Сервис поиска авиабилетов не включает API бронирования авиабилетов. Покупка билетов осуществляется на сайтах агентств и авиакомпаний. Подробнее про условия использования и получение доступа к API поиска авиабилетов смотрите [здесь](https://support.travelpayouts.com/hc/ru/articles/210995808).

## Поисковая логика

1.  Формируете запрос с направлением перелёта и количеством пассажиров.
2.  Отправляете запрос и получаете уникальный ID поиска.
3.  Отправляете ID поиска, чтобы получить данные о рейсах и стоимость билетов.
4.  С помощью полученных данных формируете поисковую страницу.
5.  При клике на кнопку **Купить** генерируете ссылку на сайт агентства/авиакомпании.

## Запросы

### Туда-обратно (round trip)

#### Инициализация поиска

**Метод: POST**

**Параметры запроса**

- **marker** (string) — партнёрский маркер — это уникальный ID, по которому мы понимаем от какого партнёра пришел пользователь (**находится в личном кабинете партнёра**);\
  ![](https://support.travelpayouts.com/hc/article_attachments/18177047266066)

&nbsp;

- **host** (string) — хост автора запроса (**необходимо заменить на адрес вашего сайта или название приложения, в котором используется** **API**);
- **user_ip** (string) — IP-адрес пользователя (запрещено указывать localhost IP-адреса);
- **locale** (string) — язык, на котором отобразится результат запроса (от локали, города вылета и прилёта зависит, по каким агентствам осуществляется поиск). Доступные языки:
  - en (en-us) — американский английский;
  - en-gb — британский английский;
  - ru — русский;
  - de — немецкий;
  - es — испанский;
  - fr — французский;
  - it — итальянский;
  - pl — польский;
  - th — тайский.
- **trip_class** (string) — класс перелёта (Y — эконом, C — бизнес. Обязательно в верхнем регистре);
- **passengers** — информация о пассажирах;
  - **adults** (integer) — количество взрослых;
  - **children** (integer) — количество детей (возраст от 2 до 12 лет);
  - **infants** (integer) — количество младенцев (возраст до 2 лет).
- **segments**
  - **origin** (string) — IATA-код пункта отправления. IATA-код указывается буквами верхнего регистра, например, MOW;
  - **destination** (string) — IATA-пункта назначения. IATA-код указывается буквами верхнего регистра, например, MOW;
  - **date** (string) — дата отправления.
- **know_english **(string) (необязательный параметр) — параметр, отвечающий за наличие англоязычных гейтов в выдаче. Принимаемые значения: true (в случае, если надо отобразить пользователю англоязычные гейты), false (по умолчанию false);
- **signature** (string) — md5-подпись запроса, которая получается из сгруппированного в алфавитном порядке списка параметров запроса. Узнайте, [как сформировать signature](https://support.travelpayouts.com/hc/ru/articles/210996008)*.*

Чтобы **получить варианты билетов «Туда-обратно»**, необходимо добавить следующий код в body запроса:

```
{
  "signature":"55b47124341b5bed8627499b0eb5de80",
  "marker":"ВашМаркер",
  "host":"%server_host%",
  "user_ip":"%user_ip_address%",
  "locale":"ru",
  "trip_class":"Y",
  "passengers":{
    "adults":1,
    "children":0,
    "infants":0
  },
  "segments":[
  {
    "origin":"NYC",
    "destination":"LAX",
    "date":"2017-11-25"
  },
  {
    "origin":"LAX",
    "destination":"NYC",
    "date":"2017-12-18"
  }
  ]
}
```

Для получения данных запустите код инициализации поиска (**обратите внимание,** что в примере указаны параметры **marker, host, user_ip**, которые **необходимо заменить на ваши**, описание параметров см. выше):

```
curl -v -X POST -d '{"signature":"deb5b02159898a6ab6f120624fa2f72c","marker":"ВставьтеСюдаВашМаркер","host":"beta.aviasales.ru","user_ip":"127.0.0.1","locale":"ru","trip_class":"Y","passengers":{"adults":1,"children":0,"infants":0},"segments":[{"origin":"NYC","destination":"LAX","date":"2017-11-25"},{"origin":"LAX","destination":"NYC","date":"2017-12-18"}]}' -H 'Content-type:application/json' http://api.travelpayouts.com/v1/flight_search
```

В результате будет получен ответ в формате JSON. Ответ содержит следующие параметры:

- **locale** — локаль, на языке которой отображается результат поиска;
- **search_id** — уникальный идентификатор поиска, используется для запроса результатов поиска;
- **geoip_city** — geoip города, из которого сделан запрос;
- **trip_class** — класс перелёта;
- **affiliate** — id партнёра;
- **marker** — маркер, с которым был сделан поиск;
- **user_ip** — ip-адрес пользователя;
- **gates_count** — общее кол-во агентств;
- **segments** — список составляющих перелёта:
  - **date** — дата вылёта;
  - **origin** — IATA-код пункта отправления;
  - **destination** — IATA-пункта назначения;
- **meta** — техническая информация;
  - **uuid** — уникальный идентификатор запроса;
- **passengers** — информация о пассажирах:
  - **adults** — количество взрослых;
  - **infants** — количество младенцев;
  - **children** — количество детей;
- **host** — хост, откуда был получен запрос;
- **currency_rates** — курс валют по отношению к рублю.\
  **Обратите внимание!** Используйте currency_rates, чтобы перевести цены на рейсы в нужную вам валюту (так как в ответе указана цена перелета в российских рублях).
- **geoip_country** — geoip страны, из которого сделан запрос;
- **banner_info **— служебная информация (просто игнорируйте данный блок);
- **preroll_question** — блок с вопросами, которые отображаются при поиске на сайте aviasales (просто игнорируйте данный блок).

#### Получение результатов поиска

**Метод: GET**

В теле ответа содержится параметр search_id, который необходимо вставить в URL:

`https://api.travelpayouts.com/v1/flight_search_results?uuid=%search_id%`

И отправить запрос на сервер за результатами поиска:

`curl -v -H 'Accept-Encoding:gzip,deflate,sdch' https://api.travelpayouts.com/v1/flight_search_results?uuid=ebe4fa71-bc07-40df-ae4e-4b72116583da --compressed`

где «ebe4fa71-bc07-40df-ae4e-4b72116583da» — это и есть search_id.

В результате будут получены данные в виде JSON массива, где каждый proposal-элемент — это билет от конкретного агентства. Описание ответа см. [ниже](https://support.travelpayouts.com/hc/ru/articles/203956173#04).

Чтобы получить все данные, запрос необходимо повторять, пока в ответе не придёт ассоциативный массив с одним элементом — **search_id**. Периодичность отправки запросов никак не ограничена.

**Обратите внимание**! На сбор данных нашим сервером требуется время (от 30 до 60 секунд). Вы можете сделать задержку после получения параметра search_id и уведомить пользователя, что идет загрузка — так сделано на сайте aviasales.ru. Или выводить данные на странице, по мере их получения от нас.

### Билеты в одну сторону

#### Инициализация поиска

**Метод: POST**

Поиск билетов в одну сторону происходит по аналогичной схеме с поиском «Туда-обратно». Единственное отличие — это содержимое JSON-запроса. Для инициализации поиска необходимо поместить следующий JSON в body запроса:

```
{
    "marker": "ВставьтеСюдаВашМаркер",
    "host": "beta.aviasales.ru",
    "user_ip": "127.0.0.1",
    "locale": "ru",
    "trip_class": "Y",
    "passengers": 
        "adults": 1,
        "children": 0,
        "infants": 0
    },
    "segments": [
    {
        "origin": "MOW",
        "destination": "LED",
        "date": "2017-12-18"
    }]
}
```

Инициируйте поиск, используя приведенный выше запрос:

```
curl -v -X POST -d '{"signature":"2a3e1bda117d113569bb6f8b60dba075","marker":"ВставьтеСюдаВашМаркер","host":"beta.aviasales.ru","user_ip":"127.0.0.1","locale":"ru","trip_class":"Y","passengers":{"adults":1,"children":0,"infants":0},"segments":[{"origin":"MOW","destination":"LED","date":"2017-12-18"}]}' -H 'Content-type:application/json' http://api.travelpayouts.com/v1/flight_search
```

### Получение результатов поиска

**Метод: GET**

В теле ответа содержится параметр search_id, который нужно поместить в URL:

`https://api.travelpayouts.com/v1/flight_search_results?uuid=%search_id%`

После чего отправить запрос на сервер за результатами поиска:

`curl -v -H 'Accept-Encoding:gzip,deflate,sdch' https://api.travelpayouts.com/v1/flight_search_results?uuid=%search_id% --compressed`

В результате будут получены данные в виде JSON массива, где каждый proposal-элемент — это билет от конкретного агентства. Описание ответа см. [ниже](https://support.travelpayouts.com/hc/ru/articles/203956173#04).

Чтобы получить все данные, запрос необходимо повторять, пока в ответе не придёт ассоциативный массив с одним элементом — **search_id**. Периодичность отправки запросов никак не ограничена.

**Обратите внимание**! На сбор данных нашим сервером требуется время (от 30 до 60 секунд). Вы можете сделать задержку после получения параметра search_id и уведомить пользователя, что идет загрузка — так сделано на сайте aviasales.ru. Или выводить данные на странице, по мере их получения от нас.

### Составной маршрут

#### Инициализация поиска

**Метод: POST**

Составные маршруты — это маршруты с несколькими последовательными перелетами из одного города в другой. Например, Москва — Санкт-Петербург — Берлин — Лондон.

Для инициализации поиска необходимо поместить данный JSON в body запроса:

```
{
    "signature":"64dd4ff2f8a6f690b69d6a72429bf827",
    "marker":"ВставьтеСюдаВашМаркер",
    "host":"beta.aviasales.ru",
    "user_ip":"127.0.0.1",
    "locale":"ru",
    "trip_class":"Y",
    "passengers":{
        "adults":1,
        "children":0,
        "infants":0
    },
    "segments":[
    {
        "origin":"MOW",
        "destination":"LED",
        "date":"2017-12-18"
    },
    {
        "origin":"LED",
        "destination":"BER",
        "date":"2017-12-25"
    },
    {
        "origin":"BER",
        "destination":"LON",
        "date":"2017-01-05"
    }]
}
```

После чего инициируйте поиск:

```
curl -v -X POST -d '{"signature":"08caa94413dc805880f835ba9d8ba98f","marker":"ВставьтеСюдаВашМаркер","host":"beta.aviasales.ru","user_ip":"127.0.0.1","locale":"ru","trip_class":"Y","passengers":{"adults":1,"children":0,"infants":0},"segments":[{"origin":"MOW","destination":"LED","date":"2017-11-18"},{"origin":"LED","destination":"BER","date":"2017-12-25"},{"origin":"BER","destination":"LON","date":"2017-01-05"}]}' -H 'Content-type:application/json' http://api.travelpayouts.com/v1/flight_search
```

#### Получение результатов поиска

**Метод: GET**

В теле ответа содержится параметр search_id. Поместите его в URL:

`https://api.travelpayouts.com/v1/flight_search_results?uuid=%search_id%`

И отправьте запрос на сервер за результатами поиска:

`curl -v -H 'Accept-Encoding:gzip,deflate,sdch' https://api.travelpayouts.com/v1/flight_search_results?uuid=%search_id% --compressed`

В результате будут получены данные в виде JSON массива, где каждый proposal-элемент — это билет от конкретного агентства. Описание ответа см. [ниже](https://support.travelpayouts.com/hc/ru/articles/203956173#04).

Чтобы получить все данные, запрос необходимо повторять, пока в ответе не придёт ассоциативный массив с одним элементом — **search_id**. Периодичность отправки запросов никак не ограничена.

**Обратите внимание**! На сбор данных нашим сервером требуется время (от 30 до 60 секунд). Вы можете сделать задержку после получения параметра search_id и уведомить пользователя, что идет загрузка — так сделано на сайте aviasales.ru. Или выводить данные на странице, по мере их получения от нас.

## Содержимое ответа API поиска

### Пример ответа

**Обратите внимание!** Ссылка на результаты поиска действует 15 минут. По истечению этого времени поисковый запрос нужно отправлять заново.

```
[{
    "segments":[{
        "destination_country":"RU",
        "original_destination":"LED",
        "origin":"MOW",
        "origin_country":"RU",
        "original_origin":"MOW",
        "date":"2017-11-25",
        "destination":"LED"
    },
    {
        "destination_country":"RU",
        "original_destination":"MOW",
        "origin":"LED",
        "origin_country":"RU",
        "original_origin":"LED",
        "date":"2017-12-18",
        "destination":"MOW"
    }],
    "banner_info":{
    },
    "internal":false,
    "airports":{
        "LED":{
            "name":"Пулково",
            "time_zone":"Europe/Moscow",
            "country":"Россия",
            "rates":"259",
            "city":"Санкт-Петербург"
        },
        "DME":{
            "name":"Домодедово",
            "time_zone":"Europe/Moscow",
            "country":"Россия",
            "rates":"392",
            "city":"Москва"
        },
        "SVO":{
            "rates":"307",
            "country":"Россия",
            "name":"Шереметьево",
            "average_rate":"3.63",
            "city":"Москва",
            "time_zone":"Europe/Moscow"
        },
        "VKO":{
            "name":"Внуково",
            "time_zone":"Europe/Moscow",
            "country":"Россия",
            "rates":"211",
            "city":"Москва"
        }},
        "gates_info":{
            "20":{
                "average_rate":4.43,
                "rates":2967,
                "currency_code":"rub",
                "is_airline":false,
                "label":"OneTwoTrip",
                "airline_iatas":{
                },
                "payment_methods":[
                    "card"
                ],
                "mobile_version":false
             }},
        "city_distance":634,
        "meta":{
            "uuid":"c9c6de8c-3fb4-404e-b88c-e9e0a605f183",
            "gates":[{
                "duration":22.402996063232422,
                "id":20,
                "count":1456,
                "good_count":1456
             }]},
        "airlines":{
        "SU":{
           "average_rate":"4.01",
           "rates":"2362",
           "alliance_name":"SkyTeam",
           "name":"Аэрофлот",
           "id":10
        },
        "UN":{
           "average_rate":"3.98",
           "rates":"2639",
           "alliance_name":null,
           "name":"Трансаэро",
           "id":492
        },
        "S7":{
           "average_rate":"3.95",
           "rates":"2430",
           "alliance_name":"OneWorld",
           "name":"S7",
           "id":444
        },
        "UT":{
           "rates":"1843",
           "alliance_name":null,
           "name":"ЮТэйр",
           "id":507
        },
        "U6":{
           "average_rate":"3.62",
           "rates":"1158",
           "alliance_name":null,
           "name":"Уральские авиалинии",
           "id":503
        }},
      "filters_boundary":{
         "arrival_datetime_0":{
            "min":1418869200,
            "max":1418950500
         },
      "arrival_datetime_1":{
         "min":1419486600,
         "max":1419554400
      },
      "flights_duration":{
         "min":70,
         "max":120
      },
      "stops_count":{
         "0":4431
      },
      "departure_time_1":{
         "min":"04:30",
         "max":"22:55"
      },
      "departure_time_0":{
         "min":"00:50",
         "max":"23:30"
      },
      "departure_minutes_0":{
         "min":50,
         "max":1410
      },
      "departure_minutes_1":{
         "min":270,
         "max":1375
      },
      "price":{
         "min":4431,
         "max":8689
      }},
   "flight_numbers":[
   [
      "SU30",
      "SU32"
   ],
   [
      "SU31",
      "SU33"
   ]],
   "proposals":[{
      "terms":{
         "20":{
         "url":2000000,
         "unified_price":5867,
         "price":5867,
         "currency":"rub"
      }},
   "sign":"7573ea707c2ff84b243241961412ed34",
   "segment":[{
      "flight":[{
         "arrival":"LED",
         "aircraft":"AIRBUS A320",
         "local_departure_timestamp":1434588600,
         "operating_carrier":"UT",
         "operated_by":"S7",
         "duration":85,
         "local_arrival_timestamp":1432549200,
         "departure_date":"2016-11-25",
         "departure_time":"11:00",
         "departure_timestamp":1608810900,
         "arrival_date":"2016-11-25",
         "arrival_time":"12:25",
         "arrival_timestamp":1608813600,
         "delay":0,
         "departure":"VKO",
         "equipment":"E70",
         "marketing_carrier":"",
         "rating":7.786111111111111,
         "technical_stops":null,
         "trip_class":"Y"
         "number":369
   }]},
   {
      "flight":[{
         "aircraft":"Boeing 737-800 (winglets)",
         "arrival":"SVO",
         "arrival_date":"2020-12-24",
         "arrival_time":"07:30",
         "arrival_timestamp":1608784200,
         "delay":0,
         "departure":"BAX",
         "departure_date":"2020-12-24",
         "departure_time":"06:55",
         "departure_timestamp":1608767700,
         "duration":275,
         "equipment":"73H",
         "local_arrival_timestamp":1608795000,
         "local_departure_timestamp":1608792900,
         "marketing_carrier":"SU",
         "number":"1431",
         "operating_carrier":"SU",
         "operated_by":"SU",
         "rating":8.23125,
         "technical_stops":null,
         "trip_class":"Y"
   }],
     "rating":{
         "total":9.45,
         "detailed":{
            "arrival_time":9.375,
            "departure_time":7.875000000000001,
            "transfer":10
      }
   },
    "transfers":[{
      "at":"OVB",
      "to":"OVB",
      "airports":[
          "OVB"
      ],
      "airlines":[
          "S7"
      ],
      "country_code":"RU",
      "city_code":"OVB",
      "visa_rules":{
          "required":false
      },
      "night_transfer":false,
      "duration_seconds":3600,
      "duration":{
         "seconds":3600
      }}]
   }]},
   {
      "terms":{
         "20":{
            "url":2000001,
            "unified_price":6953,
            "price":6953,
            "currency":"rub"
   }]}],
      "validating_carrier": "SU"
   }],
   "_ga":null,
   "signature":"4bb635b9353a1e933907d41841e23414",
   "search_id":"c9c6de8c-3fb4-404e-b88c-e9e0a605f183"
   },
   {
   "search_id":"c9c6de8c-3fb4-404e-b88c-e9e0a605f183"
}]
```

Полный пример ответа: [response-example.json](https://support.travelpayouts.com/hc/article_attachments/5007180197778).

Ответ содержит следующие данные:

- **search_id** — идентификатор поиска;
- **flight_numbers** — номера рейсов;
- **uuid** — идентификатор поиска;
- **city_distance** — расстояние между городами отправления и назначения;
- **gates_info** — информация об агенте, продавце билетов. Цифра — id гейта. 
  - **gates_info.\[id\].currency_code** — код валюты оплаты;
  - **gates_info.\[id\].is_airline** — true если билет продает авиакомпания;
  - **gates_info.\[id\].average_rate** — средний рейтинг агентства;
  - **gates_info.\[id\].rates** — рейтинг агентства (кол-во проголосовавших);
  - **gates_info.\[id\].mobile_version** — наличие мобильной версии сайта;
  - **gates_info.\[id\].airline_iatas** — IATA код авиакомпании, если билеты продает она сама;
  - **gates_info.\[id\].payment_methods** — способы оплаты;
  - **gates_info.\[id\].working_hours** — дни и часы работы агентства;
  - **gates_info.\[id\].email** — адрес электронной почты (**deprecated**);
  - **gates_info.\[id\].phone** — номер телефона (**deprecated**);
  - **gates_info.\[id\].site** — сайт агентства;
  - **gates_info.\[id\].helplink** — ссылка на сайт с помощью;
  - **gates_info.\[id\].label** — название агентства.
- **signature** — сигнатура запроса;
- **segments** — массив данных о перелетах (**deprecated**):
  - **segments.destination_country** — код страны назначения;
  - **segments.original_destination** — код города назначения;
  - **segments.origin** — IATA-код пункта отправления;
  - **segments.destination** — IATA-пункта назначения;
  - **segments.date** — дата вылета.
- **flight_numbers** — номера рейсов;
- **airlines** — информация об авиакомпании:
  - **airlines.deeplink_site_name** — адрес сайта авиакомпании;
  - **airlines.id** — идентификационный номер;
  - **airlines.site_name** — название сайта авиакомпании;
  - **airlines.alliance_name** — альянс, в который входит авиакомпания;
  - **airlines.average_rate** — средний рейтинг;
  - **airlines.rates** — количество оценок;
  - **airlines.deeplink_id** — id ссылки на сайт авиакомпании;
  - **airlines.name** — название.
- **proposals** — массив найденных вариантов;
  - **proposals.segment** — сегменты перелета;
    - **proposals.segment.flight** — информация о рейсе:
      - proposals.segment.flight.**departure** — IATA код аэропорта отправления;
      - proposals.segment.flight.**duration** — продолжительность перелета в минутах;
      - proposals.segment.flight.**departure_date** — дата отправления;
      - proposals.segment.flight.**departure_time** — время отправления по местному времени;
      - proposals.segment.flight.**departure_timestamp** — время отправления в UNIX формате;
      - proposals.segment.flight.**local_departure_timestamp** — местное время отправления в UNIX формате;
      - proposals.segment.flight.**arrival_date** — дата прибытия;
      - proposals.segment.flight.**arrival_time** — время прибытия по местному времени;
      - proposals.segment.flight.**arrival_timestamp** — время прибытия в UNIX формате;
      - proposals.segment.flight.**local_arrival_timestamp** — местное время прибытия в UNIX формате;
      - proposals.segment.flight.**aircraft** — тип самолета;
      - proposals.segment.flight.**number** — номер рейса (строковое значение). Чтобы сформировать полный номер рейса вида КодАвиакомпанииНомерРейса, используйте параметры **operating_carrier **и **number**;
      - proposals.segment.flight.**delay** — время между рейсами (время ожидания в аэропорту) в минутах;
      - proposals.segment.flight.**marketing_carrier **— IATA код авиакомпании, которая продаёт билет. Используйте, чтобы сформировать номер рейса. Если данное поле пустое, используйте operating_carrier;
      - proposals.segment.flight.**operating_carrier** — IATA код авиакомпании, выполняющей перевозку;
      - proposals.segment.flight.**is_bus, flight.is_train** — true, если в этом сегменте передвигаются не самолетом, а автобусом или поездом; 
      - proposals.segment.flight.**technical_stops** — IATA код аэропорта технической остановки;
      - proposals.segment.flight.**arrival** — IATA код аэропорта прибытия.
    - **proposals.segment.rating** — внутренняя информация о рейтинге полетов;
    - **proposals.segment.transfers** — информация о пересадке:
      - proposals.segment.transfers.**at** — IATA код аэропорта, в который прилетает самолёт для пересадки;
      - proposals.segment.transfers.**to** — IATA код аэропорта, из которого вылетает самолёт;
      - proposals.segment.transfers.**airports** — список IATA кодов аэропортов пересадки;
      - proposals.segment.transfers.**airlines** — список IATA кодов авиакомпаний, участвующих в перелёте;
      - proposals.segment.transfers.**country_code** — IATA код страны пересадки;
      - proposals.segment.transfers.**city_code** — IATA код города пересадки;
      - proposals.segment.transfers.**visa_rules** — требуется ли виза для российских граждан для пересадки;
      - proposals.segment.transfers.**night_transfer** — является ли ночной пересадкой;
      - proposals.segment.transfers.**duration_seconds** — продолжительность пересадки в секундах;
      - proposals.segment.transfers.**duration** — **deprecated**.
  - **proposals.carriers **— iata код авиакомпаний перевозчиков;
  - **proposals.terms** — информация о стоимости перелета (цифра — id гейта):
    - proposals.**terms.currency** — валюта, в которой указана оригинальная цена;
    - proposals.**terms.price** — цена перелета в валюте гейта (указана в поле **terms.currency**);
    - proposals.**terms.unified_price** — цена перелета в базовой валюте (в рублях);
    - proposals.**terms.url** — код для формирования ссылки для покупателей (как формировать ссылку [см. ниже](#05));
    - proposals.**terms.flights_baggage** — количество мест багажа и его вес. Принимает одно из значений:
      - terms.flights_baggage.**flights_baggage.""** — нет информации о багаже;
      - terms.flights_baggage.**flights_baggage.false** — багаж не включен в стоимость;
      - terms.flights_baggage.**flights_baggage.0PC** — нет багажа;
      - terms.flights_baggage.**flights_baggage.{int}PC{int}** — количество сумок по %somevalue% килограмм. Например, 2PC23 означает два багажных места по 23 кг.
      - terms.flights_baggage.**flights_baggage.{int}PC** — количество мест багажа без указания информации о весе. Например, 1PC означает одно багажное место.
      - terms.flights_baggage.**flights_baggage.{int}** — количество сумок не имеет значения, ограничивается суммарная масса.
    - proposals.**terms.flights_handbags** — информация о ручной клади.
  - **proposals.xterms** — содержит информацию аналогичную **terms**, плюс может содержать дополнительные данные о тарифах (если она передаётся от агентств/авиакомпаний). 
    - proposals**.xterms.flight_additional_tariff_infos** – информация о возможности возврата и обмена билетов. Принимаемые значения: true (доступен) и false (недоступен);
      - proposals.xterms.flight_additional_tariff_infos.**return_before_flight** — возврат до вылета;
      - proposals.xterms.flight_additional_tariff_infos.**return_after_flight** — возврат после вылета;
      - proposals.xterms.flight_additional_tariff_infos.**change_before_flight** — обмен после вылета;
      - proposals.xterms.flight_additional_tariff_infos.**change_after_flight** — обмен после вылета;
  - **proposals.is_direct** — true, если перелет без пересадок;
  - **proposals.segments_airports** — IATA коды основных аэропортов отправления и назначения;
  - **proposals.stops_airports** — IATA коды аэропортов отправления, назначения и пересадок;
  - **proposals.is_charter** — является ли рейс чартерным (false или true);
  - **proposals.segment_durations** — продолжительность перелетов (например, туда и обратно);
  - **proposals.total_duration** — общее время перелета;
  - **proposals.max_stops** — максимальное количество остановок;
  - **proposals.sign** — уникальный id билета, для объединения информации от разных агентств в один билет.
- **meta**:
  - **gates** — информация о ходе опроса агентств в процессе поиска;
    - **gates.tos** — содержимое ошибки.
    - **gates.good_count** — количество подходящих билетов (если есть билеты с неверной датой, то они отфильтровываются);
    - **gates.count** — количество билетов от агентства;
    - **gates.duration** — время запроса;
    - **gates.id** — ID агентства;
    - **gates.error** — информация об ошибке;
    - **gates.uuid** — ID запроса;
- **airports** — данные об аэропортах;
  - **airports.city** — город, в котором находится аэропорт;
  - **airports.city_code** — IATA код города;
  - **airports.average_rate** — средний рейтинг аэропорта;
  - **airports.rates **— количество оценок;
  - **airports.country** — страна, в которой находится аэропорт;
  - **airports.name** — название аэропорта;
  - **airports.time_zone** — часовой пояс аэропорта;
  - **airports.IATA** — IATA код аэропорта.
- **market** — рынок маршрутов (**deprecated**);
- **initiated_at** — дата и время поиска (**deprecated**);
- **open_jaw** — верно, если это часть составного маршрута (**deprecated**);
- **clean_marker** — партнёрский маркер (**deprecated**);
- **currency** — тип валюты (**deprecated**);
- **filters_boundary** — массив с данными для фильтрации (**deprecated**):
  - **filters_boundary.stops_duration** — время между рейсами (максимальное и минимальное);
  - **filters_boundary.flights_duration** — время перелета (максимальное и минимальное среди всех перелетов);
  - **filters_boundary.arrival_datetime_0** — время прибытия (максимальное и минимальное). Здесь 0, 1 в конце переменной означают номер перелёта (0 - туда, 1 - обратно);
  - **filters_boundary.price** — цена перелета (максимальное и минимальное);
  - **filters_boundary.departure_time_0** — время отправления (максимальное и минимальное);
  - **filters_boundary.stops_count** — количество пересадок от указанной минимальной стоимости.
- **validating_carrier** — IATA код основной авиакомпании (**deprecated**).

**Совет: как определить аэропорт вылета и назначения.**

Чтобы выяснить является ли аэропорт пунктом назначения или отправления, необходимо сопоставить данные из двух блоков. Первый — это **airports**, в нем указан IATA-код города, в котором расположен каждый аэропорт. Второй — блок **flight**, он содержит параметры *arrival* и *departure* — это IATA коды городов назначения и отправления.

### Лоукостеры в результатах поиска

В результатах поиска содержится ряд авиакомпаний и агентств, не умеющих работать с партнерским маркером. При продаже билетов через эти компании вознаграждение партнеру начислено не будет.

Вы можете по своему усмотрению оставлять билеты от данных компаний в результатах поиска или фильтровать их перед тем, как отобразить вашим пользователям. Однако имейте ввиду — это компании лоукостеры, т. е. у них, как правило, самые низкие цены на билеты. И если их убрать, поисковая выдача станет гораздо менее интересной для пользователей.

Список компаний, не поддерживающих работу с партнерским маркером:

- [представлены на сайте aviasales.ru](https://support.travelpayouts.com/hc/ru/articles/206593288#ru) (локаль ru в API);
- [представлены на сайте jetradar.com](https://support.travelpayouts.com/hc/ru/articles/206593288#en) (локаль en).

## Получение ссылки для перехода на сайт агентства

**Внимание**! Получение ссылки на сайт агентства может инициироваться только при нажатии пользователем кнопки «Купить». Автоматический сбор всех ссылок из ответа запрещен. Нарушение этого правила приводит к отключению API поиска у партнера.

Чтобы получить ссылку на сайт агентства для бронирования билетов, необходимо:

1\. Отправить запрос на адрес:

https://api.travelpayouts.com/v1/flight_searches/**search_id**/clicks/**terms.url**.json?**marker**=ВашМаркер

где **search_id **— id поиска из ответа, **terms.url** — значение параметра **url** из ответа, **marker** — маркер партнёра.

2\. В результате будет получен ответ вида:

```
{
 "gate_id": 112,
 "click_id": 1638059365479303264,
 "str_click_id": "1638059365479303264",
 "url": "https://www.flightnetwork.com/flights/showflight?cid=175&qid=b4efe7e975ff9eee42d420600345160e&utm_source=jetradar&utm_medium=3rdparty&client_ref=fnjtrus_tg2b&utm_campaign=metasearch&currency=RUB&cmpid=3P-CPC-JTR-USA-XXX-FLT-MET-PYP-cg0zkzxavu5c-DIR-XXX-2014-01-28",
 "method": "GET",
 "params": {}
}
```

Для перехода к бронированию посетителю отдается ссылка из параметра url, то есть в нашем примере это ссылка вида:

```
https://www.svyaznoy.travel/?utm_source=aviasales.ru&utm_medium=cpa&utm_campaign=meta_avia#MOW0906/BKK1506/A1/C0/I0/S0/22316/EK-132;EK-374/EK-373;EK-131&marker=7uh46i0v2
```

**Важно!** Время «жизни» подобной ссылки 15 минут, после чего потребуется произвести поиск заново для получения актуальных цен и генерации новой ссылки на переход.

### Особенности формирования ссылок на некоторые гейты

Ряд гейтов не отдают прямые ссылки, а передают данные в массиве **params**. Такие агентства содержат в массиве параметр **method** со значением «**POST**» (у обычных гейтов этот параметр равен «GET»). Пример такого ответа:

```
{
  "params": {
    "FlightNumber1": "7793",
    "PointOfSaleCountry": "US",
    "Origin2": "BLQ",
    "UserLanguage": "en",
    "Adult": "1",
    "Destination2": "MLA",
    "DisplayedPriceCurrency": "EUR",
    "DepartureDate2": "2016-08-18",
    "Origin1": "MLA",
    "FlightNumber2": "7794",
    "ReferralId": "GoogleFlightSearch",
    "Destination1": "BLQ",
    "TripType": "RoundTrip", 
    "UserCurrency": "RUB",
    "DisplayedPrice": "153.98",
    "BookingCode1": "Y",
    "Carrier1": "FR",
    "Cabin1": "Economy", 
    "DepartureDate1": "2016-07-25", 
    "BookingCode2": "Y",
    "Cabin2": "Economy",
    "Carrier2": "FR"
    },
  "click_id": 56773565449083,
  "gate_id": "50",
  "method": "POST",
  "url": "https://www.bookryanair.com/SkySales/booking.aspx?utm_source=aviasales_ru"
}
```

Чтобы перенаправить пользователя на сайт подобного агентства и отследить переход, необходимо сделать страницу-редирект.

Для отслеживания партнерского маркера, страница должна содержать такой код:

```
<img width="0" height="0" id="pixel" src="//yasen.aviasales.ru/adaptors/pixel_click.png?click_id=CLICK_ID&gate_id=GATE_ID">
```

где **click_id** — это значение параметра из массива с ответом (в примере выше этот параметр равен *56773565449083*), **gate_id** — идентификатор гейта (в примере выше равен *50*).

Чтобы изображение наверняка загрузилось, на странице добавляется пауза, во время которой пользователю отображается сообщение, что идет перенаправление на сайт агентства.

На странице добавляется так же проверка параметра **method** и в зависимости от результата выполняется один из сценариев:

- если метод GET — выполняется редирект по прямой ссылке из параметра **url**;
- если метод POST — формируется форма с параметрами из массива **params **и пользователь перенаправляется с этой страницы по ссылке из поля **url** в ответе выше.

Вариант страницы-редиректа, можно посмотреть [здесь](https://support.travelpayouts.com/hc/article_attachments/204615417).

**Обратите внимание!** Так как логика работы агентств и авиакомпаний может меняться мы рекомендуем использовать пиксель вне зависимости от значения параметра **method** из ответа.

```
$('body').append(`<img src="//yasen.aviasales.ru/adaptors/pixel_click.png?click_id=${response.str_click_id}&gate_id=${response.gate_id}" width="0" height="0" id="pixel">`);
```

### Логотипы авиакомпаний и агентств

**Логотипы авиакомпаний** доступны по адресу: http://img.wway.io/pics/root/iata@png?exar=1&rs=fit:width:height

где,

- **width** — ширина логотипа
- **height** — высота логотипа
- **iata** — [IATA—код авиакомпании](https://support.travelpayouts.com/hc/ru/articles/203956063).\
  Размер может быть любым.

Пример: <http://img.wway.io/pics/root/UN@png?exar=1&rs=fit:200:200> 

**Логотипы агентств** доступны по адресу: http://img.wway.io/pics/as_gates/**id**@png?exar=1&rs=fit:**width**:**height**

где,

- **width** — ширина логотипа
- **height** — высота логотипа
- **id** — ID агентства из ответа поискового API (смотри поле **terms** — информация о стоимости перелета (цифра — id гейта)).\
  Размер может быть любым.

Пример: <http://img.wway.io/pics/as_gates/70@png?exar=1&rs=fit:110:70> 

Чтобы получить логотип для дисплеев Retina, умножьте высоту и ширину в ссылке на два, например: <http://img.wway.io/pics/as_gates/70@png?exar=1&rs=fit:220:140> 

## Как сформировать сигнатуру (md5 signature)

Рассмотрим пример формирования сигнатуры (подписи) для [запроса](https://support.travelpayouts.com/hc/ru/articles/203956173#01) к старой версии API поиска авиабилетов от Aviasales. К примеру, у нас есть набор параметров, которые мы хотим передать в API, чтобы получить данные по авиабилетам:

    {
        "signature": "55b47124341b5bed8627499b0eb5de80",
        "marker": "ВашМаркер",
        "host": "%server_host%",
        "user_ip": "%user_ip_address%",
        "locale": "ru",
        "trip_class": "Y",
        "passengers": {
            "adults": 1,
            "children": 0,
            "infants": 0
        },
        "segments": [
            {
                "origin": "NYC",
                "destination": "LAX",
                "date": "2017-11-25"
            },
            {
                "origin": "LAX",
                "destination": "NYC",
                "date": "2017-12-18"
            }
        ]
    }

В примере выше слева от двоеточия находятся параметры, а справа их значения.

Для начала вам нужно переставить параметры (и их значения) так, чтобы они шли в алфавитном порядке.

Обратите внимание, при сортировке учитывается вложенность данных. Это значит, что если элемент содержит массив (например, **segments**) или список параметров (например, **passengers**), то содержимое данного элемента сортируется отдельно и ставится на его место в общем списке. При этом содержимое не сортируется с параметрами верхнего уровня. Параметры внутри массива сортируются в порядке следования фигурных скобок { }.

Чтобы было проще понять процесс сортировки, посмотрите gif ниже (нажмите, чтобы открыть в новом окне):

![](https://support.travelpayouts.com/hc/article_attachments/201260186)

    "host": "%server_host%",
    "locale": "ru",
    "marker": "ВашМаркер",
    "adults": 1,
    "children": 0,
    "infants": 0
    "date": "2017-11-25"
    "destination": "LAX",
    "origin": "NYC",
    "date": "2017-12-18"
    "destination": "NYC",
    "origin": "LAX",
    "trip_class": "Y",
    "user_ip": "%user_ip_address%"

После сортировки соберите строку, содержащую только **значения** параметров, отделяя их друг от друга двоеточием, например:

    beta.aviasales.ru:ru:ВставьтеСюдаВашМаркер:1:0:0:2021-12-25:NYC:LAX:2021-12-30:LAX:NYC:Y:127.0.0.1

Маркер находится в нижнем левом углу личного кабинета:\
![](https://support.travelpayouts.com/hc/article_attachments/31367030593682)

Добавьте в начало строки **ваш партнерский API-ключ** (токен). Он находится в разделе [Профиль](https://app.travelpayouts.com/profile/info) на вкладке API-ключ.\
![](https://support.travelpayouts.com/hc/article_attachments/33566121946130)

У вас получится строка вида:

    ВашКлюч:beta.aviasales.ru:ru:ВашМаркер:1:0:0:2021-12-25:NYC:LAX:2021-12-30:LAX:NYC:Y:127.0.0.1 

Возьмите её и [сформируйте md-5 подпись](https://miraclesalad.com/webtools/md5.php). Полученный результат и является сигнатурой запроса. Используйте подпись для отправки запроса к [API поиска](https://support.travelpayouts.com/hc/ru/articles/203956173).

Внимание! Сигнатура чувствительна к регистру.
