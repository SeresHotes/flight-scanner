# Как определить города отправления и назначения из ответа API поиска авиабилетов?

> Источник: https://support.travelpayouts.com/hc/ru/articles/206635327 (обновлено 2026-04-20, сохранено 2026-09-27)

В статье приводится пример определения городов отправления и назначения из ответа API поиска.       

В каждом ответе содержатся параметры **original_destination** и **original_origin** — это значения городов отправления и назначения.

Например, мы сформировали [запрос](http://hurl.quickblox.com/hurls/7f5b6ea5172e81b48ef7c503ea85657a640b82df/86139064284f8b95bd23cf95cb5926539bd01764) на перелёт Мурманск-Барнаул-Мурманск, на который получили [ответ](https://www.dropbox.com/s/qog10ylrnvag3dv/response150513.json?dl=0) с вариантами перелётов.

Рассмотрим один перелет туда-обратно из этого ответа:

    {
    "proposals":[
        {
        "sign":"ce23f75099cc3756c1396362061e379b",
        "max_stops":2,
        "carriers":[
            "SU"
        ],
        "min_stop_duration":110,
        "terms":{
            "183":{
                "currency":"rub",
                "price":"28855",
                "unified_price":28855,
                "url":18300000
            }
        },
        "segment":[
            {
            "flight":[
                {
                    "departure_time":"11:25",
                    "departure_timestamp":1432542300,
                    "departure":"MMK",
                    "local_departure_timestamp":1432553100,
                    "duration":125,
                    "operating_carrier":"SU",
                    "arrival":"LED",
                    "number":6342,
                    "delay":0,
                    "arrival_time":"13:30",
                    "aircraft":"319",
                    "departure_date":"2015-05-25",
                    "arrival_date":"2015-05-25",
                    "arrival_timestamp":1432549800,
                    "local_arrival_timestamp":1432560600
                },
                {
                    "departure_time":"18:45",
                    "departure_timestamp":1432568700,
                    "departure":"LED",
                    "local_departure_timestamp":1432579500,
                    "duration":75,
                    "operating_carrier":"SU",
                    "arrival":"SVO",
                    "number":39,
                    "delay":315,
                    "arrival_time":"20:00",
                    "aircraft":"320",
                    "departure_date":"2015-05-25",
                    "arrival_date":"2015-05-25",
                    "arrival_timestamp":1432573200,
                    "local_arrival_timestamp":1432584000
                },
                {
                    "departure_time":"21:50",
                    "departure_timestamp":1432579800,
                    "departure":"SVO",
                    "local_departure_timestamp":1432590600,
                    "duration":250,
                    "operating_carrier":"SU",
                    "arrival":"BAX",
                    "number":1430,
                    "delay":110,
                    "arrival_time":"05:00",
                    "aircraft":"321",
                    "departure_date":"2015-05-25",
                    "arrival_date":"2015-05-26",
                    "arrival_timestamp":1432594800,
                    "local_arrival_timestamp":1432616400
                }
                ]
            },
            {
            "flight":[
                {
                    "departure_time":"09:00",
                    "departure_timestamp":1434596400,
                    "departure":"BAX",
                    "local_departure_timestamp":1434618000,
                    "duration":260,
                    "operating_carrier":"SU",
                    "arrival":"SVO",
                    "number":1433,
                    "delay":0,
                    "arrival_time":"10:20",
                    "aircraft":"320",
                    "departure_date":"2015-06-18",
                    "arrival_date":"2015-06-18",
                    "arrival_timestamp":1434612000,
                    "local_arrival_timestamp":1434622800
                },
                {
                    "departure_time":"15:05",
                    "departure_timestamp":1434629100,
                    "departure":"SVO",
                    "local_departure_timestamp":1434639900,
                    "duration":155,
                    "operating_carrier":"SU",
                    "arrival":"MMK",
                    "number":1324,
                    "delay":285,
                    "arrival_time":"17:40",
                    "aircraft":"SU9",
                    "departure_date":"2015-06-18",
                    "arrival_date":"2015-06-18",
                    "arrival_timestamp":1434638400,
                    "local_arrival_timestamp":1434649200
                }
            ]
            }
        ],
        "is_direct":false,
        "total_duration":1575,
        "segment_durations":[
            875,
            700
        ],
        "stops_airports":[
            "LED",
            "SVO",
            "BAX",
            "SVO",
            "MMK"
        ],
        "segments_time":[
            [
                1432542300,
                1432594800
            ],
            [
                1434596400,
                1434638400
            ]
        ],
        "segments_airports":[
            [
                "MMK",
                "BAX"
            ],
            [
                "BAX",
                "MMK"
            ]
        ],
        "validating_carrier":"SU",
        "max_stop_duration":315
        }]
    }

В ответе содержится параметр **max_stops** — он показывает максимальное количество пересадок в данном результате.

В массиве **segment** содержится информация о перелётах в массивах **flight**.

Первый массив **flight** — это перелёт туда, второй — обратно. В каждом из массивов с перелётами находятся параметры **departure** и **arrival** — это IATA-коды аэропортов отправления и назначения.

В нашем примере мы видим, что найдены:

- рейс туда: "Мурманск (MMK) - Санкт-Петербург (LED) - Москва (SVO) - Барнаул (BAX)";
- рейс обратно: "Барнаул (BAX) - Москва (SVO) - Мурманск (MMK)".

Таким образом мы можем по полученному ответу определить города отправления и назначения.

К слову, в ответе еще содержится параметр **segments_airports**, который содержит IATA-коды аэропортов отправления и назначения без аэропортов пересадок.
