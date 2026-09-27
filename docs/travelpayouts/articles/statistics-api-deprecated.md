# API статистики по бронированиям, балансу и выплатам (deprecated)

> Источник: https://support.travelpayouts.com/hc/ru/articles/206111067 (обновлено 2025-03-19, сохранено 2026-09-27)

Как получить партнёрскую статистику по всем партнёрским программам без входа в личный кабинет.      

**Обратите внимание!** API, описанные в данной статье, больше не поддерживаются. Для получения финансовой статистики, используйте информацию из статьи [API статистики по балансу и выплатам](https://support.travelpayouts.com/hc/ru/articles/5169505760402).

 

Партнёрам [Travelpayouts](https://www.travelpayouts.com/) доступно получение партнёрской статистики по всем партнёрским программам без входа в личный кабинет. С помощью API партнёр может получить:

- информацию о балансе;
- информацию по выплатам партнёрского вознаграждения;
- данные по переходам и покупкам.

**Обратите внимание**, для методов API используются лимиты, которые описаны в статье [Лимиты при работе с API данных](https://support.travelpayouts.com/hc/ru/articles/4402565416594-%D0%9B%D0%B8%D0%BC%D0%B8%D1%82%D1%8B-%D0%BF%D1%80%D0%B8-%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B5-%D1%81-API-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85).

## Доступ к данным

Для получения данных вместе с запросом требуется передавать партнёрский токен. Он доступен всем партнёрам сразу после регистрации в [личном кабинете](https://www.travelpayouts.com/programs/100/tools/api).

![mceclip0.png](https://support.travelpayouts.com/hc/article_attachments/360022646739/mceclip0.png)

## Баланс (deprecated)

Возвращает текущее состояние баланса партнёра.

**Запрос**

**http://api.travelpayouts.com/v2/statistics/balance?token=api_token**

**Параметры запроса**

- **token** — ваш партнёрский токен;
- **currency** — валюта, в которой выводится статистика.

**Ответ**

    {
        "success":true,
        "data":{
            "balance":1416.07,
            "currency":"RUB"
        }
    }

**Параметры ответа**

- **success** — запрос завершился успешно (true) или возникла ошибка (false);
- **balance** — сумма средств на балансе партнёрского счёта;
- **currency** — валюта счёта.

## Выплаты партнёрского вознаграждения (deprecated)

Возвращает список выплат партнёру с момента регистрации в партнёрской программе.

**Запрос**

**http://api.travelpayouts.com/v2/statistics/payments?token=api_token**

**Параметры запроса**

- **token** — ваш партнёрский токен.

**Ответ**

    {
        "success":true,
        "data":{
            "payments":[
                {
                    "timestamp":"2020-05-11 05:47:18",
                    "status":"paid",
                    "currency":"rub",
                    "amount":3399.07,
                    "comment":"Travelpayouts"
                }
            ]
        }
    }

**Параметры ответа**

- **success** — запрос завершился успешно (true) или возникла ошибка (false);
- **timestamp** — дата совершения платежа;
- **status** — статус операции (paid — выплата вознаграждения);
- **currency** — валюта платежа;
- **amount** — сумма операции;
- **comment** — комментарий для платежа.

## Данные по переходам и покупкам (deprecated)

**Методы v2/statistics/sales и v2/statistics/detailed-sales помечены как deprecated. Рекомендуем перейти на новый [API статистики партнёрских программ](https://support.travelpayouts.com/hc/ru/articles/360019864079).**

Данные по дате, доп. маркеру или хосту (deprecated)

Возвращает количество поисков/кликов/бронирований и заработок по ним, сгруппированные по дате, доп. маркеру или хосту. Данные за месяц, как по отелям, так и по авиабилетам.

**Запрос**

**http://api.travelpayouts.com/v2/statistics/sales?group_by=date&month=2020-05-14&host_filter=null&marker_filter=null&token=api_token**

**Параметры запроса**

- **group_by** — способ группировки:
  - **date** — по дате;
  - **host** — по хосту;
  - **marker** — по дополнительному маркеру.
- **month** — месяц, за который формируется отчёт;
- **host_filter** — фильтровать по указанному хосту. Если в работе используется [White Label](https://support.travelpayouts.com/hc/ru/articles/203955753-White-Label), то с помощью этого фильтра можно получить данные по его результативности;
- **marker_filter** — фильтровать по [sub ID](https://support.travelpayouts.com/hc/ru/articles/203955653);
- **currency** — валюта, в которой выводится статистика;
- **token** — ваш партнёрский токен.

**Ответ**

    {
        "success":true,
        "data":{
            "sales":[
            {
                "key":"2020-05-14",
                "flights":{
                    "visitors":1,
                    "searches":17,
                    "clicks":0,
                    "paid_clicks":0,
                    "paid_clicks_profit":0,
                    "bookings":0,
                    "paid_bookings":0,
                    "paid_bookings_profit":0,
                    "pending_bookings_profit":0
                 },
                 "hotels":{
                    "visitors":0,
                    "searches":0,
                    "clicks":0,
                    "paid_clicks":0,
                    "paid_clicks_profit":0,
                    "bookings":0,
                    "paid_bookings":0,
                    "paid_bookings_profit":0,
                    "pending_bookings_profit":0
                 }
            }
        ]
        }
    }

**Параметры ответа**

- **success** — запрос завершился успешно (true) или возникла ошибка (false);
- **key** — отчётная дата;
- **flights** — массив данных по авиабилетам / **hotels** — массив данных по отелям:
  - **visitors** — количество человек, перешедших по партнёрской ссылке и совершивших хотя бы один поиск;
  - **searches** — количество посетителей, воспользовавшихся формой поиска (ввели запрос и нажали «Найти»);
  - **clicks** — количество посетителей, кликнувших на ссылку в результатах поиска;
  - **paid_clicks** — количество платных кликов (к системе подключено несколько агентств, которые платят комиссию не за бронирование, а за клик в результатах поиска по ссылке агентства);
  - **paid_clicks_profit** — доход от платных кликов;
  - **bookings** — количество посетителей, забронировавших билеты/номера в отелях;
  - **paid_bookings** — количество оплаченных броней;
  - **paid_bookings_profit** — полученный партнёром доход от оплаченных броней;
  - **pending_bookings_profit** — отображается суммарный доход с бронирований, находящихся в статусе «В обработке».

## Данные по дате и доп. маркеру (deprecated)

Возвращает количество поисков/кликов/бронирований и заработок по ним, сгруппированные по дате и доп. маркеру. Данные за месяц, как по отелям, так и по авиабилетам.

**Запрос**

**http://api.travelpayouts.com/v2/statistics/detailed-sales?group_by=date_marker&month=2020-05-14&host_filter=null&marker_filter=null&token=api_token**

**Параметры запроса**

- **group_by** — способ группировки:
  - **date_marker** — по дате и маркеру.
- **month** — месяц, за который формируется отчет;
- **host_filter** — фильтровать по указанному хосту. Если в работе используется [White Label](https://support.travelpayouts.com/hc/ru/articles/203955753-White-Label), то с помощью этого фильтра можно получить данные по его результативности;
- **marker_filter** — фильтровать по [sub ID](https://support.travelpayouts.com/hc/ru/articles/203955653);
- **currency** — валюта, в которой выводится статистика;
- **token** — ваш партнёрский токен.

**Ответ**

    {
        "success":true,
        "data":{
            "sales":{
                "2020-05-01":{
                    "16886":{
                        "flights":{
                            "visitors":2,
                            "searches":394,
                            "clicks":0,
                            "paid_clicks":0,
                            "paid_clicks_profit":0,
                            "bookings":0,
                            "paid_bookings":0,
                            "paid_bookings_profit":0,
                            "pending_bookings_profit":0
                        },
                        "hotels":{
                            "visitors":9,
                            "searches":9,
                            "clicks":0,
                            "paid_clicks":0,
                            "paid_clicks_profit":0,
                            "bookings":0,
                            "paid_bookings":0,
                            "paid_bookings_profit":0,
                            "pending_bookings_profit":0
                            }
                        },
                        "test":{
                            "flights":{
                                "visitors":1,
                                "searches":1,
                                "clicks":0,
                                "paid_clicks":0,
                                "paid_clicks_profit":0,
                                "bookings":0,
                                "paid_bookings":0,
                                "paid_bookings_profit":0,
                                "pending_bookings_profit":0
                    },
                "hotels":{
                                "visitors":0,
                                "searches":0,
                                "clicks":0,
                                "paid_clicks":0,
                                "paid_clicks_profit":0,
                                "bookings":0,
                                "paid_bookings":0,
                                "paid_bookings_profit":0,
                                "pending_bookings_profit":0
                            }
                        }
                    }
                }
            }
    }

**Параметры ответа**

- **success** — запрос завершился успешно (true) или возникла ошибка (false);
- **sales** — массив данных в разрезе дат (напр. 2020-05-01);
- **16886** / **test** — внутри каждой даты находится перечень партнёрских маркеров, по которым были переходы/поиски/бронирования. 
  - **flights** — массив данных по авиабилетам / **hotels** — массив данных по отелям:\
    - **visitors** — количество человек, перешедших по партнёрской ссылке и совершивших хотя бы один поиск;
    - **searches** — количество посетителей, воспользовавшихся формой поиска (ввели запрос и нажали «Найти»);
    - **clicks** — количество посетителей, кликнувших на ссылку в результатах поиска;
    - **paid_clicks** — количество платных кликов (к системе подключено несколько агентств, которые платят не комиссию за бронирование, а за клик в результатах поиска по ссылке агентства);
    - **paid_clicks_profit** — доход от платных кликов;
    - **bookings** — количество посетителей, забронировавших билеты/номера в отелях;
    - **paid_bookings** — количество оплаченных броней;
    - **paid_bookings_profit** — полученный партнёром доход от оплаченных броней;
    - **pending_bookings_profit** — отображается суммарный доход с бронирований, находящихся в статусе «В обработке».
