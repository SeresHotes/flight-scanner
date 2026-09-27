# API статистики бронирований партнёрских программ

> Источник: https://support.travelpayouts.com/hc/ru/articles/360019864079 (обновлено 2026-09-20, сохранено 2026-09-27)

Изучите информацию о том, как и какие данные можно получить с помощью API статистики Travelpayouts.                                                                                                                

С помощью API статистики Travelpayouts вы можете получать данные по бронированиям, сделанным через подключённые партнёрские программы. Это полезно, если нужно выгружать данные для аналитики, автоматизировать отчёты или отслеживать эффективность инструментов.

При работе с API вы можете задавать какие данные необходимо получить, за какой период, как отсортировать и так далее.

## Доступ к API

Доступ к API статистики Travelpayouts автоматически выдаётся партнёрам после регистрации на платформе. 

Чтобы начать работать с API, нужен **API-ключ** (токен).

- Найти ключ можно в[ личном кабинете Travelpayouts](https://app.travelpayouts.com/profile/info) в разделе **Профиль → API-ключ**.\
  ![](https://support.travelpayouts.com/hc/article_attachments/33565977644690)
- **Ключ передаётся в Header запроса в параметре** **X-Access-Token**.

**Обратите внимание!** Для API запросов существуют лимиты, которые описаны в статье [Лимиты при работе с API данных](https://support.travelpayouts.com/hc/ru/articles/4402565416594-%D0%9B%D0%B8%D0%BC%D0%B8%D1%82%D1%8B-%D0%BF%D1%80%D0%B8-%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B5-%D1%81-API-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85).

## Получение списка доступных полей

Чтобы сформировать запрос, сначала получите список всех доступных полей и их типов:

GET [https://api.travelpayouts.com/statistics/v1/get_fields_list](https://api.travelpayouts.com/statistics/v1/get_fields_list) 

- По умолчанию возвращаются **raw** поля.
- Для получения **aggregated** данных используйте ?data_type=aggregated.\
  [https://api.travelpayouts.com/statistics/v1/get_fields_list?data_type=aggregated](https://api.travelpayouts.com/statistics/v1/get_fields_list?data_type=aggregated) 
- Можно указать **campaign_id=\<id\>** для специфичных полей программы. \
  Например, для программы Aviasales можно выполнить запрос [https://api.travelpayouts.com/statistics/v1/get_fields_list?campaign_id=100](https://api.travelpayouts.com/statistics/v1/get_fields_list?campaign_id=100) и тогда ещё вернутся такие поля, как *airline_code, airline_name, adults, children* и некоторые другие. \
  ID программы можно найти в ссылке на её страницу на Travelpayouts: https://app.travelpayouts.com/programs/100/about 

### Пример ответа со списком raw полей:

```
{
  "fields": [
    {
      "name": "action_id",
      "type": "string",
      "available_filters": [
        "eq",
        "ne"
      ]
    },
    {
      "name": "action_type",
      "type": "string",
      "available_filters": [
        "eq",
        "ne"
      ],
      "available_values": [
        "booking",
        "paid_click",
        "advertise_profit"
      ]
    },
    {
      "name": "user_device_type",
      "type": "string",
      "available_filters": [
        "eq",
        "ne"
      ]
    }
  ]
}
```

## Получение данных

Далее, поля, полученные в get_fields_list, используются для отправки POST-запроса в методе

[https://api.travelpayouts.com/statistics/v1/execute_query](https://api.travelpayouts.com/statistics/v1/execute_query). 

Ответ будет содержать данные по партнёрской программе, ID которой указан в запросе. ID программы можно получить из ссылки на неё в личном кабинете Travelpayouts. Например, [https://www.travelpayouts.com/programs/84/about](https://www.travelpayouts.com/programs/84/about), где 84 — это ID программы Booking.com.

### **Структура запроса**

В теле POST запроса должен быть JSON, структурированный следующим образом:

- **fields** — массив строк, составленный из названий полей, которые необходимо получить в ответе (из полей, полученных в get_fields_list);
- **filters** — фильтры, применяемые к запросу. Важно: фильтр по date является обязательным. Каждый фильтр состоит из трех полей:
  - **field** — строка, указывающая на поле, к которому будет применяться фильтр;
  - **op** — операция, которая будет применяться (список возможных к применению операций выдается в get_fields_list в поле available_filters): 
    - **eq** — равенство
    - **ne** — неравенство
    - **gt** — больше
    - **ge** — больше, либо равно
    - **le** — меньше, либо равно
    - **lt** — меньше
    - **in** — любое из перечисленных значений\
      **Важно!** Оператор in можно использовать только с полями, у которых в available_values заполнено значение (type и state)
  - **value** — значение, относительно которого будет применяться операция (для некоторых полей существует заранее определенный набор допустимых значений — выдается в get_fields_list в поле available_values).
- **sort** — настройка сортировки ответа
  - **field** — строка, указывающая на поле, по которому мы будем сортировать данные в ответе;
  - **order** — строка, указывающая на порядок, в котором мы будем сортировать по этому полю:
    - **asc** — сортировка по возрастанию;
    - **desc** — сортировка по убыванию.
- **offset** — сдвиг (0 по дефолту);
- **limit** — количество записей (100 по дефолту, максимальное — 10 000).

## Пример 1. Базовый запрос.

```
{
  "fields": [
    "user_device_type",
    "inits_count",
    "searches_count",
    "redirects_count",
    "processing_actions_count",
    "cancelled_actions_count",
    "paid_actions_count",
    "paid_profit_eur_sum",
    "processing_profit_eur_sum"
  ],
  "filters": [
    {
      "field": "date",
      "op": "ge",
      "value": "2020-01-30"
    },
    {
      "field": "date",
      "op": "le",
      "value": "2020-01-30"
    },
    {
      "field": "campaign_id",
      "op": "eq",
      "value": 100
    }
  ],
  "group": [
    "user_device_type"
  ],
  "offset": 0,
  "limit": 100
}
```

### Пример ответа

```
{
    "results": [
        {
            "action_id": "b91cb378-770e-55c7-b126439d10",
            "created_at": "2025-06-23 13:43:30",
            "date": "2025-06-23",
            "external_click_id": "31130496725740",
            "paid_profit_rub": "47.14",
            "price_rub": "4288.00",
            "state": "paid",
            "sub_id": ".mobile_app_5f78c121aea38e9a62f424a64",
            "updated_at": "2025-06-24 10:28:44"
        }
    ],
    "fields": [
        "action_id",
        "external_click_id",
        "sub_id",
        "price_rub",
        "paid_profit_rub",
        "state",
        "date",
        "updated_at",
        "created_at"
    ],
    "filters": [
        {
            "field": "type",
            "op": "eq",
            "value": "action"
        },
        {
            "field": "campaign_id",
            "op": "eq",
            "value": 100
        },
        {
            "field": "date",
            "op": "ge",
            "value": "2025-06-22"
        }
    ],
    "sort": [
        {
            "field": "date",
            "order": "asc"
        }
    ],
    "total_rows": 40,
    "offset": 0,
    "limit": 1
}
```

### Поля ответа

- **action_id** — уникальный идентификатор действия (бронирования или заказа);
- **external_click_id** — ID клика, по которому было совершено действие;
- **sub_id** — SubID, если он был указан при создании партнёрского инструмента;
- **price_rub** — сумма заказа в рублях;
- **paid_profit_rub** — подтверждённый доход в рублях по данному действию;
- **state** — статус действия: например, paid — оплачено, pending — в обработке, canceled — отменено;
- **date** — дата действия (чаще всего дата бронирования или покупки);
- **updated_at** — дата и время последнего обновления данных по действию;
- **created_at** — дата и время создания записи в системе;\
  \
  Служебные поля
- **fields** — список полей, которые были запрошены и возвращены в ответе;
- **filters** — применённые фильтры при получении данных;
- **sort** — правило сортировки данных (по какому полю и в каком порядке);
- **total_rows** — общее количество записей, удовлетворяющих условиям запроса;
- **offset** — количество пропущенных записей (например, для постраничного вывода);
- **limit** — количество записей, возвращаемых в ответе (ограничение на размер выборки).

## **Пример 2. Запрос с группировкой**

```
{
  "fields": [
    "user_device_type",
    "inits_count",
    "searches_count",
    "redirects_count",
    "processing_actions_count",
    "cancelled_actions_count",
    "paid_actions_count",
    "paid_profit_eur_sum",
    "processing_profit_eur_sum"
  ],
  "filters": [
    {
      "field": "date",
      "op": "ge",
      "value": "2020-01-30"
    },
    {
      "field": "date",
      "op": "le",
      "value": "2020-01-30"
    },
    {
      "field": "campaign_id",
      "op": "eq",
      "value": 100
    }
  ],
  "group": [
    "user_device_type"
  ],
  "offset": 0,
  "limit": 100
}
```

### Пример ответа

```
{
  "results": [
    {
      "cancelled_actions_count": 44,
      "redirect_count": 8312,
      "inits_count": 11642,
      "paid_actions_count": 303,
      "paid_profit_eur_sum": "557.37788661",
      "processing_actions_count": 0,
      "processing_profit_eur_sum": "0.00000000000000",
      "searches_count": 48987,
      "user_device_type": "Desktop"
    }
  ],
  "fields": [
    "user_device_type",
    "inits_count",
    "searches_count",
    "redirect_count",
    "processing_actions_count",
    "cancelled_actions_count",
    "paid_actions_count",
    "paid_profit_eur_sum",
    "processing_profit_eur_sum"
  ],
  "filters": [
    {
      "field": "date",
      "op": "ge",
      "value": "2020-01-30"
    },
    {
      "field": "date",
      "op": "le",
      "value": "2020-01-30"
    },
    {
      "field": "campaign_id",
      "op": "eq",
      "value": 100
    }
  ],
  "group": [
    "user_device_type"
  ],
  "offset": 0,
  "limit": 100
}
```

### Поля ответа

- **user_device_type** — тип устройства (Desktop, Mobile и т.п.).
- **inits_count** — количество инициализаций виджета.
- **searches_count** — количество поисков.
- **redirect_count** — количество кликов по партнёрским инструментам.
- **paid_actions_count** — количество оплаченных бронирований/заказов.
- **paid_profit_eur_sum** — подтверждённый доход в евро.
- **processing_actions_count** — количество заказов в обработке.
- **processing_profit_eur_sum** — сумма потенциального дохода в обработке.
- **cancelled_actions_count** — количество отменённых заказов.

## Советы по применению фильтров для запроса

Скорость выполнения запроса напрямую зависит от набора переданных фильтров. Чем они строже, тем быстрее вы получите свои данные.

Самые важные фильтры, влияющие на скорость:

- **date** — фильтр по дате. Чем шире диапазон дат, тем медленнее выполняется запрос, старайтесь не указывать широкие диапазоны без необходимости.
- **type** — тип события. Если вы четко знаете, какого типа данные хотите получить, обязательно добавляйте такой фильтр.
- **state** — статус события, работает только для *type* = action\|referral, если вы запрашиваете данные по таким типам событий и точно знаете, в каком статусе данные вам нужны - обязательно добавляйте такой фильтр.

### Примеры оптимальных фильтров для разных случаев:

#### Запрос с датой бронирования

Чтобы в ответе API отобразилась дата бронирования, необходимо явно указать поле created_at в массиве fields:

```
{
  "fields": ["paid_profit_rub_sum", "created_at"],
  "filters": [
    {
      "field": "date",
      "op": "ge",
      "value": "2025-05-01"
    }
  ]
}
```

Поле created_at возвращает дату создания брони и может быть полезно для отслеживания активности пользователей.

#### Число инициализаций виджетов за июнь 2025.

Тут нужен фильтр по типу события и нас интересуют только init:

```
[{"field":"date","op":"ge","value":"2025-06-01"},{"field":"date","op":"lt","value":"2025-07-01"},{"field":"type","op":"eq","value":"init"}]
```

#### Общее число бронирований за июль 2025.

В этом случае лучше всего применить фильтр по типу события с оператором in, и включить action и referral:

```
[{"field":"date","op":"ge","value":"2025-07-01"},{"field":"date","op":"lt","value":"2025-08-01"},{"field":"type","op":"in","value":["action", "referral"]}]
```

#### Число оплаченных бронирований, не включая реферальные, за август 2025.

Тут нужно применить не только фильтр по типу события, равный action, но еще и фильтр по состоянию события (нас интересуют только оплаченные):

```
[{"field":"date","op":"ge","value":"2025-08-01"},{"field":"date","op":"lt","value":"2025-09-01"},{"field":"type","op":"eq","value":"action"},{"field":"state","op":"eq","value":"paid"}]
```

#### Сумма оплаченных букингов в валюте X за август 2025.

Мы платим только за бронирования и реферальные действия, а значит в этом случае нужен фильтр по типу события, равный action или referral, плюс фильтр по состоянию события (нас интересуют только оплаченные). В итоге фильтры для этого случая будут идентичны случаю выше:

```
[{"field":"date","op":"ge","value":"2025-08-01"},{"field":"date","op":"lt","value":"2025-09-01"},{"field":"type","op":"eq","value":"action"},{"field":"state","op":"eq","value":"paid"}]
```
