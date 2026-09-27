# API статистики по балансу и выплатам

> Источник: https://support.travelpayouts.com/hc/ru/articles/5169505760402 (обновлено 2026-02-24, сохранено 2026-09-27)

Как с помощью API получить текущий баланс, историю выплат вознаграждения и информацию по действиям.                          

С помощью данного API партнёры Travelpayouts могут получить информацию о балансе и выплатах

Партнёрам [Travelpayouts](https://www.travelpayouts.com/) доступно получение партнёрской статистики по всем партнёрским программам без входа в личный кабинет.

С помощью данного API партнёр может получить:

- текущий баланс и действия, влияющие на него;
- историю выплат партнёрского вознаграждения;
- детальную информацию по действиям.

**Обратите внимание**, для методов API используются лимиты, которые описаны в статье [Лимиты при работе с API данных](https://support.travelpayouts.com/hc/ru/articles/4402565416594-%D0%9B%D0%B8%D0%BC%D0%B8%D1%82%D1%8B-%D0%BF%D1%80%D0%B8-%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B5-%D1%81-API-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85).

## Доступ к данным

Для получения данных вместе с запросом требуется передавать **API-ключ** (токен). Он доступен партнёрам после регистрации на платформе Travelpayouts в разделе [Профиль](https://app.travelpayouts.com/profile/api-token).

![](https://support.travelpayouts.com/hc/article_attachments/33566494852114)

## Текущий баланс пользователя

Запрос:

```
curl --request GET \
  --url 'https://api.travelpayouts.com/finance/v2/get_user_balance' \
  --header 'X-Access-Token: <TOKEN>'
```

```
 
```

Ответ:

```
{
  "balance": {
    "usd": "1794.34",
    "eur": "1524.08",
    "rub": "134661.93"
  }
}
```

## Действия, влияющие на баланс пользователя

Запрос:

```
curl --request GET \
  --url 'https://api.travelpayouts.com/finance/v2/get_user_actions_affecting_balance?currency=usd&limit=3' \
  --header 'X-Access-Token: <TOKEN>'
```

Параметры запроса:

- **currency** — валюта, возможные варианты — \[rub, usd, eur\], по умолчанию — rub;
- **action_id** — ограничение по ID действия, пример — 100:012345678;
- **action_state** — ограничение по статусу действия, возможные варианты — \[paid, processing, cancelled\];
- **campaign_id** — ограничение по ID партнёрской программы, пример — 100;
- **from**, **until** — ограничение по дате создания действия (поле `created_at`), пример — 2020-01-01
- **offset**, **limit** — пагинация, максимальный limit — 300.

Ответ:

```
{
  "actions": [
    {
      "action_id": "100:14358982146",
      "campaign_id": 100,
      "action_state": "paid",
      "price": "60.83",
      "profit": "1.52",
      "description": "OW: Yekaterinburg (SVX) – Samara (KUF), 2021-05-12, Red Wings",
      "booked_at": "2021-05-12 06:26:54",
      "updated_at": "2021-05-12 06:30:58"
    }
  ],
  "total_price": "45364.93",
  "total_profit": "1794.34",
  "available_campaigns": [
    0,
    75,
    100,
    101,
    157
  ],
  "count": 289
}
```

## Сумма к выплате для пользователя

Запрос:

```
curl --request GET \
  --url 'https://api.travelpayouts.com/finance/v2/get_user_next_payout' \
  --header 'X-Access-Token: <TOKEN>'
```

```
 
```

Ответ:

```
{
  "next_payout": {
    "usd": "2322.25",
    "eur": "2124.65",
    "rub": "192022.6"
  }
}
```

## Список выплат пользователя

Запрос:

```
curl --request GET \
  --url 'https://api.travelpayouts.com/finance/v2/get_user_payments' \
  --header 'X-Access-Token: <TOKEN>'
```

```
 
```

Ответ:

```
[
  {
    "paid_at": "2021-04-20 13:34:42",
    "payment_uuid": "4af0c247-df4c-4873-a6b6-7bbf91afa3b9",
    "amount": "29.82",
    "currency": "usd",
    "payment_info_id": 92798,
    "comment": "test"
  }
]
```

## Список действий специфичной выплаты пользователя

Запрос:

```
curl --request GET \
  --url 'https://api.travelpayouts.com/finance/v2/get_user_actions_affecting_payment?payment_uuid=<UUID>&limit=3' \
  --header 'X-Access-Token: <TOKEN>'
```

Параметра запроса:

- **payment_uuid** — ID выплаты, пример — 4af0c247-df4c-4873-a6b6-7bbf91afa3b9;
- **offset**, **limit** — пагинация; максимальный limit — 300.

Ответ:

```
{
  "actions": [
    {
      "campaign_id": 100,
      "action_id": "100:10080977893",
      "finished_amount": "0.01",
      "booked_at": "2020-02-25 11:05:21"
    }
  ],
  "currency": "usd",
  "count": 396
}
```

## Детализация по специфичному действию

Запрос:

```
curl --request GET \
  --url 'https://api.travelpayouts.com/finance/v2/get_action_details?action_id=<ACTION_ID>&currency=usd' \
  --header 'X-Access-Token: <TOKEN>'
```

Параметра запроса:

- **action_id** — ID действия, пример — 100:012345678;
- **currency** — валюта, возможные варианты — \[rub, usd, eur\], по умолчанию rub.

Ответ:

```
{
  "action_id": "100:14195346240",
  "campaign_id": 100,
  "action_state": "paid",
  "sub_id": ".mobile_app_1eddc55e58ba904651d31ad2f1473e80",
  "price": "397.9",
  "profit": "21.09",
  "history": [
    {
      "action_state": "paid",
      "price": "397.9",
      "profit": "21.09",
      "profit_diff": "0",
      "updated_at": "2021-04-28 08:33:46"
    }
  ],
  "metadata": [
    {
      "name": "user_country",
      "value": "Bahrain"
    },
    {
      "name": "user_city",
      "value": "Manama"
    },
    {
      "name": "user_device_type",
      "value": "Desktop"
    },
    {
      "name": "user_os",
      "value": "Unknown"
    },
    {
      "name": "user_browser",
      "value": "Unknown"
    },
    {
      "name": "referrer_domain",
      "value": "iphone.wl.aviasales.ru"
    },
    {
      "name": "is_mobile_app",
      "value": "Yes"
    },
    {
      "name": "direction",
      "value": "Dubai (DXB) – Tashkent (TAS)"
    },
    {
      "name": "count",
      "value": "3"
    },
    {
      "name": "depart_date",
      "value": "2021-05-05"
    },
    {
      "name": "gate",
      "value": "Uzairways.online"
    },
    {
      "name": "trip_class",
      "value": "Economy"
    },
    {
      "name": "airline",
      "value": "HY - Uzbekistan Airways"
    },
    {
      "name": "flights",
      "value": "HY 334"
    }
  ],
  "booked_at": "2021-04-28 08:13:47"
}
```

Примечание: объект **metadata** не имеет зафиксированного набора полей и может быть специфичным для каждой программы. При этом он всегда содержит массив объектов *name/value*, где *value* — строка.
