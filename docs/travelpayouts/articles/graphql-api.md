# Aviasales GraphQL API для доступа к данным по авиабилетам

> Источник: https://support.travelpayouts.com/hc/ru/articles/4417975783314 (обновлено 2026-03-30, сохранено 2026-09-27)

В представленной статье находится информация о том, что такое GraphQL и как с ним работать.                                                                                     

Travelpayouts подготовил для партнёров GraphQL сервис для доступа к историческим данным поисков авиабилетов.

Доступ к GraphQL открыт для всех партнёров, зарегистрированных в [Travelpayouts](https://www.travelpayouts.com/) и подключенных к программе [Aviasales](https://app.travelpayouts.com/programs/100/about).

**Обратите внимание** на [лимиты запросов](https://support.travelpayouts.com/hc/ru/articles/4402565416594) при работе с GraphQL API.

## Что такое GraphQL

GraphQL — это язык запросов к API, позволяющий с помощью одного запроса получить все требуемые данные. С его помощью можно получать больше данных, отправляя меньше запросов, чем при работе с классическим REST API. Подробнее с принципами работы GraphQL можно познакомиться в [официальной документации](https://graphql.org/learn/).

Мы подготовили интерфейс, в котором вы можете познакомиться с возможностями GraphQL API: <http://api.travelpayouts.com/graphql/v1/playground>.\
![](https://support.travelpayouts.com/hc/article_attachments/26342068361106)

## Как работать с GraphQL API

Рассмотрим принцип работы с GraphQL API на примере [песочницы](http://api.travelpayouts.com/graphql/v1/playground).

Каждый запрос должен содержать ваш [API-ключ](https://app.travelpayouts.com/programs/100/tools/api) (токен). Поместите его в заголовок запроса (**HTTP Headers**) в поле **X-Access-token**:

```
{
"X-Access-token": "ВставьтеСюдаВашКлюч"
}
```

![](https://support.travelpayouts.com/hc/article_attachments/26342084918162)

В запросе необходимо передать параметры, которые отвечают за сам запрос и за содержимое ответа.

Пример запроса:

```
{
  prices_one_way( #в скобках указываются параметры, отвечающие за запрос
    params: {
        origin: "MOW"
        destination: "LON"
        depart_months: "2025-09-01"
        no_lowcost: true
        }
    paging: {
        limit: 3
        offset: 0
      }
    sorting: VALUE_ASC
  )
#далее перечисляем параметры ответа
 {
  departure_at
  value
  trip_duration
  ticket_link
 }
}
```

С помощью приведённого выше запроса мы получим три самых дешевых билета из Москвы в Лондон без учёта лоукостеров за октябрь 2025.

Пример ответа:

```
{
  "data": {
    "prices_one_way": [
      {
        "departure_at": "2022-02-18T16:00:00+03:00",
        "value": 7967,
        "trip_duration": 0,
        "ticket_link": "/MOW1802LON1?t=BA16451892001645203900000245DMELHR_1bd2ff318f7e333fde2f3acc7271537a_7967&search_date=09022022&expected_price_uuid=34db070b-0234-4b8b-a423-a5839a446575&expected_price_currency=rub"
      },
      {
        "departure_at": "2022-02-28T05:10:00+03:00",
        "value": 8356,
        "trip_duration": 0,
        "ticket_link": "/MOW2802LON1?t=A316460142001646082900001145DMEATHLHR_6ff5344b9a723d764f583415fc5b31e3_8356&search_date=09022022&expected_price_uuid=d30e37dc-16bb-4275-9fd7-7fd866a8d65d&expected_price_currency=rub"
      },
      {
        "departure_at": "2022-02-24T18:30:00+03:00",
        "value": 8444,
        "trip_duration": 0,
        "ticket_link": "/MOW2402LON2?t=PC16457166001645781400001080DMESAWSTN_3d528a2649ce79fe6e46b9598c18311c_16889&search_date=09022022&expected_price_uuid=5846bd36-9277-4d03-a91c-d88d22246e87&expected_price_currency=rub"
      }
    ]
  }
}
```

В ответе получаем цену, дату отправления, время в пути и код, который нужно подставить к <https://www.aviasales.ru/search/> для получения ссылки на билет в выдаче Aviasales.

Список всех доступных запросов с описанием параметров и полей ответа находится на вкладке **Docs**. Учтите, что вам также необходимо указать ваш API-ключ (токен) в **X-Access-Token** в поле **Headers**, чтобы просмотреть содержимое вкладки.\
![](https://support.travelpayouts.com/hc/article_attachments/26342084918674)

![](https://support.travelpayouts.com/hc/article_attachments/26342084922514)

## Пример запроса на PHP

Ниже приведён пример запроса к Aviasales GraphQL API на PHP с использованием cURL.

``` p1
<?php
// Define the GraphQL query as a string
$query = <<<GQL
{
  prices_one_way(
    params: {
        origin: "BCN"
        destination: "LON"
        depart_months: "2025-07-01"
        no_lowcost: true
    }
    paging: {
        limit: 3
        offset: 0
    }
    sorting: VALUE_ASC
  ) {
    departure_at
    value
    trip_duration
    ticket_link
  }
}
GQL;

// Set the headers and endpoint
$headers = [
    "Content-Type: application/json",
    "X-Access-Token: РазместитеЗдесьВашКлюч"
];

$endpoint = "https://api.travelpayouts.com/graphql/v1/query";

// Initialize a cURL session
$ch = curl_init($endpoint);

// Set cURL options
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_POST, true);
curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode(['query' => $query]));
curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);

// Execute the request
$response = curl_exec($ch);

// Check for cURL errors
if (curl_errno($ch)) {
    echo "cURL error: " . curl_error($ch);
} else {
    // Decode and display the response
    $decodedResponse = json_decode($response, true);
    print_r($decodedResponse);
}

// Close the cURL session
curl_close($ch);

?>
```
