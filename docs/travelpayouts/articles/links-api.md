# API партнёрских ссылок Travelpayouts

> Источник: https://support.travelpayouts.com/hc/ru/articles/25289759198226 (обновлено 2026-09-25, сохранено 2026-09-27)

Как с помощью API быстро преобразовать прямые ссылки на сайты туристических брендов в партнёрские.                                                                 

С помощью API партнёрских ссылок от [Travelpayouts](https://www.travelpayouts.com/?utm_source=support.travelpayouts.com&utm_medium=referral&utm_content=360019864079) можно быстро преобразовывать прямые ссылки на сайты туристических брендов в партнёрские без необходимости использования личного кабинета Travelpayouts. 

## Требования

1.  Зарегистрироваться на платформе [Travelpayouts](https://app.travelpayouts.com/). 
2.  Подписаться на [программы](https://app.travelpayouts.com/programs) брендов, ссылки которых вы хотите получать с помощью API.
3.  Скопировать API-ключ (токен). Он доступен партнёрам сразу после регистрации на платформе Travelpayouts. Вы сможете найти AP-ключ в разделе [Профиль](https://app.travelpayouts.com/profile/info) на вкладке API-ключ.Он передаётся в Header запроса в параметре **X-Access-Token**.\
    ![](https://support.travelpayouts.com/hc/article_attachments/33566364630802)

## Ограничения

- Максимум **100 запросов в минуту** на один маркер.
- В одном запросе можно передавать **не более 10 ссылок**.
- Для корректной работы API нельзя использовать короткие ссылки брендов, **используйте длинные ссылки**.
- Для указанных ниже брендов API партнёрских ссылок **не работает**:
  - Т-Банк
  - Сравни.ру
  - Альфа Банк
  - Park&Fly
  - Ticketmaster
  - Expedia UK
  - HolidayTaxis
  - inDrive

## Запрос

```
https://api.travelpayouts.com/links/v1/create
```

### Пример запроса

```
{
   "trs": 197987,
   "marker": 339296,
   "shorten": true,
   "links": [
       {
           "url": "https://travel.yandex.ru/hotels/moscow/beta-izmailovo/?adults=2&checkinDate=2025-03-24&checkoutDate=2025-03-29",
           "sub_id": "example"
       }
   ]
}
```

Пример запроса с несколькими ссылками

```
{
   "trs": 197987,
   "marker": 339296,
   "shorten": false,
   "links": [
       { "url": "https://www.aviasales.ge/search/TBS1803PAR1" },
       { "url": "https://travel.yandex.ru/hotels/moscow/beta-izmailovo/" }
   ]
}
```

### Параметры запроса

- **trs** – ID проекта, подписанного на программу бренда (можно найти в списке проектов)\
  ![](https://support.travelpayouts.com/hc/article_attachments/25289921037714)
- **marker** – уникальный ID партнёра в Travelpayouts:\
  ![](https://support.travelpayouts.com/hc/article_attachments/25289880094866)
- **shorten** – флаг для генерации короткой ссылки. Возможные значения true/false, если true — на выходе получится короткая ссылка, если false — длинная.
- **links** – массив ссылок, которые нужно преобразовать.
- **url** – исходная брендовая ссылка.\
  **Обратите внимание!** Для корректной работы API используйте длинные ссылки 
- **sub_id** (необязательно) – текстовое значение, которое впоследствии можно использовать, чтобы отслеживать статистику по партнёрским ссылкам. [Подробнее](https://support.travelpayouts.com/hc/ru/articles/203955653-ID-%D0%B8-SUB-ID-%D0%9C%D0%B0%D1%80%D0%BA%D0%B5%D1%80-%D0%B8-%D0%B4%D0%BE%D0%BF%D0%BE%D0%BB%D0%BD%D0%B8%D1%82%D0%B5%D0%BB%D1%8C%D0%BD%D1%8B%D0%B9-%D0%BC%D0%B0%D1%80%D0%BA%D0%B5%D1%80#stat:~:text=%D0%BB%D0%B8%D1%87%D0%BD%D0%BE%D0%B3%D0%BE%20%D0%BA%D0%B0%D0%B1%D0%B8%D0%BD%D0%B5%D1%82%D0%B0%3A-,SUB%20ID,-%D0%9F%D0%BE%D0%BC%D0%B8%D0%BC%D0%BE%20%D0%BE%D1%81%D0%BD%D0%BE%D0%B2%D0%BD%D0%BE%D0%B3%D0%BE%20ID).

## Пример ответа

```
{
   "result": {
       "trs": 197987,
       "marker": 339296,
       "shorten": true,
       "links": [
           {
               "url": "https://travel.yandex.ru/hotels/moscow/beta-izmailovo/?adults=2&checkinDate=2025-03-24&checkoutDate=2025-03-29",
               "code": "success",
               "partner_url": "https://yandex.tp.st/NHifzZw5"
           }
       ]
   },
   "code": "success",
   "status": 200
}
```

### Параметры ответа

- **trs** – ID проекта, подписанного на программу бренда
- **marker** – уникальный ID партнёра в Travelpayouts:
- **shorten** – если true — то ссылка короткая, если false — длинная.
- **links** – исходные ссылки для преобразования
- **code** – идентификатор успешности преобразования ссылки
- **partner_url** – итоговая партнёрская ссылка
- **code** – идентификатор успешности выполнения запроса
- **status** – статус выполнения запроса

## Возможные ошибки

### Ошибка в API-ключе

- Код: 401 Unauthorized

### Ошибка в trs

```
{
    "code": "incorrect_request_body",
    "error": "invalid traffic source",
    "status": 400
}
```

### Ошибка в marker

```
{
    "code": "incorrect_request_body",
    "error": "incorrect marker in the request",
    "status": 400
}
```

### Ошибка в url

```
{
    "result": {
        "trs": 197987,
        "marker": 339296,
        "shorten": true,
        "links": [
            {
                "url": "1https://travel.yandex.ru/hotels/moscow/beta-izmailovo/",
                "code": "failed",
                "message": "can't create partner link",
                "partner_url": ""
            }
        ]
    },
    "code": "success",
    "status": 200
}
```

### Ошибка: Нет подписки на бренд

```
{
    "result": {
        "trs": 197987,
        "marker": 339296,
        "shorten": true,
        "links": [
            {
                "url": "https://www.airalo.com/ru/georgia-esim/kargi-mobile-7days-1gb",
                "code": "failed",
                "message": "trs is not subscribed for brand",
                "partner_url": ""
            }
        ]
    },
    "code": "success",
    "status": 200
}
```

### Ошибка: Бренд не поддерживается в Travelpayouts

```
{
    "result": {
        "trs": 197987,
        "marker": 339296,
        "shorten": true,
        "links": [
            {
                "url": "https://www.ozon.ru/travel/?__rr=1",
                "code": "failed",
                "message": "can't create partner link",
                "partner_url": ""
            }
        ]
    },
    "code": "success",
    "status": 200
}
```
