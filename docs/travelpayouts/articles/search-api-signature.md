# Как сформировать сигнатуру (md5 signature)

> Источник: https://support.travelpayouts.com/hc/ru/articles/210996008 (обновлено 2026-05-13, сохранено 2026-09-27)

В данной статье подробно описано, как правильно сформировать сигнатуру для запроса к API поиска авиабилетов. чтобы он работал корректно.                           

Рассмотрим пример формирования сигнатуры (подписи) для [запроса](https://support.travelpayouts.com/hc/ru/articles/30565016140434-API-Aviasales-%D0%B4%D0%BB%D1%8F-%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0-%D0%B0%D0%B2%D0%B8%D0%B0%D0%B1%D0%B8%D0%BB%D0%B5%D1%82%D0%BE%D0%B2-%D1%81%D0%BB%D0%BE%D0%B6%D0%BD%D1%8B%D0%B5-%D0%BC%D0%B0%D1%80%D1%88%D1%80%D1%83%D1%82%D1%8B-%D0%B8-%D0%BF%D0%BE%D0%B8%D1%81%D0%BA-%D0%B2-%D1%80%D0%B5%D0%B0%D0%BB%D1%8C%D0%BD%D0%BE%D0%BC-%D0%B2%D1%80%D0%B5%D0%BC%D0%B5%D0%BD%D0%B8#:~:text=%D1%81%D0%B0%D0%B9%D1%82%D0%B5%20%D0%B0%D0%B3%D0%B5%D0%BD%D1%82%D1%81%D1%82%D0%B2%D0%B0/%D0%B0%D0%B2%D0%B8%D0%B0%D0%BA%D0%BE%D0%BC%D0%BF%D0%B0%D0%BD%D0%B8%D0%B8.-,%D0%90%D1%83%D1%82%D0%B5%D0%BD%D1%82%D0%B8%D1%84%D0%B8%D0%BA%D0%B0%D1%86%D0%B8%D1%8F%20%D0%B8%20%D0%B7%D0%B0%D0%B3%D0%BE%D0%BB%D0%BE%D0%B2%D0%BA%D0%B8,-%D0%9A%D0%B0%D0%B6%D0%B4%D1%8B%D0%B9%20%D0%B7%D0%B0%D0%BF%D1%80%D0%BE%D1%81%20%D0%B4%D0%BE%D0%BB%D0%B6%D0%B5%D0%BD) к API поиска авиабилетов от Aviasales. 

К примеру, у нас есть набор параметров, которые мы хотим передать в API, чтобы получить данные по авиабилетам:

``` hljs
{
    "signature": "YourSignature",
    "currency_code": "USD",
    "marker": "YourMarker",
    "market_code": "US",
    "locale": "US",
    "search_params": {
        "directions": [
            {
                "origin": "LAX",
                "destination": "NYC",
                "date": "2026-09-09"
            },
            {
                "origin": "NYC",
                "destination": "LAX",
                "date": "2026-09-25"
            }
        ],
        "trip_class": "Y",
        "passengers": {
            "adults": 1,
            "children": 0,
            "infants": 0
        }
    }
}
```

В примере выше слева от двоеточия находятся параметры, а справа их значения.

1.  Для начала вам нужно переставить параметры (и их значения) так, чтобы они шли в алфавитном порядке.\
    \
    Обратите внимание, при сортировке учитывается вложенность данных. Это значит, что если элемент содержит массив (например, **segments**) или список параметров (например, **passengers**), то содержимое данного элемента сортируется отдельно и ставится на его место в общем списке. При этом содержимое не сортируется с параметрами верхнего уровня. Параметры внутри массива сортируются в порядке следования фигурных скобок { }.\
     
2.  В результате у вас получится отсортированный список, например:

``` hljs
{
    "currency_code": "USD",
    "locale": "US",
    "marker": "YourMarker",
    "market_code": "US",
    "search_params": {
        "directions": [
            {
                "date": "2026-09-09",
                "destination": "NYC",
                "origin": "LAX"
            },
            {
                "date": "2026-09-25",
                "destination": "LAX",
                "origin": "NYC"
            }
        ],
        "passengers": {
            "adults": 1,
            "children": 0,
            "infants": 0
        },
        "trip_class": "Y"
    }
}
```

3.  После сортировки соберите строку, содержащую только **значения** параметров, отделяя их друг от друга двоеточием, например:

```
USD:US:YourMarker:US:2026-09-09:NYC:LAX:2026-09-25:LAX:NYC:1:0:0:Y
```

Маркер находится в нижнем левом углу личного кабинета:\
![](https://support.travelpayouts.com/hc/article_attachments/18177167939602)

4.  Добавьте в начало строки **ваш API-ключ**. Он находится в разделе [Профиль](https://app.travelpayouts.com/profile/info) на вкладке API-ключ\
    ![](https://support.travelpayouts.com/hc/article_attachments/33565728612114)
5.  У вас получится строка вида:

```
ВашКлюч:USD:US:ВашМаркер:US:2026-09-09:NYC:LAX:2026-09-25:LAX:NYC:1:0:0:Y
```

6.  Возьмите её и [сформируйте md-5 подпись](https://miraclesalad.com/webtools/md5.php). Полученный результат и является сигнатурой запроса. Используйте подпись для отправки запроса к [API поиска](https://support.travelpayouts.com/hc/ru/articles/203956173).

**Обратите внимание**: Сигнатура чувствительна к регистру.
