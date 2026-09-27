# Как определить IATA-код города из поисковой фразы

> Источник: https://support.travelpayouts.com/hc/ru/articles/203955773 (обновлено 2025-03-19, сохранено 2026-09-27)

В представленной статье описываются варианты использования API по определению IATA-кода города.     

- [Введение](#intro)
- [Работа с API](#api)
- [Реализация запроса на PHP](#php)
- [Где используется](#usecases)
  - [Пример кода формы поиска](#search_form)
  - [Пример кода виджета](#calendar_widget)
- [Ограничения](#restriction)

## Введение

В этой статье описываются варианты использования [API по определению IATA-кода города](https://support.travelpayouts.com/hc/ru/articles/360002755812-API-%D0%B4%D0%BB%D1%8F-%D0%BE%D0%BF%D1%80%D0%B5%D0%B4%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F-IATA-%D0%BA%D0%BE%D0%B4%D0%B0).

Данная информация будет полезна в следующих случаях:

- вы хотите оптимизировать размещение [партнёрских инструментов](https://support.travelpayouts.com/hc/ru/categories/200359097) на страницах вашего сайта. С помощью данного метода можно автоматически подставлять в виджеты IATA-коды городов;
- вы работаете с контекстной рекламой и хотите, чтобы при переходе по ссылке пользователь видел страницу с теми же городами вылета и назначения, что и в рекламе;
- в любых других ситуациях, когда нужно по названию городов вылета и прилёта определить их IATA-коды.

API позволяет определить IATA-коды городов вылета и назначения из поисковой фразы вида «Из Москвы в Лондон», где вместо «Москвы» может стоять любой город вылета, а вместо «Лондон» — любой город назначения. Кроме того, метод возвращает IATA-код столицы по названию страны. 

## Работа с API

Запрос отправляется на адрес:

    https://www.travelpayouts.com/widgets_suggest_params?q=search

где вместо **search** подставляется требуемая фраза.

### Пример запроса

    https://www.travelpayouts.com/widgets_suggest_params?q=Из%20Москвы%20в%20Лондон

### Пример ответа

    {
      "origin":{
       "iata":"MOW",
       "name":"Moscow"
     },
      "destination":{
        "iata":"LON",
        "name":"London"
     }} 

### Содержимое ответа

- **origin**
  - **iata **— IATA-код города отправления;
  - **name **— название города отправления.
- **destination**
  - **iata **— IATA-код города назначения;
  - **name **— название города назначения.

## Реализация запроса на PHP

Ниже приводится пример PHP-кода, который можно использовать у себя на сайте для определения IATA-кода города. 

    <?php
     class TravelpayoutsParamsSuggester
     {
       static $url = 'https://www.travelpayouts.com/widgets_suggest_params?q=';
       public function suggest($query) {
         $url = self::$url . urlencode($query);
         return self::get_json($url);
     }
     
     private function get_json($url) {
       $ch = curl_init();
       curl_setopt($ch, CURLOPT_URL, $url);
       curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
       curl_setopt($ch, CURLOPT_FAILONERROR, 1);
     
       $answer = curl_exec($ch);
       curl_close($ch);
     
       return json_decode($answer); 
     } } 

    $example_result = TravelpayoutsParamsSuggester::suggest("Из Москвы в Цюрих"); ?> 

Для любителей coffee доступен [пример кода](https://support.travelpayouts.com/hc/article_attachments/360003564031/mewtwo.litcoffee).

## Где используется API

Полученный IATA-код может использоваться в формах поиска и партнёрских виджетах. Чтобы использовать код в форме поиска, установите на странице сайта [расширенную поисковую форму](https://support.travelpayouts.com/hc/ru/articles/203955743-%D0%A0%D0%B5%D0%B4%D0%B0%D0%BA%D1%82%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5-%D0%BA%D0%BE%D0%B4%D0%B0-%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%BE%D0%B2%D0%BE%D0%B9-%D1%84%D0%BE%D1%80%D0%BC%D1%8B). Не указывайте в ней IATA-коды городов отправления и назначения. На их месте укажите две переменные. Передайте в эти переменные значения IATA-кодов, полученные при помощи данного скрипта. **В итоге на странице отобразится поисковая форма с заполненными городами вылета и назначения, совпадающими с поисковой фразой.**

Основное направление для использования подобного подхода — это контекстная реклама Google Ads и Яндекс Директ. Создавая множество объявлений вида «Дешёвые билеты из Москвы в Симферополь», «Как быстрее долететь из Владивостока в Санкт-Петербург» и им подобным, передавайте часть рекламной фразы «… из Москвы в Симферополь» данному скрипту. В результате пользователь будет попадать на страницу с поиском билетов из Москвы в Симферополь. 

### Пример кода формы поиска

Код страницы со скриптом определения IATA-кодов и формой поиска (с размещёнными в ней полученными параметрами) выглядит следующим образом:

    <?php
    class TravelpayoutsParamsSuggester
    {
      static $url = 'https://www.travelpayouts.com/widgets_suggest_params?q=';
      public function suggest($query) {
        $url = self::$url . urlencode($query);
        return self::get_json($url);
      }
      private function get_json($url) {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_FAILONERROR, 1);
        $answer = curl_exec($ch);
        curl_close($ch);
       return json_decode($answer);
      }
    }
    $widget_params = TravelpayoutsParamsSuggester::suggest("Из Москвы в Цюрих");
    ?>

    <script charset="UTF-8" type="text/javascript">
      window.TP_FORM_SETTINGS = window.TP_FORM_SETTINGS || {};
      window.TP_FORM_SETTINGS["4677677a69f7a4291f985e1c4ce13cb7"] = {
      "handle": "4677677a69f7a4291f985e1c4ce13cb7",
      "widget_name": "Поисковая форма #10",
      "border_radius": "2",
      "additional_marker": null,
      "width": 620,
      "show_logo": true,
      "show_hotels": true,
      "form_type": "avia_hotel",
      "locale": "ru",
      "currency": "rub",
      "sizes": "default",
      "search_target": "_blank",
      "active_tab": "avia",
      "search_host": "engine.aviasales.ru",
      "hotels_host": "hotellook.ru/search",
      "hotel": "",
      "hotel_alt": "",
      "avia_alt": "",
      "id": 29551,
      "marker": 21553,
      "origin": {
        "name": "<?php echo $widget_params["origin"]["name"] ?>",
        "iata": "<?php echo $widget_params["origin"]["iata"] ?>"
      },
      "destination": {
        "name": "<?php echo $widget_params["destination"]["name"] ?>",
        "iata": "<?php echo $widget_params["destination"]["iata"] ?>"
      },
      "color_scheme": {
        "name": "white_blue",
        "icons": "icons_blue",
        "background": "#ffffff",
        "color": "#000000",
        "border_color": "#c1c1c1",
        "button": "#1b9ed9",
        "button_text_color": "#ffffff"
      },
      "hotels_type": "hotellook_host",
      "best_offer": {
        "locale": "ru",
        "currency": "rub",
        "marker": ЗдесьВашМаркер,
        "search_host": "engine.aviasales.ru",
        "offers_switch": true,
        "api_url": "//www.travelpayouts.com/minimal_prices/offers.json",
        "routes": [
        {
          "one_way": false,
          "origin": {
            "name": ""
          },
          "destination": {
            "name": ""
          }
        }
        ]
      },
      "hotel_logo_host": "hotellook.ru",
      "search_logo_host": "www.aviasales.ru",
      "hotel_marker_format": "marker=",
      "hotelscombined_marker": null,
      "height": 361
     };
    </script>

    <script charset="UTF-8" src="//www.travelpayouts.com/widgets/4677677a69f7a4291f985e1c4ce13cb7.js?v=270"></script>

### Пример кода виджета

Код страницы со скриптом определения IATA-кодов и виджетом календаря низких цен (с размещёнными в нём полученными параметрами) выглядит следующим образом:

    <?php
      class TravelpayoutsParamsSuggester {
        static $url = 'https://www.travelpayouts.com/widgets_suggest_params?q=';
        public function suggest($query) {
        $url = self::$url . urlencode($query);
        return self::get_json($url);
       }

      private function get_json($url) {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_FAILONERROR, 1);
        $answer = curl_exec($ch);
        curl_close($ch);
        return json_decode($answer, true);
      }
    }
    $widget_params = TravelpayoutsParamsSuggester::suggest("Из Москвы в Цюрих");
    ?>
    <script src="//www.travelpayouts.com/calendar_widget/iframe.js?marker=77110&origin=<?php echo $widget_params['origin']['iata']?>&destination=<?php echo $widget_params['destination']['iata']?>&currency=rub&searchUrl=hydra.aviasales.ru&one_way=false&only_direct=false&locale=ru&period=year&range=7%2C14&width=800" async></script>

## Ограничения

Скрипт имеет ряд ограничений, о которых необходимо помнить:

- скрипт не понимает опечатки;
- поддерживается только русский язык для поисковых фраз;
- скрипт не содержит вариантов из автокомплита.
