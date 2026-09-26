"""Регресс: флаг города из сети аэропортов.

build_airport_network кладёт ISO2 страны в поле "country"; city_info читал
только "iso_country", и у всех городов из сети (Chengdu, Istanbul…) не было
ни страны, ни флага.
"""
from core.trip_builder import flag_emoji, make_city_lookup

NETWORK = {
    "CTU": {"name": "Chengdu Shuangliu International Airport",
            "municipality": "Chengdu (Shuangliu)", "country": "CN"},
    "OLD": {"name": "Legacy", "municipality": "Legacy", "iso_country": "TR"},
}


def test_city_from_network_has_country_and_flag():
    ci = make_city_lookup(NETWORK)("CTU")
    assert ci == {"city": "Chengdu (Shuangliu)", "country": "CN", "flag": flag_emoji("CN")}


def test_legacy_iso_country_still_supported():
    assert make_city_lookup(NETWORK)("OLD")["country"] == "TR"
