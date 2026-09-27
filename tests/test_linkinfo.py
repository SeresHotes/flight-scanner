"""Разбор link: цепочка аэропортов из t=, багаж из static_fare_key."""
from core.linkinfo import booking_url, parse_baggage, parse_link, parse_t

LINK_CONNECTING = (
    "/search/MOW0305CAN1?t=CZ17778429001777983900002050SVOWUHCAN_beb3c97afcb975fe2d480176aad0e66d_25387"
    "&search_date=24032026&static_fare_key=TY%7CP1%7CH1%7CL1_1_23%7CCH1%7CR1%7CTBC1"
    "&expected_price_currency=rub&expected_price=25387"
)
LINK_DIRECT_NO_BAG = (
    "/search/MOW2910BJS1?t=MU17932941001793339400000455SVOPKX_da993a5355f4f5bab894efa40c87270c_36584"
    "&static_fare_key=TY%7CP1%7CH1%7CL0%7CCH1%7CR0%7CTBC0&expected_price=36454"
)
LINK_OLD = "/search/HKT0203KBV1?t=FD17724930001772560800001130HKTDMKKBV_0ae8e3f2_5328&search_date=13122025"


def test_duration_matches_timestamps_within_one_timezone():
    # HKT→DMK→KBV (весь в Таиланде): прилёт − вылет == длительность × 60
    info = parse_link(LINK_OLD)
    assert info.duration_min == 1130
    assert info.arrive_ts - info.depart_ts == 1130 * 60


def test_parse_chain_and_times():
    info = parse_link(LINK_CONNECTING)
    assert info.airline == "CZ"
    assert info.airports == ["SVO", "WUH", "CAN"]
    assert info.transfers == ["WUH"]
    assert info.duration_min == 2050
    assert info.depart_ts == 1777842900 and info.arrive_ts == 1777983900
    assert info.price == 25387


def test_baggage_included():
    info = parse_link(LINK_CONNECTING)
    assert info.baggage.known and info.baggage.included
    assert (info.baggage.pieces, info.baggage.kg) == (1, 23)
    assert info.fare["L"] == "1_1_23" and info.fare["CH"] == "1"


def test_baggage_absent_and_unknown():
    assert parse_link(LINK_DIRECT_NO_BAG).baggage.included is False
    assert parse_link(LINK_DIRECT_NO_BAG).baggage.known is True
    old = parse_link(LINK_OLD)
    assert old.baggage.known is False
    assert old.airports == ["HKT", "DMK", "KBV"]


def test_bad_input():
    assert parse_link(None).airports == []
    assert parse_link("/search/MOW0305CAN1").airports == []
    assert parse_t("garbage").airline is None
    assert parse_baggage("").known is False


def test_booking_url():
    assert booking_url("/search/X").startswith("https://www.aviasales.ru/search/X")
    assert booking_url(None) is None
