"""Ядро Flight Scanner — рефактор CLI-скриптов без смены поведения.

- aggregate.py    — поиск комбинаций перелётов (переходы между аэропортами).
- trip_builder.py — сборка контракта {meta, trips} (== web/data.json == /api/search).
- collector.py    — сбор котировок из Travelpayouts.
- airports.py     — справочник аэропортов, city lookup, автокомплит A/B.
"""
