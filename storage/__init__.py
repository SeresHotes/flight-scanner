"""Storage-слой Flight Scanner.

- hot.py — горячее хранилище: SQLite (WAL) с котировками и джобами планировщика.

История наблюдений цены (серии билетов) — в Parquet-озере коллектора в Object
Storage (collector/lake.py, docs/COLLECTOR.md).
"""
