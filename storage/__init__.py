"""Storage-слой Flight Scanner.

- hot.py  — горячее хранилище: SQLite (WAL) с котировками и джобами.
- lake.py — архив: append-only Parquet (полная история наблюдений цены).
"""
