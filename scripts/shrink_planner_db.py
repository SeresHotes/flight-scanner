"""Сжимает SQLite планировщика после переезда серий в коллектор: удаляет ticket_cache
и пересобирает файл через VACUUM INTO (нужно место только под живые данные, а не
под копию всего файла, как у обычного VACUUM). Запускать при остановленном планировщике.

    python3 scripts/shrink_planner_db.py /opt/flights/data/flights.db
"""
import os
import sqlite3
import sys


def shrink(path: str) -> None:
    if not os.path.exists(path):
        print(f"{path}: нет файла, нечего сжимать")
        return
    before = os.path.getsize(path)
    conn = sqlite3.connect(path)
    conn.execute("DROP TABLE IF EXISTS ticket_cache")
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    tmp = path + ".vacuum"
    if os.path.exists(tmp):
        os.remove(tmp)
    conn.execute("VACUUM INTO ?", (tmp,))
    conn.close()
    os.replace(tmp, path)
    for suffix in ("-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass
    after = os.path.getsize(path)
    print(f"{path}: {before / 2**20:.0f} МБ → {after / 2**20:.0f} МБ")


if __name__ == "__main__":
    shrink(sys.argv[1] if len(sys.argv) > 1 else "data/flights.db")
