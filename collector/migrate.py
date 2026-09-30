"""Разовый перенос озера из раскладки date=<день вылета>/origin= в fetched=<день
загрузки>/origin= (collector.lake.series_file_key).

Для каждого файла индекса со старым ключом: копия внутри бакета → ключ в индексе
(файл и его серии одной транзакцией) → старые файлы удаляются пачками после паузы
(чтение, начатое по старому ключу, успевает закончиться). Коллектор при этом
работает: новые файлы он уже пишет в fetched=, ретеншн может удалить файл посреди
переноса — такой просто пропускается. Повторный запуск доделывает оставшееся.

Запуск на VM: docker exec flights-collector-1 python -m collector.migrate [--dry-run]
"""
import argparse
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from collector.config import Settings
from collector.index import Index
from collector.lake import TICKETS_PREFIX, parse_file_key, series_file_key
from collector.store import make_store

LEGACY_PREFIX = f"{TICKETS_PREFIX}/date="


def new_key(old: str) -> Optional[str]:
    meta = parse_file_key(old)
    if meta is None:
        return None
    return series_file_key(meta["origin"], meta["destination"], meta["day"], meta["params_key"],
                           meta["observed"], day_to=meta["day_to"])


def migrate(store, index: Index, *, workers: int = 16, delete_grace: float = 60.0,
            dry_run: bool = False, sleep=time.sleep, log=print) -> Dict[str, Any]:
    old_keys = [f["key"] for f in index.files_oldest_first() if f["key"].startswith(LEGACY_PREFIX)]
    plan = [(k, new_key(k)) for k in old_keys]
    skipped = [k for k, n in plan if n is None]
    plan = [(k, n) for k, n in plan if n is not None]
    stats = {"legacy": len(old_keys), "unparsed": len(skipped), "moved": 0, "missing": 0, "deleted": 0}
    log(f"[migrate] старых файлов {len(old_keys)}, к переносу {len(plan)}, не по схеме {len(skipped)}")
    if dry_run:
        for k, n in plan[:5]:
            log(f"  {k}\n→ {n}")
        return stats
    moved: List[str] = []

    def one(pair):
        old, new = pair
        try:
            store.copy(old, new)
        except Exception as e:  # удалён ретеншном посреди переноса и т.п.
            return old, None, e
        index.rename_file(old, new)
        return old, new, None

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for i, (old, new, err) in enumerate(pool.map(one, plan), 1):
            if err is None:
                moved.append(old)
            else:
                stats["missing"] += 1
            if i % 5000 == 0:
                log(f"[migrate] {i}/{len(plan)} за {time.monotonic() - started:.0f} с")
    stats["moved"] = len(moved)
    log(f"[migrate] скопировано {len(moved)}, пропущено {stats['missing']}; "
        f"удаление старых через {delete_grace:.0f} с")
    sleep(delete_grace)
    for i in range(0, len(moved), 1000):
        store.delete(moved[i:i + 1000])
    stats["deleted"] = len(moved)
    log(f"[migrate] готово: {stats}")
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()
    settings = Settings()
    migrate(make_store(settings), Index(settings.db_path), workers=args.workers, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
