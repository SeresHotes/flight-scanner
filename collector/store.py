"""Объектное хранилище озера: S3 (Yandex Object Storage) или локальный каталог.

Один интерфейс для записи файлов, листинга по префиксу, удаления и чтения
Parquet (pyarrow NativeFile). Файл озера — одна маленькая серия (десятки КБ), поэтому
из S3 он читается целиком одним GET: pyarrow S3FileSystem ходил отдельным range-GET
за футером и каждой колонкой — ~0.7 с на серию против ~30 мс (замер на проде)."""
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

import pyarrow as pa


@dataclass
class ObjectInfo:
    key: str
    size: int
    modified: Optional[datetime]


class LocalStore:
    """Каталог на диске (dev, тесты). Ключи — относительные пути."""

    def __init__(self, root: str):
        self.root = Path(root)

    def put_bytes(self, key: str, data: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)

    def open_input_file(self, key: str) -> pa.NativeFile:
        return pa.memory_map(str(self.root / key), "r")

    def list(self, prefix: str) -> List[ObjectInfo]:
        base = self.root / prefix
        if not base.exists():
            return []
        out = []
        for p in sorted(base.rglob("*")):
            if p.is_file() and not p.name.endswith(".tmp"):
                st = p.stat()
                out.append(ObjectInfo(str(p.relative_to(self.root)), st.st_size,
                                      datetime.fromtimestamp(st.st_mtime, timezone.utc)))
        return out

    def delete(self, keys: Iterable[str]) -> None:
        for k in keys:
            try:
                (self.root / k).unlink()
            except FileNotFoundError:
                pass


class S3Store:
    """Object Storage через boto3: put/list/delete и чтение файла одним GET."""

    def __init__(self, bucket: str, access_key: str, secret_key: str,
                 endpoint: str = "https://storage.yandexcloud.net", region: str = "ru-central1"):
        import boto3
        self.bucket = bucket
        self._s3 = boto3.client("s3", endpoint_url=endpoint, region_name=region,
                                aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    def put_bytes(self, key: str, data: bytes) -> None:
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def open_input_file(self, key: str) -> pa.NativeFile:
        body = self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return pa.BufferReader(body)

    def list(self, prefix: str) -> List[ObjectInfo]:
        out = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents") or []:
                out.append(ObjectInfo(obj["Key"], int(obj["Size"]), obj.get("LastModified")))
        return out

    def delete(self, keys: Iterable[str]) -> None:
        keys = list(keys)
        for i in range(0, len(keys), 1000):
            chunk = [{"Key": k} for k in keys[i:i + 1000]]
            if chunk:
                self._s3.delete_objects(Bucket=self.bucket, Delete={"Objects": chunk, "Quiet": True})


def make_store(settings):
    """S3, если заданы все S3_*; иначе локальный каталог LAKE_LOCAL_ROOT."""
    if settings.s3_configured:
        return S3Store(settings.s3_bucket, settings.s3_access_key, settings.s3_secret_key,
                       endpoint=settings.s3_endpoint or "https://storage.yandexcloud.net",
                       region=settings.s3_region or "ru-central1")
    return LocalStore(settings.lake_local_root)
