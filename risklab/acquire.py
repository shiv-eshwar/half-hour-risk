"""Verify publisher SHA-256s and convert monthly archives to splittable Parquet."""

import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as acsv
import pyarrow.parquet as pq

BASE = "https://data.binance.vision/data/spot/monthly"
MANIFEST = Path("sources/manifest.json")
TRADE = {"agg_id": pa.int64(), "price": pa.float64(), "qty": pa.float64(), "first_id": pa.int64(),
         "last_id": pa.int64(), "ms": pa.int64(), "buyer_maker": pa.bool_(), "best_match": pa.bool_()}
BAR = {"ms": pa.int64(), "open": pa.float64(), "high": pa.float64(), "low": pa.float64(),
       "close": pa.float64(), "volume": pa.float64(), "close_ms": pa.int64(),
       "quote": pa.float64(), "trades": pa.int64(), "buy_volume": pa.float64(),
       "buy_quote": pa.float64(), "ignore": pa.float64()}


def inventory():
    for month in range(1, 7):
        date = f"2024-{month:02}"
        for symbol, kind in (("BTCUSDT", "aggTrades"), ("BTCUSDT", "1m"), ("ETHUSDT", "1m")):
            name = f"{symbol}-{kind}-{date}.zip"
            folder = f"aggTrades/{symbol}" if kind == "aggTrades" else f"klines/{symbol}/1m"
            yield name, f"{BASE}/{folder}/{name}", "tape" if kind == "aggTrades" else symbol, date


def curl(url, destination=None):
    cmd = ["curl", "-fsSL", "--retry", "4", "--connect-timeout", "20"]
    if destination:
        cmd += ["-o", str(destination)]
    return subprocess.check_output([*cmd, url])


def digest(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def temporary(directory):
    with tempfile.NamedTemporaryFile(dir=directory, prefix=".stage-", delete=False) as tmp:
        return Path(tmp.name)


def convert(archive, member, destination, types, sha):
    if destination.is_file():
        saved = pq.ParquetFile(destination)
        if saved.schema_arrow.metadata.get(b"source_sha256") == sha.encode():
            return saved.metadata.num_rows
    temp = temporary(destination.parent)
    try:
        with archive.open(member) as file:
            reader = acsv.open_csv(file, read_options=acsv.ReadOptions(
                column_names=list(types), block_size=16 << 20, use_threads=False),
                convert_options=acsv.ConvertOptions(column_types=types))
            schema = reader.schema.with_metadata({b"source_sha256": sha.encode()})
            rows = 0
            with pq.ParquetWriter(temp, schema, compression="snappy") as writer:
                for batch in reader:
                    writer.write_batch(batch)
                    rows += batch.num_rows
        temp.replace(destination)
        return rows
    finally:
        temp.unlink(missing_ok=True)


def acquire(item, pinned):
    name, url, group, month = item
    raw, parquet = Path("data/raw"), Path("data/parquet") / group
    parquet.mkdir(parents=True, exist_ok=True)
    checksum = curl(url + ".CHECKSUM").decode().strip().split()
    if len(checksum) != 2 or not re.fullmatch("[0-9a-fA-F]{64}", checksum[0]) or checksum[1].lstrip("*") != name:
        raise ValueError(f"Malformed checksum for {url}")
    sha = checksum[0].lower()
    if pinned is not None and pinned["sha256"] != sha:
        raise ValueError(f"The publisher revised {name}. Review the pinned study snapshot.")
    archive = raw / name
    if archive.is_file():
        if digest(archive) != sha:
            raise ValueError(f"Local checksum mismatch for {archive}")
    else:
        temp = temporary(raw)
        try:
            curl(url, temp)
            if digest(temp) != sha:
                raise ValueError(f"Download checksum mismatch for {url}")
            temp.replace(archive)
        finally:
            temp.unlink(missing_ok=True)
    with zipfile.ZipFile(archive) as file:
        member = name.removesuffix(".zip") + ".csv"
        info = file.getinfo(member)
        rows = convert(file, member, parquet / f"{month}.parquet", TRADE if group == "tape" else BAR, sha)
    entry = {"name": name, "group": group, "month": month, "url": url, "sha256": sha,
             "zip_bytes": archive.stat().st_size, "csv_bytes": info.file_size, "rows": rows}
    if pinned is not None and entry != pinned:
        raise ValueError(f"Manifest mismatch for {name}")
    print(f"Verified {name}: {rows:,} rows", flush=True)
    return entry


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--jobs", type=int, default=3)
    p.add_argument("--record-manifest", action="store_true", help="record the initial study snapshot")
    args = p.parse_args()
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    inputs = list(inventory())
    expected = {} if args.record_manifest else {x["name"]: x for x in json.loads(MANIFEST.read_text())["files"]}
    if not args.record_manifest and set(expected) != {i[0] for i in inputs}:
        raise ValueError("Incomplete study manifest")
    with ThreadPoolExecutor(args.jobs) as pool:
        entries = list(pool.map(lambda i: acquire(i, expected.get(i[0])), inputs))
    if args.record_manifest:
        MANIFEST.parent.mkdir(exist_ok=True)
        temp = temporary(MANIFEST.parent)
        temp.write_text(json.dumps({"files": entries}, indent=2) + "\n")
        os.replace(temp, MANIFEST)


if __name__ == "__main__":
    main()
