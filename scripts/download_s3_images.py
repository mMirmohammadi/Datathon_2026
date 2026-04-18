"""Download listing images from the challenge S3 bucket into raw_data/s3/.

Resumable: skips files that already exist locally with the same size.
Parallel: uses a thread pool (network-bound, not CPU-bound).
"""
from __future__ import annotations

import concurrent.futures as cf
import os
import sys
import threading
import time
from pathlib import Path

import boto3
from botocore.config import Config

BUCKET = os.environ.get("LISTINGS_S3_BUCKET", "crawl-data-951752554117-eu-central-2-an")
REGION = os.environ.get("LISTINGS_S3_REGION", "eu-central-2")
PREFIXES = ["prod/comparis/images/", "prod/robinreal/images/"]
DEST_ROOT = Path(__file__).resolve().parent.parent / "raw_data" / "s3"
WORKERS = 32

# Thread-safe counters
lock = threading.Lock()
stats = {"downloaded": 0, "skipped": 0, "failed": 0, "bytes": 0}


def make_client():
    return boto3.client(
        "s3",
        region_name=REGION,
        config=Config(max_pool_connections=WORKERS * 2, retries={"max_attempts": 5, "mode": "adaptive"}),
    )


def download_one(s3, key: str, size: int) -> None:
    dest = DEST_ROOT / key
    try:
        if dest.exists() and dest.stat().st_size == size:
            with lock:
                stats["skipped"] += 1
            return
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        s3.download_file(BUCKET, key, str(tmp))
        tmp.replace(dest)
        with lock:
            stats["downloaded"] += 1
            stats["bytes"] += size
    except Exception as exc:
        with lock:
            stats["failed"] += 1
        print(f"[WARN] download_one: expected=ok, got={type(exc).__name__}: {exc}, fallback=skip key={key}", flush=True)


def list_all_keys(s3, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            yield obj["Key"], obj["Size"]


def main() -> int:
    t0 = time.time()
    s3 = make_client()

    print(f"Listing {len(PREFIXES)} prefixes from s3://{BUCKET}/ ...", flush=True)
    all_items: list[tuple[str, int]] = []
    for prefix in PREFIXES:
        items = list(list_all_keys(s3, prefix))
        print(f"  {prefix}: {len(items):,} objects", flush=True)
        all_items.extend(items)

    total_bytes = sum(s for _, s in all_items)
    print(f"Total: {len(all_items):,} objects, {total_bytes/1e9:.2f} GB", flush=True)
    print(f"Destination: {DEST_ROOT}", flush=True)
    print(f"Workers: {WORKERS}\n", flush=True)

    last_report = time.time()
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(download_one, s3, k, s) for k, s in all_items]
        for i, _ in enumerate(cf.as_completed(futures), 1):
            now = time.time()
            if now - last_report > 10 or i == len(futures):
                with lock:
                    d, sk, f, b = stats["downloaded"], stats["skipped"], stats["failed"], stats["bytes"]
                pct = 100 * i / len(futures)
                elapsed = now - t0
                rate_mb = b / elapsed / 1e6 if elapsed else 0
                print(
                    f"[{elapsed:6.0f}s] {i:>6}/{len(futures)} ({pct:5.1f}%) "
                    f"dl={d} skip={sk} fail={f} | {b/1e9:.2f} GB @ {rate_mb:.1f} MB/s",
                    flush=True,
                )
                last_report = now

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s. {stats}", flush=True)
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
