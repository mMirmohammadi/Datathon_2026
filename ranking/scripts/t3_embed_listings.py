"""T3.1 — Multilingual listing embeddings via Arctic-Embed-L v2.

One-shot offline pass. Builds a fp16 numpy matrix + parallel ids.json so the
query-time cosine search (in runtime/embedding_search.py) is a pure 50 MB
memory read + one matmul (~2 ms).

Why Arctic-Embed-L v2 (per the research agent):
  * Beats bge-m3 by 32 % on CLEF DE/FR/IT/EN (0.541 vs 0.410).
  * Apache-2.0, 568 M params, 1024-d output, 8192 ctx.
  * Supports Matryoshka compression (1024 → 256 with <2 % quality loss); we
    keep full 1024 here because 25k docs × 1024 × fp16 = 50 MB — trivial.
  * Requires a `"query: "` prefix on queries (NOT on documents). We follow
    the model card; the query wrapper lives in runtime/embedding_search.py.

Document template (what we embed per listing):
    f"{title}\n{city}, {canton}\n{rooms} rooms · {area}m² · CHF {price}\n"
    f"{features_csv}\n{description[:800]}"

Using the **enriched** city/canton/features so reverse-geocoded SRED rows
get meaningful locality context.

Per CLAUDE.md §5:
  * Every skipped listing (empty text) emits a [WARN].
  * Hash each doc so we can detect later which listings drifted after a
    re-index.
  * Fails loud if the model load fails or produces the wrong embedding dim.

Usage:
    python -m ranking.scripts.t3_embed_listings --db data/listings.db
    python -m ranking.scripts.t3_embed_listings --db data/listings.db --limit 200   # smoke
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from ranking.common.db import connect
from ranking.schema import check_db_matches_registry

EMBEDDINGS_NPY = Path("data/ranking/embeddings.fp16.npy")
EMBEDDINGS_IDS = Path("data/ranking/embeddings_ids.json")
MODEL_ID = os.getenv(
    "EMBED_MODEL_ID",
    "Snowflake/snowflake-arctic-embed-l-v2.0",
)
EXPECTED_DIM = 1024
BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "8"))


def _format_doc(row: dict) -> str:
    """The single source of truth for the text we embed per listing.

    MUST match the prompt-prefix convention of the chosen encoder. Arctic
    wants the document AS IS (no prefix); the query gets "query: " prefixed
    at retrieval time.
    """
    title       = (row.get("title") or "").strip()
    city        = (row.get("city_filled") or "").strip()
    canton      = (row.get("canton_filled") or "").strip()
    rooms       = row.get("rooms")
    area        = row.get("area")
    price       = row.get("price")
    description = (row.get("description") or "").strip()
    # Enriched features JSON (from the original listing)
    features    = row.get("features_json") or ""
    try:
        feats = ", ".join(json.loads(features)) if features else ""
    except json.JSONDecodeError:
        feats = ""

    loc_line = f"{city}, {canton}" if canton else (city or "")
    stats_bits = []
    if rooms is not None: stats_bits.append(f"{rooms} rooms")
    if area  is not None: stats_bits.append(f"{int(area)}m²")
    if price is not None: stats_bits.append(f"CHF {int(price)}")
    stats = " · ".join(stats_bits)

    # Truncate description at 800 chars (per the plan in ARCHITECTURE.md).
    desc_head = description[:800]
    return "\n".join(filter(None, [title, loc_line, stats, feats, desc_head]))


def _doc_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _load_rows(conn, *, limit: int | None) -> list[dict]:
    sql = """
        SELECT
            l.listing_id, l.title, l.description, l.rooms, l.area, l.price,
            l.features_json,
            le.city_filled, le.canton_filled
        FROM listings l
        JOIN listings_enriched le USING(listing_id)
        WHERE l.title IS NOT NULL AND l.title != ''
        ORDER BY l.listing_id
    """
    params: tuple = ()
    if limit is not None:
        sql += " LIMIT ?"
        params = (int(limit),)
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def run(db_path: Path, *, limit: int | None = None) -> dict:
    t_start = time.monotonic()

    EMBEDDINGS_NPY.parent.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] t3_embed_listings: model={MODEL_ID} batch={BATCH_SIZE} limit={limit}", flush=True)

    # Load model — loud if it fails (no silent fallback to a different model).
    t0 = time.monotonic()
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise RuntimeError(
            f"sentence_transformers not installed: {exc}. "
            "pip install sentence-transformers torch"
        )
    try:
        model = SentenceTransformer(MODEL_ID, trust_remote_code=True)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load embedding model {MODEL_ID!r}: {type(exc).__name__}: {exc}. "
            "Check network / HF cache."
        )
    print(
        f"[INFO] t3_embed_listings: model loaded in {time.monotonic() - t0:.1f}s",
        flush=True,
    )

    with connect(db_path) as conn:
        check_db_matches_registry(conn)
        rows = _load_rows(conn, limit=limit)
        print(f"[INFO] t3_embed_listings: {len(rows):,} listings to embed", flush=True)

        docs: list[str] = []
        ids: list[str] = []
        hashes: list[str] = []
        skipped = 0
        for r in rows:
            text = _format_doc(r)
            if len(text.strip()) < 10:
                skipped += 1
                print(
                    f"[WARN] t3_embed_listings: expected=non-trivial text, "
                    f"got=<10 chars, fallback=skip listing_id={r['listing_id']}",
                    flush=True,
                )
                continue
            docs.append(text)
            ids.append(r["listing_id"])
            hashes.append(_doc_hash(text))

        if not docs:
            raise RuntimeError("No documents to embed — all skipped as trivial?")

        # Encode in batches
        t0 = time.monotonic()
        embs = model.encode(
            docs,
            batch_size=BATCH_SIZE,
            normalize_embeddings=True,
            show_progress_bar=True,
        )
        if embs.shape[1] != EXPECTED_DIM:
            raise RuntimeError(
                f"Embedding dim mismatch: expected {EXPECTED_DIM}, got {embs.shape[1]}. "
                "Wrong model?"
            )
        elapsed_encode = time.monotonic() - t0
        print(
            f"[INFO] t3_embed_listings: encoded {len(docs):,} docs "
            f"({embs.shape[1]}-d fp32) in {elapsed_encode:.1f}s "
            f"= {len(docs)/elapsed_encode:.1f} docs/s",
            flush=True,
        )

        # Save fp16 matrix + ids
        fp16 = embs.astype(np.float16)
        np.save(EMBEDDINGS_NPY, fp16)
        with EMBEDDINGS_IDS.open("w", encoding="utf-8") as fh:
            json.dump(ids, fh)
        matrix_mb = fp16.nbytes / (1024 * 1024)
        print(
            f"[INFO] t3_embed_listings: wrote {EMBEDDINGS_NPY} "
            f"({matrix_mb:.1f} MB fp16) + {EMBEDDINGS_IDS}",
            flush=True,
        )

        # Write row_index + hash + model id back to listings_ranking_signals
        t0 = time.monotonic()
        now_iso = datetime.now(timezone.utc).isoformat()
        conn.execute("BEGIN;")
        for idx, (lid, h) in enumerate(zip(ids, hashes)):
            conn.execute(
                """
                UPDATE listings_ranking_signals SET
                    embedding_row_index = ?,
                    embedding_model     = ?,
                    embedding_doc_hash  = ?,
                    last_updated_utc    = ?
                WHERE listing_id = ?;
                """,
                (idx, MODEL_ID, h, now_iso, lid),
            )
        conn.commit()
        print(
            f"[INFO] t3_embed_listings: wrote {len(ids):,} row_index entries "
            f"in {time.monotonic() - t0:.1f}s",
            flush=True,
        )

    total = time.monotonic() - t_start
    stats = {
        "listings_embedded": len(ids),
        "skipped":           skipped,
        "matrix_mb":         round(matrix_mb, 1),
        "elapsed_s":         round(total, 1),
        "model":             MODEL_ID,
    }
    print(f"[INFO] t3_embed_listings: DONE {stats}", flush=True)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] t3_embed_listings: db not found at {args.db}", file=sys.stderr)
        return 2
    try:
        run(args.db, limit=args.limit)
    except RuntimeError as exc:
        print(f"[ERROR] t3_embed_listings: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
