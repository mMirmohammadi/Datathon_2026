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

Feature tokens come from the **enriched** `feature_*_filled` columns
(source ∈ {original, text_gpt_5_4}). Any `_filled == "1"` contributes
its tag to `features_csv`. This matters because 18,811 of 25,546
listings (73.6%) had empty raw `features_json` and rely on Pass 2 GPT
to surface feature flags from the description.

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
    python -m ranking.scripts.t3_embed_listings --db data/listings.db --refresh-stale
        # recompute doc_hash for every row, re-embed only those whose stored hash
        # differs (keeps row_index stable so ids.json does not change).
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
BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "16"))

# Enriched feature columns whose `_filled == "1"` contributes the tag to the
# embedding doc. Ordered so the CSV string is deterministic across runs.
FEATURE_TAGS: tuple[str, ...] = (
    "balcony", "elevator", "parking", "garage", "fireplace",
    "child_friendly", "pets_allowed", "temporary", "new_build",
    "wheelchair_accessible", "private_laundry", "minergie_certified",
)


def _format_doc(row: dict) -> str:
    """The single source of truth for the text we embed per listing.

    MUST match the prompt-prefix convention of the chosen encoder. Arctic
    wants the document AS IS (no prefix); the query gets "query: " prefixed
    at retrieval time.

    Feature tokens come from the enriched `feature_<tag>_filled` columns —
    any value of "1" contributes its tag to the CSV. Sources include both
    the raw listings CSV (`original`) and GPT-5.4 extraction (`text_gpt_5_4`);
    we treat them equally for retrieval purposes.
    """
    title       = (row.get("title") or "").strip()
    city        = (row.get("city_filled") or "").strip()
    canton      = (row.get("canton_filled") or "").strip()
    rooms       = row.get("rooms")
    area        = row.get("area")
    price       = row.get("price")
    description = (row.get("description") or "").strip()

    feats = ", ".join(
        tag for tag in FEATURE_TAGS
        if row.get(f"feature_{tag}_filled") == "1"
    )

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


def _feature_cols_sql() -> str:
    """Comma-separated `le.feature_<tag>_filled` list for inclusion in SELECT."""
    return ",\n            ".join(
        f"le.feature_{tag}_filled AS feature_{tag}_filled" for tag in FEATURE_TAGS
    )


def _load_rows(conn, *, limit: int | None) -> list[dict]:
    sql = f"""
        SELECT
            l.listing_id, l.title, l.description, l.rooms, l.area, l.price,
            le.city_filled, le.canton_filled,
            {_feature_cols_sql()}
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


def _load_rows_with_stored_hash(conn, *, limit: int | None) -> list[dict]:
    """Same rows as _load_rows, but also fetches the stored embedding_doc_hash."""
    sql = f"""
        SELECT
            l.listing_id, l.title, l.description, l.rooms, l.area, l.price,
            le.city_filled, le.canton_filled,
            {_feature_cols_sql()},
            lrs.embedding_doc_hash AS stored_hash,
            lrs.embedding_row_index AS stored_row_index
        FROM listings l
        JOIN listings_enriched le USING(listing_id)
        JOIN listings_ranking_signals lrs USING(listing_id)
        WHERE l.title IS NOT NULL AND l.title != ''
        ORDER BY l.listing_id
    """
    params: tuple = ()
    if limit is not None:
        sql += " LIMIT ?"
        params = (int(limit),)
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def run_refresh_stale(db_path: Path) -> dict:
    """Re-embed only rows whose current doc_hash != stored embedding_doc_hash.

    Matrix row_index is preserved so ids.json does not change. Runtime
    `embedding_search.py` can keep its cached matrix pointer; only the stale
    rows' 1024-d vectors get patched in place.
    """
    t_start = time.monotonic()
    if not EMBEDDINGS_NPY.exists() or not EMBEDDINGS_IDS.exists():
        raise RuntimeError(
            f"--refresh-stale requires existing artifacts at {EMBEDDINGS_NPY} "
            f"and {EMBEDDINGS_IDS}. Run a full pass first."
        )

    matrix = np.load(EMBEDDINGS_NPY)
    with EMBEDDINGS_IDS.open("r", encoding="utf-8") as fh:
        stored_ids: list[str] = json.load(fh)
    if matrix.shape != (len(stored_ids), EXPECTED_DIM):
        raise RuntimeError(
            f"Matrix shape {matrix.shape} disagrees with ids.json len={len(stored_ids)} "
            f"× expected_dim={EXPECTED_DIM}. Refusing to patch — run a full re-embed."
        )
    id_to_row = {lid: i for i, lid in enumerate(stored_ids)}

    with connect(db_path) as conn:
        check_db_matches_registry(conn)
        rows = _load_rows_with_stored_hash(conn, limit=None)
        print(
            f"[INFO] t3_embed_listings: loaded {len(rows):,} listings; "
            f"computing current doc_hash per row",
            flush=True,
        )

        stale_docs: list[str] = []
        stale_ids: list[str] = []
        stale_hashes: list[str] = []
        stale_row_indices: list[int] = []
        for r in rows:
            text = _format_doc(r)
            if len(text.strip()) < 10:
                # Same guard as full path — do not silently drop.
                print(
                    f"[WARN] t3_embed_listings: expected=non-trivial text, "
                    f"got=<10 chars, fallback=skip listing_id={r['listing_id']}",
                    flush=True,
                )
                continue
            current_hash = _doc_hash(text)
            if current_hash == r.get("stored_hash"):
                continue
            row_idx = id_to_row.get(r["listing_id"])
            if row_idx is None:
                # Row present in DB but not in the saved ids.json: fall-through
                # to a full re-embed would be required. Fail loud.
                raise RuntimeError(
                    f"listing_id={r['listing_id']} is stale but missing from "
                    f"{EMBEDDINGS_IDS}. Run a full re-embed."
                )
            stale_docs.append(text)
            stale_ids.append(r["listing_id"])
            stale_hashes.append(current_hash)
            stale_row_indices.append(row_idx)

        print(
            f"[INFO] t3_embed_listings: refresh-stale found {len(stale_docs):,} "
            f"stale rows (out of {len(rows):,})",
            flush=True,
        )
        if not stale_docs:
            return {
                "stale_found": 0, "rows_re_embedded": 0, "elapsed_s": round(time.monotonic() - t_start, 1),
                "model": MODEL_ID,
            }

        # Load model only when we actually need it.
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

        t0 = time.monotonic()
        embs = model.encode(
            stale_docs,
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
            f"[INFO] t3_embed_listings: encoded {len(stale_docs):,} stale docs in "
            f"{elapsed_encode:.1f}s = {len(stale_docs)/elapsed_encode:.1f} docs/s",
            flush=True,
        )

        # Patch the matrix in place at each stale row_index (row_index preserved).
        fp16 = embs.astype(np.float16)
        for row_idx, vec in zip(stale_row_indices, fp16):
            matrix[row_idx] = vec
        np.save(EMBEDDINGS_NPY, matrix)
        matrix_mb = matrix.nbytes / (1024 * 1024)
        print(
            f"[INFO] t3_embed_listings: patched {len(stale_ids):,} rows of matrix "
            f"({matrix_mb:.1f} MB fp16)",
            flush=True,
        )

        # Update stored doc_hash + last_updated_utc for just the patched rows.
        t0 = time.monotonic()
        now_iso = datetime.now(timezone.utc).isoformat()
        conn.execute("BEGIN;")
        for lid, h in zip(stale_ids, stale_hashes):
            conn.execute(
                """
                UPDATE listings_ranking_signals SET
                    embedding_doc_hash  = ?,
                    last_updated_utc    = ?
                WHERE listing_id = ?;
                """,
                (h, now_iso, lid),
            )
        conn.commit()
        print(
            f"[INFO] t3_embed_listings: updated {len(stale_ids):,} DB rows in "
            f"{time.monotonic() - t0:.1f}s",
            flush=True,
        )

    total = time.monotonic() - t_start
    stats = {
        "stale_found":        len(stale_ids),
        "rows_re_embedded":   len(stale_ids),
        "matrix_mb":          round(matrix_mb, 1),
        "elapsed_s":          round(total, 1),
        "model":              MODEL_ID,
    }
    print(f"[INFO] t3_embed_listings: refresh-stale DONE {stats}", flush=True)
    return stats


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
    parser.add_argument(
        "--refresh-stale",
        action="store_true",
        help="Re-embed only rows whose doc_hash differs from the stored one. "
             "Preserves row_index (ids.json unchanged).",
    )
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] t3_embed_listings: db not found at {args.db}", file=sys.stderr)
        return 2
    if args.refresh_stale and args.limit is not None:
        print("[ERROR] --refresh-stale and --limit are mutually exclusive", file=sys.stderr)
        return 2
    try:
        if args.refresh_stale:
            run_refresh_stale(args.db)
        else:
            run(args.db, limit=args.limit)
    except RuntimeError as exc:
        print(f"[ERROR] t3_embed_listings: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
