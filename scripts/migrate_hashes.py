"""
scripts/migrate_hashes.py — one-shot content_hash recompute.

Run once after changing HASH_FIELDS in utils/db.py. Pages through every
active row for the given source, recomputes content_hash using the current
Python formula, and writes it back. After this, the next scrape sees
matching hashes and skips those rows as "unchanged" instead of flagging
the whole catalogue as "updated".

Usage:
    python -m scripts.migrate_hashes                 # default: dubizzle
    python -m scripts.migrate_hashes carswitch       # specific source
    python -m scripts.migrate_hashes --all           # every source

Env:
    SUPABASE_URL, SUPABASE_SERVICE_KEY  (same as main.py; loaded from .env)
"""

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from utils.db import compute_hash, get_client, _safe_exec  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("migrate_hashes")

PAGE_SIZE = 500


def migrate_source(client, source: str) -> dict:
    """Recompute content_hash for every active row of `source`. Returns counts."""
    offset = 0
    updated = 0
    skipped = 0
    total = 0

    while True:
        batch = _safe_exec(
            client.table("car_listings")
            .select("id, price_aed, price_aed_max, mileage_km, content_hash")
            .eq("source", source)
            .eq("is_active", True)
            .range(offset, offset + PAGE_SIZE - 1)
        ).data or []

        if not batch:
            break

        total += len(batch)
        for row in batch:
            new_hash = compute_hash(row)
            if row.get("content_hash") == new_hash:
                skipped += 1
                continue
            _safe_exec(
                client.table("car_listings")
                .update({"content_hash": new_hash})
                .eq("id", row["id"])
            )
            updated += 1

        logger.info(f"[{source}] processed {total} rows (updated={updated}, skipped={skipped})")

        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return {"total": total, "updated": updated, "skipped": skipped}


def main():
    parser = argparse.ArgumentParser(description="Recompute content_hash for existing rows")
    parser.add_argument("source", nargs="?", default="dubizzle")
    parser.add_argument("--all", action="store_true",
                        help="Migrate every source, not just the given one")
    args = parser.parse_args()

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        logger.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set (check .env)")
        sys.exit(1)

    client = get_client(url, key)

    if args.all:
        sources = (
            _safe_exec(
                client.table("car_listings").select("source").eq("is_active", True)
            ).data or []
        )
        unique = sorted({r["source"] for r in sources})
        logger.info(f"Migrating sources: {unique}")
    else:
        unique = [args.source]

    grand = {"total": 0, "updated": 0, "skipped": 0}
    for src in unique:
        logger.info(f"▶ {src}")
        r = migrate_source(client, src)
        logger.info(
            f"✓ {src}: total={r['total']} updated={r['updated']} skipped={r['skipped']}"
        )
        for k in grand:
            grand[k] += r[k]

    logger.info("─" * 50)
    logger.info(
        f"DONE — total={grand['total']} updated={grand['updated']} skipped={grand['skipped']}"
    )


if __name__ == "__main__":
    main()
