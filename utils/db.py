"""
utils/db.py — Supabase operations

Fixes applied vs v1:
  - Batch SELECT by external_id (single query per source batch, not N+1)
  - Paginated soft-delete (handles >1000 active listings)
  - Dict copy before mutation (never pollutes caller's object)
  - JSONB specs whitelist (no 200KB Next.js blobs)
  - Safety threshold: abort soft-delete if >35% would be deleted (likely scraper break)
  - image_urls filtered to valid http/https only
  - price range detection (store min+max)
"""

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from postgrest.exceptions import APIError
from supabase import create_client, Client
from tenacity import (
    before_sleep_log, retry, retry_if_exception, stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Transient error retry — Supabase sits behind Cloudflare, which occasionally
# returns 502/503/504 (esp. during long-running scrapes). A single bad gateway
# must not kill a 30-minute run.
# ─────────────────────────────────────────────────────────────────────────────

def _is_transient(exc: BaseException) -> bool:
    if not isinstance(exc, APIError):
        return False
    code_raw = getattr(exc, "code", None)
    try:
        code = int(code_raw)
    except (TypeError, ValueError):
        code = None
    if code in (502, 503, 504):
        return True
    msg = str(exc)
    return any(
        s in msg for s in ("502 Bad Gateway", "503 Service", "504 Gateway Timeout")
    )


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _safe_exec(builder):
    """Execute a PostgREST query builder, retrying transient 5xx errors."""
    return builder.execute()

# Max keys stored in the specs JSONB column per listing
SPECS_WHITELIST = {
    "regional_specs", "warranty", "service_history", "no_of_owners",
    "steering_side", "import", "insurance", "stickers", "accident",
    "horsepower", "torque", "trim_level", "sunroof", "leather_seats",
    "navigation", "camera", "parking_sensors", "cruise_control",
    "bluetooth", "usb", "apple_carplay", "android_auto",
}

# If more than this fraction of active listings would be soft-deleted in a single
# run, abort and alert — almost certainly a scraper breakage, not mass delistings.
SOFT_DELETE_SAFETY_THRESHOLD = 0.35


def get_client(url: str, key: str) -> Client:
    return create_client(url, key)


# ─────────────────────────────────────────────────────────────────────────────
# Hash
# ─────────────────────────────────────────────────────────────────────────────

# Fields included in the change-detection hash.
# Kept list-level only (fields reliably present on every scrape, with or without
# detail enrichment) so detail-scrape vs list-scrape of the same listing
# produces identical hashes and doesn't flap the `updated` counter.
HASH_FIELDS = ("price_aed", "price_aed_max", "mileage_km")


def compute_hash(listing: dict) -> str:
    """MD5 of list-level fields that signal a meaningful commercial change."""
    fields = [listing.get(f) for f in HASH_FIELDS]
    raw = json.dumps(fields, sort_keys=True, default=str)
    return hashlib.md5(raw.encode()).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# Sanitisers
# ─────────────────────────────────────────────────────────────────────────────

def _sanitise(listing: dict) -> dict:
    """Return a clean copy of a listing dict, safe for Supabase insert/update."""
    d = dict(listing)

    # Filter image_urls to valid absolute URLs only
    imgs = d.get("image_urls") or []
    d["image_urls"] = [u for u in imgs if isinstance(u, str) and u.startswith(("http://", "https://"))][:20]

    # Whitelist specs keys + ensure JSON-serialisable
    raw_specs = d.get("specs") or {}
    if isinstance(raw_specs, dict):
        filtered = {k: v for k, v in raw_specs.items() if k.lower() in SPECS_WHITELIST}
        try:
            json.dumps(filtered)  # validate serialisability
            d["specs"] = filtered
        except (TypeError, ValueError):
            d["specs"] = {}
    else:
        d["specs"] = {}

    # Ensure price is numeric or None (strip commas before casting)
    for pf in ("price_aed", "price_aed_max"):
        val = d.get(pf)
        if val is not None:
            try:
                d[pf] = float(str(val).replace(",", ""))
            except (TypeError, ValueError):
                d[pf] = None

    # Strip None values from top-level to avoid overwriting existing DB values
    # with NULL on partial updates — keep them explicit for INSERT
    return d


# ─────────────────────────────────────────────────────────────────────────────
# Batch upsert (single SELECT per batch, not N+1)
# ─────────────────────────────────────────────────────────────────────────────

def upsert_listings(client: Client, listings: list[dict]) -> dict:
    """
    Upsert a batch of listings efficiently:
      - One SELECT to load all existing records for this source+batch
      - Bulk INSERT for new listings
      - Individual UPDATE only for changed listings (hash differs)
      - Touch (last_seen_at only) for unchanged listings
    Returns counts: {new, updated, skipped}
    """
    if not listings:
        return {"new": 0, "updated": 0, "skipped": 0}

    counts = {"new": 0, "updated": 0, "skipped": 0}
    # External IDs that diffed on hash — caller (dubizzle._run) feeds these
    # into the detail-enrichment queue so updated rows get fresh image URLs.
    updated_external_ids: set[str] = set()
    source = listings[0]["source"]
    now_iso = datetime.now(timezone.utc).isoformat()

    # Build lookup: external_id → {id, content_hash}.
    # PostgREST passes `in.(...)` as a query string; at ~35 chars per 32-char
    # hex ID the URL caps out well before 1000 IDs. Chunk to 100.
    ext_ids = [l["external_id"] for l in listings]
    existing: dict = {}
    for chunk in _chunks(ext_ids, 100):
        rows = _safe_exec(
            client.table("car_listings")
            .select("id, external_id, content_hash")
            .eq("source", source)
            .in_("external_id", chunk)
        )
        for r in (rows.data or []):
            existing[r["external_id"]] = r

    to_insert = []
    to_touch_ids = []  # only last_seen_at update needed
    changed: list[tuple[str, dict, str]] = []  # (old_id, new_listing, old_hash)

    for raw in listings:
        ext_id = raw["external_id"]
        listing = _sanitise(raw)
        new_hash = compute_hash(listing)
        listing["content_hash"] = new_hash
        listing["last_seen_at"] = now_iso

        if ext_id not in existing:
            listing["first_seen_at"] = now_iso
            listing["last_changed_at"] = now_iso
            listing["is_active"] = True
            to_insert.append(listing)
            counts["new"] += 1

        elif existing[ext_id]["content_hash"] != new_hash:
            listing["last_changed_at"] = now_iso
            listing["is_active"] = True
            changed.append((existing[ext_id]["id"], listing, existing[ext_id]["content_hash"]))
            counts["updated"] += 1
            updated_external_ids.add(ext_id)

        elif listing.get("detail_scraped_at"):
            # List-level hash matches, but detail was freshly scraped this
            # run. Route through the per-row update path so detail fields
            # persist (the bulk-touch path only writes last_seen_at).
            listing["is_active"] = True
            changed.append((existing[ext_id]["id"], listing, existing[ext_id]["content_hash"]))
            counts["updated"] += 1

        else:
            to_touch_ids.append(existing[ext_id]["id"])
            counts["skipped"] += 1

    # Batch-fetch old rows for every update in one query per chunk (not N+1).
    old_rows: dict = {}
    if changed:
        changed_ids = [c[0] for c in changed]
        for chunk in _chunks(changed_ids, 100):
            rows = _safe_exec(
                client.table("car_listings")
                .select(
                    "id,price_aed,mileage_km,description,seller_phone,"
                    "color,condition,image_urls,area,emirate"
                )
                .in_("id", chunk)
            )
            for r in (rows.data or []):
                old_rows[r["id"]] = r

        logger.info(f"[{source}] upsert: applying {len(changed)} updates")
        for old_id, listing, old_hash in changed:
            _safe_exec(
                client.table("car_listings").update(listing).eq("id", old_id)
            )
            changed_fields = _diff(old_rows.get(old_id, {}), listing)
            _log_change(client, old_id, "updated", old_hash, listing["content_hash"], changed_fields)

    # Bulk insert all new listings
    if to_insert:
        result = _safe_exec(
            client.table("car_listings").insert(to_insert)
        )
        for row in (result.data or []):
            _log_change(client, row["id"], "created", None, row.get("content_hash"), {})

    # Bulk touch unchanged listings (single UPDATE per batch via 'in').
    # UUIDs are ~39 chars once URL-encoded — cap at 150 per chunk to stay
    # under PostgREST's URL length limit.
    if to_touch_ids:
        for chunk in _chunks(to_touch_ids, 150):
            _safe_exec(
                client.table("car_listings")
                .update({"last_seen_at": now_iso})
                .in_("id", chunk)
            )

    counts["updated_external_ids"] = updated_external_ids
    return counts


# ─────────────────────────────────────────────────────────────────────────────
# Soft-delete with pagination + safety threshold
# ─────────────────────────────────────────────────────────────────────────────

def soft_delete_missing(client: Client, source: str, live_external_ids: set[str]) -> int:
    """
    Paginate through ALL active listings for this source (handles >1000 rows).
    Soft-delete any not in live_external_ids.
    Aborts if deletion count would exceed SOFT_DELETE_SAFETY_THRESHOLD.
    Returns count of deleted listings.
    """
    now = datetime.now(timezone.utc).isoformat()
    all_stored = []
    page_size = 1000
    offset = 0

    while True:
        batch = _safe_exec(
            client.table("car_listings")
            .select("id, external_id")
            .eq("source", source)
            .eq("is_active", True)
            .range(offset, offset + page_size - 1)
        )
        rows = batch.data or []
        all_stored.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size

    to_delete = [r for r in all_stored if r["external_id"] not in live_external_ids]
    total_active = len(all_stored)

    if total_active > 50 and len(to_delete) / total_active > SOFT_DELETE_SAFETY_THRESHOLD:
        logger.error(
            f"[{source}] SAFETY ABORT: {len(to_delete)}/{total_active} active listings "
            f"({len(to_delete)/total_active:.0%}) would be soft-deleted — "
            f"exceeds {SOFT_DELETE_SAFETY_THRESHOLD:.0%} threshold. "
            f"Likely scraper breakage. No deletions performed."
        )
        return 0

    deleted = 0
    for chunk in _chunks(to_delete, 150):
        ids = [r["id"] for r in chunk]
        _safe_exec(
            client.table("car_listings").update({
                "is_active": False,
                "deleted_at": now,
                "last_seen_at": now,
            }).in_("id", ids)
        )
        for r in chunk:
            _log_change(client, r["id"], "deleted", None, None, {})
        deleted += len(chunk)

    return deleted


# ─────────────────────────────────────────────────────────────────────────────
# Run logging
# ─────────────────────────────────────────────────────────────────────────────

def get_detail_plan(client: Client, source: str, batch_size: int = 100) -> dict:
    """
    Build the per-run detail-scrape plan for a source.

    Returns:
        known_external_ids:    set of all active external_ids already in DB.
                               Used by the scraper to tell "new" listings apart
                               from ones we've seen before.
        backfill_external_ids: up to `batch_size` active listings with no
                               detail_scraped_at, oldest-seen first. These are
                               the next slice of the backfill queue.
    """
    known: set[str] = set()
    offset = 0
    page_size = 1000
    while True:
        rows = _safe_exec(
            client.table("car_listings")
            .select("external_id")
            .eq("source", source)
            .eq("is_active", True)
            .range(offset, offset + page_size - 1)
        ).data or []
        if not rows:
            break
        known.update(r["external_id"] for r in rows)
        if len(rows) < page_size:
            break
        offset += page_size

    backfill_rows = _safe_exec(
        client.table("car_listings")
        .select("external_id")
        .eq("source", source)
        .eq("is_active", True)
        .is_("detail_scraped_at", "null")
        .order("first_seen_at")
        .limit(batch_size)
    ).data or []
    backfill_ids = {r["external_id"] for r in backfill_rows}

    return {
        "known_external_ids": known,
        "backfill_external_ids": backfill_ids,
    }


def build_change_digest(
    client: Client,
    source: str,
    since_iso: str,
    limit_per_section: int = 15,
) -> str:
    """Human-readable summary of every car_listing_changes row written since
    `since_iso`, grouped by change type, restricted to `source`. Returns an
    empty string if no changes. Designed to be appended to the Telegram
    summary so the sales team can cross-check each diff against the website.
    """
    try:
        changes = _safe_exec(
            client.table("car_listing_changes")
            .select("listing_id, change_type, changed_fields, changed_at")
            .gte("changed_at", since_iso)
            .order("changed_at")
        ).data or []
    except Exception as e:
        logger.warning(f"[{source}] change digest query failed: {e}")
        return ""

    if not changes:
        return ""

    # Resolve listing_id → listing info (chunked to stay under URL limits).
    listing_ids = list({c["listing_id"] for c in changes})
    listings: dict = {}
    for chunk in _chunks(listing_ids, 100):
        try:
            rows = _safe_exec(
                client.table("car_listings")
                .select("id, external_id, url, make, model, year, "
                        "price_aed, emirate, area, trim")
                .in_("id", chunk)
                .eq("source", source)
            ).data or []
            for r in rows:
                listings[r["id"]] = r
        except Exception as e:
            logger.warning(f"[{source}] digest listing lookup failed: {e}")

    # Drop changes whose listing isn't from this source (or was hard-deleted).
    changes = [c for c in changes if c["listing_id"] in listings]
    if not changes:
        return ""

    def _header(l: dict) -> str:
        parts = []
        if l.get("year"):
            parts.append(str(l["year"]))
        if l.get("make"):
            parts.append(str(l["make"]).title())
        if l.get("model"):
            parts.append(str(l["model"]))
        if l.get("trim"):
            parts.append(l["trim"])
        return " ".join(parts) or "Listing"

    def _loc(l: dict) -> str:
        return ", ".join(x for x in (l.get("area"), l.get("emirate")) if x)

    def _money(v) -> str:
        try:
            return f"AED {int(float(v)):,}"
        except (TypeError, ValueError):
            return "AED ?"

    def _fmt_new(c: dict) -> str:
        l = listings[c["listing_id"]]
        bits = [f"• {_header(l)}"]
        if l.get("price_aed") is not None:
            bits.append(_money(l["price_aed"]))
        loc = _loc(l)
        if loc:
            bits.append(f"({loc})")
        line = " ".join(bits)
        if l.get("url"):
            line += f"\n  {l['url']}"
        return line

    def _fmt_update(c: dict) -> str:
        l = listings[c["listing_id"]]
        cf = c.get("changed_fields") or {}
        line = f"• {_header(l)}"
        if "price_aed" in cf:
            old = cf["price_aed"].get("old")
            new = cf["price_aed"].get("new")
            try:
                pct = (float(new) - float(old)) / float(old) * 100
                line += f": {_money(old)} → {_money(new)} ({pct:+.1f}%)"
            except (TypeError, ValueError, ZeroDivisionError):
                line += f": {_money(old)} → {_money(new)}"
        elif "mileage_km" in cf:
            old = cf["mileage_km"].get("old") or 0
            new = cf["mileage_km"].get("new") or 0
            line += f": {int(old):,} km → {int(new):,} km"
        elif cf:
            line += f": {', '.join(sorted(cf.keys()))} changed"
        if l.get("url"):
            line += f"\n  {l['url']}"
        return line

    def _fmt_delete(c: dict) -> str:
        l = listings[c["listing_id"]]
        bits = [f"• {_header(l)}"]
        if l.get("price_aed") is not None:
            bits.append(f"(was {_money(l['price_aed'])})")
        line = " ".join(bits)
        if l.get("url"):
            line += f"\n  {l['url']}"
        return line

    news    = [c for c in changes if c["change_type"] == "created"]
    updates = [c for c in changes if c["change_type"] == "updated"]
    deletes = [c for c in changes if c["change_type"] == "deleted"]

    sections: list[str] = []
    if news:
        lines = [_fmt_new(c) for c in news[:limit_per_section]]
        if len(news) > limit_per_section:
            lines.append(f"  …and {len(news) - limit_per_section} more")
        sections.append(f"🆕 {len(news)} new:\n" + "\n".join(lines))
    if updates:
        lines = [_fmt_update(c) for c in updates[:limit_per_section]]
        if len(updates) > limit_per_section:
            lines.append(f"  …and {len(updates) - limit_per_section} more")
        sections.append(f"💰 {len(updates)} updated:\n" + "\n".join(lines))
    if deletes:
        lines = [_fmt_delete(c) for c in deletes[:limit_per_section]]
        if len(deletes) > limit_per_section:
            lines.append(f"  …and {len(deletes) - limit_per_section} more")
        sections.append(f"❌ {len(deletes)} delisted:\n" + "\n".join(lines))

    return "\n\n".join(sections)


def log_run(client: Client, source: str, status: str, counts: dict,
            duration: float, error: Optional[str] = None):
    try:
        _safe_exec(
            client.table("scrape_runs").insert({
                "source": source,
                "status": status,
                "listings_found": counts.get("found", 0),
                "listings_new": counts.get("new", 0),
                "listings_updated": counts.get("updated", 0),
                "listings_deleted": counts.get("deleted", 0),
                "error_message": error[:2000] if error else None,
                "duration_seconds": round(duration, 2),
            })
        )
    except Exception as e:
        logger.error(f"Failed to write scrape_run log: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Internals
# ─────────────────────────────────────────────────────────────────────────────

# Columns written by the detail-page scrape. Kept here (not in the scraper)
# so update_detail_fields can filter incoming dicts safely.
DETAIL_FIELDS: frozenset = frozenset({
    "trim", "horsepower_text", "engine_capacity_cc_text",
    "seating_capacity_text", "interior_color", "target_market",
    "warranty", "posted_at", "features",
    "dealer_name", "dealer_logo_url",
    "image_urls", "description",
    "body_type", "fuel_type", "cylinders", "doors", "area",
})


def update_detail_fields(
    client: Client, source: str, external_id: str, fields: dict,
) -> bool:
    """
    Write just the detail-enrichment fields for ONE listing, identified by
    (source, external_id). Called incrementally during detail scraping so
    data isn't lost if the run crashes later. Non-fatal on error: logs and
    returns False so the caller can keep enriching the remaining listings.
    """
    if not fields:
        return False

    patch = {
        k: v for k, v in fields.items()
        if k in DETAIL_FIELDS and v is not None and v != {} and v != []
    }
    if not patch:
        return False

    now_iso = datetime.now(timezone.utc).isoformat()
    patch["detail_scraped_at"] = now_iso
    patch["last_seen_at"] = now_iso

    try:
        _safe_exec(
            client.table("car_listings")
            .update(patch)
            .eq("source", source)
            .eq("external_id", external_id)
        )
        return True
    except Exception as e:
        logger.warning(
            f"[{source}] incremental detail commit failed for {external_id}: {e}"
        )
        return False


def _log_change(client: Client, listing_id: str, change_type: str,
                old_hash: Optional[str], new_hash: Optional[str], changed_fields: dict):
    try:
        _safe_exec(
            client.table("car_listing_changes").insert({
                "listing_id": listing_id,
                "change_type": change_type,
                "changed_fields": changed_fields or {},
                "old_hash": old_hash,
                "new_hash": new_hash,
            })
        )
    except Exception as e:
        logger.warning(f"Failed to log change for {listing_id}: {e}")


def _diff(old: dict, new: dict) -> dict:
    WATCH = [
        "price_aed", "price_aed_max", "mileage_km", "description",
        "seller_phone", "color", "condition", "image_urls", "area", "emirate"
    ]
    return {
        f: {"old": old.get(f), "new": new.get(f)}
        for f in WATCH
        if old.get(f) != new.get(f)
    }


def _chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
