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

from supabase import create_client, Client

logger = logging.getLogger(__name__)

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

def compute_hash(listing: dict) -> str:
    """MD5 of fields that signal a meaningful change."""
    fields = [
        listing.get("price_aed"),
        listing.get("price_aed_max"),
        listing.get("mileage_km"),
        listing.get("description"),
        listing.get("seller_phone"),
        # Sort image list so reordering doesn't trigger a spurious update
        sorted(listing.get("image_urls") or []),
        listing.get("is_active"),
    ]
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
    source = listings[0]["source"]
    now_iso = datetime.now(timezone.utc).isoformat()

    # Build lookup: external_id → {id, content_hash}.
    # PostgREST passes `in.(...)` as a query string; at ~35 chars per 32-char
    # hex ID the URL caps out well before 1000 IDs. Chunk to 100.
    ext_ids = [l["external_id"] for l in listings]
    existing: dict = {}
    for chunk in _chunks(ext_ids, 100):
        rows = (
            client.table("car_listings")
            .select("id, external_id, content_hash")
            .eq("source", source)
            .in_("external_id", chunk)
            .execute()
        )
        for r in (rows.data or []):
            existing[r["external_id"]] = r

    to_insert = []
    to_touch_ids = []  # only last_seen_at update needed

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
            old_id = existing[ext_id]["id"]
            listing["last_changed_at"] = now_iso
            listing["is_active"] = True
            # Fetch old for diff (targeted columns only, not *)
            old = (
                client.table("car_listings")
                .select("id,price_aed,mileage_km,description,seller_phone,color,condition,image_urls,area,emirate")
                .eq("id", old_id)
                .single()
                .execute()
            ).data
            client.table("car_listings").update(listing).eq("id", old_id).execute()
            changed_fields = _diff(old or {}, listing)
            _log_change(client, old_id, "updated", existing[ext_id]["content_hash"], new_hash, changed_fields)
            counts["updated"] += 1

        else:
            to_touch_ids.append(existing[ext_id]["id"])
            counts["skipped"] += 1

    # Bulk insert all new listings
    if to_insert:
        result = client.table("car_listings").insert(to_insert).execute()
        for row in (result.data or []):
            _log_change(client, row["id"], "created", None, row.get("content_hash"), {})

    # Bulk touch unchanged listings (single UPDATE per batch via 'in').
    # UUIDs are ~39 chars once URL-encoded — cap at 150 per chunk to stay
    # under PostgREST's URL length limit.
    if to_touch_ids:
        for chunk in _chunks(to_touch_ids, 150):
            client.table("car_listings").update({"last_seen_at": now_iso}).in_("id", chunk).execute()

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
        batch = (
            client.table("car_listings")
            .select("id, external_id")
            .eq("source", source)
            .eq("is_active", True)
            .range(offset, offset + page_size - 1)
            .execute()
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
        client.table("car_listings").update({
            "is_active": False,
            "deleted_at": now,
            "last_seen_at": now,
        }).in_("id", ids).execute()
        for r in chunk:
            _log_change(client, r["id"], "deleted", None, None, {})
        deleted += len(chunk)

    return deleted


# ─────────────────────────────────────────────────────────────────────────────
# Run logging
# ─────────────────────────────────────────────────────────────────────────────

def log_run(client: Client, source: str, status: str, counts: dict,
            duration: float, error: Optional[str] = None):
    try:
        client.table("scrape_runs").insert({
            "source": source,
            "status": status,
            "listings_found": counts.get("found", 0),
            "listings_new": counts.get("new", 0),
            "listings_updated": counts.get("updated", 0),
            "listings_deleted": counts.get("deleted", 0),
            "error_message": error[:2000] if error else None,
            "duration_seconds": round(duration, 2),
        }).execute()
    except Exception as e:
        logger.error(f"Failed to write scrape_run log: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Internals
# ─────────────────────────────────────────────────────────────────────────────

def _log_change(client: Client, listing_id: str, change_type: str,
                old_hash: Optional[str], new_hash: Optional[str], changed_fields: dict):
    try:
        client.table("car_listing_changes").insert({
            "listing_id": listing_id,
            "change_type": change_type,
            "changed_fields": changed_fields or {},
            "old_hash": old_hash,
            "new_hash": new_hash,
        }).execute()
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
