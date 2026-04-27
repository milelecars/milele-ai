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
    # External IDs of brand-new listings inserted this batch — main.py uses
    # these to trigger inline color extraction via Groq vision.
    new_external_ids: set[str] = set()
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
            .select("id, external_id, content_hash, is_active")
            .eq("source", source)
            .in_("external_id", chunk)
        )
        for r in (rows.data or []):
            existing[r["external_id"]] = r

    to_insert = []
    to_touch_ids = []  # only last_seen_at update needed
    # Each tuple: (old_id, new_listing, old_hash, hash_changed)
    # hash_changed=True  → real list-level diff detected at routing time;
    #                       always counted/logged.
    # hash_changed=False → routed in only because detail_scraped_at is set;
    #                       counted/logged only if actual fields differ.
    changed: list[tuple[str, dict, str, bool]] = []
    relisted_ids: list[str] = []  # rows being re-activated after a prior soft-delete

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
            new_external_ids.add(ext_id)
            continue

        was_inactive = existing[ext_id].get("is_active") is False
        if was_inactive:
            # Listing reappeared after a previous soft-delete. Re-activate it
            # regardless of which routing branch it falls into below, and log
            # a "relisted" change row for the digest.
            relisted_ids.append(existing[ext_id]["id"])

        if existing[ext_id]["content_hash"] != new_hash:
            listing["last_changed_at"] = now_iso
            listing["is_active"] = True
            changed.append((existing[ext_id]["id"], listing, existing[ext_id]["content_hash"], True))
            counts["updated"] += 1
            updated_external_ids.add(ext_id)

        elif listing.get("detail_scraped_at"):
            # List-level hash matches, but detail was freshly scraped this
            # run. Route through the per-row update path so detail fields
            # persist. Counter + change log are deferred to the loop below
            # so we only count/log when real fields differ — empty-diff
            # detail re-entries no longer pollute the digest.
            listing["is_active"] = True
            changed.append((existing[ext_id]["id"], listing, existing[ext_id]["content_hash"], False))

        elif was_inactive:
            # Hash matches and no detail re-scrape, but the row is currently
            # inactive — promote to a per-row update so is_active/deleted_at
            # actually get cleared (the bulk-touch path only writes
            # last_seen_at).
            listing["is_active"] = True
            changed.append((existing[ext_id]["id"], listing, existing[ext_id]["content_hash"], False))

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
        for old_id, listing, old_hash, hash_changed in changed:
            # Strip None values from the UPDATE payload. List-scrape doesn't
            # extract every field every run (e.g. `color` is missing from
            # most search-page JSON-LD entries), and sending those Nones
            # would overwrite valid DB columns populated by a previous run.
            # INSERT path (above) keeps the dict intact — Postgres uses the
            # column default for missing keys, so this is safe.
            update_payload = {k: v for k, v in listing.items() if v is not None}

            # Never let a shorter image_urls list overwrite a richer one.
            # The search-page JSON-LD typically only carries the cover photo
            # (1 image), and the detail page's extractor can drop images on
            # lazy-load races. We always want the largest image set we've
            # ever seen — drop the field from both the write and the diff
            # when this run produced fewer images than the DB already has.
            new_imgs = update_payload.get("image_urls") or []
            old_imgs = (old_rows.get(old_id) or {}).get("image_urls") or []
            if len(new_imgs) < len(old_imgs):
                update_payload.pop("image_urls", None)

            _safe_exec(
                client.table("car_listings").update(update_payload).eq("id", old_id)
            )
            # Compute the diff against update_payload (the values actually
            # written), not the raw listing — so fields we deliberately
            # preserved (image_urls above, Nones in general) don't show up
            # as phantom changes in the digest.
            changed_fields = _diff(old_rows.get(old_id, {}), update_payload)
            if hash_changed:
                # Real list-level diff — already counted at routing time.
                _log_change(
                    client, old_id, "updated", old_hash,
                    listing["content_hash"], changed_fields,
                )
            elif changed_fields:
                # Detail-only re-entry that actually moved fields (e.g.
                # detail page provided fresher image_urls). Count + log now.
                _log_change(
                    client, old_id, "updated", old_hash,
                    listing["content_hash"], changed_fields,
                )
                counts["updated"] += 1
                updated_external_ids.add(listing["external_id"])
            # else: silent no-op write — no log row, no counter bump.

    # Re-activate listings that reappeared after a previous soft-delete.
    # Done as a dedicated UPDATE because the per-row update payload above
    # strips Nones, so it can't clear deleted_at back to NULL on its own.
    # Each re-activation gets a "relisted" change row so the digest can
    # surface them separately from new arrivals and price updates.
    if relisted_ids:
        for chunk in _chunks(relisted_ids, 150):
            _safe_exec(
                client.table("car_listings")
                .update({
                    "is_active": True,
                    "deleted_at": None,
                    "missed_run_count": 0,
                })
                .in_("id", chunk)
            )
        for rid in relisted_ids:
            _log_change(client, rid, "relisted", None, None, {})
        counts["relisted"] = len(relisted_ids)
        logger.info(
            f"[{source}] re-activated {len(relisted_ids)} previously-deleted listings"
        )

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
    counts["new_external_ids"] = new_external_ids
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

    Used by scrapers that do not implement URL-level verification. Scrapers
    that can verify dead URLs should use mark_missing + soft_delete_verified.
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
# Verified-delete pipeline: mark_missing → verify_dead_urls → soft_delete_verified
#
# Two-step delete with a one-run grace period and URL-level confirmation:
#   1. mark_missing: bumps missed_run_count for listings not seen this run,
#      resets to 0 for listings that were seen. Returns the rows whose count
#      just reached >= MISS_GRACE_THRESHOLD (default 2) — the verification
#      candidates.
#   2. The scraper visits each candidate URL and reports back which ones are
#      actually dead (e.g. served the "page not found" page).
#   3. soft_delete_verified: deletes only those confirmed-dead listings.
#
# Net effect: a listing must be missing for 2 consecutive runs AND its URL
# must serve a confirmed-dead marker before it gets soft-deleted. Routine
# pagination flicker no longer causes false delistings.
# ─────────────────────────────────────────────────────────────────────────────

# Number of consecutive missed runs before a listing becomes a verification
# candidate. 2 = first miss is grace, second miss triggers URL check.
MISS_GRACE_THRESHOLD = 2

# Number of consecutive missed runs after which a listing is force-deleted
# without URL verification. Bounds the cost of listings whose verification
# never resolves to a clean "dead"/"alive" answer (e.g. Imperva keeps
# blocking the verify, page redirects in a way we can't classify, etc.).
# At ~1 run/day, 30 ≈ a month of consecutive misses — strong enough signal
# that the listing is gone even without an explicit not-found marker.
MISS_HARD_DELETE_THRESHOLD = 30


def mark_missing(
    client: Client, source: str, live_external_ids: set[str],
) -> dict:
    """
    Reconcile this run's results with DB state:
      - Reset missed_run_count to 0 for any active listing seen this run.
      - Increment missed_run_count for any active listing NOT seen this run.
      - Return rows whose new count is >= MISS_GRACE_THRESHOLD as
        verification candidates.

    Returns a dict:
      {
        "total_active":  int — active listings in DB before this run,
        "missing":       int — total listings missing this run,
        "first_miss":    int — first-time misses (grace, no action),
        "candidates":    list[{id, external_id, url, missed_run_count}],
      }
    """
    all_stored: list[dict] = []
    page_size = 1000
    offset = 0
    while True:
        batch = _safe_exec(
            client.table("car_listings")
            .select("id, external_id, url, missed_run_count")
            .eq("source", source)
            .eq("is_active", True)
            .range(offset, offset + page_size - 1)
        )
        rows = batch.data or []
        all_stored.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size

    seen = [r for r in all_stored if r["external_id"] in live_external_ids]
    missing = [r for r in all_stored if r["external_id"] not in live_external_ids]

    # Reset counter for seen rows that were previously missing.
    to_reset = [r["id"] for r in seen if (r.get("missed_run_count") or 0) > 0]
    for chunk in _chunks(to_reset, 150):
        _safe_exec(
            client.table("car_listings")
            .update({"missed_run_count": 0})
            .in_("id", chunk)
        )

    # Increment counter for missing rows. Group by current count so we can
    # batch-update each group with a single new value.
    by_old_count: dict = {}
    for r in missing:
        cur = r.get("missed_run_count") or 0
        by_old_count.setdefault(cur, []).append(r["id"])
    for old_count, ids in by_old_count.items():
        for chunk in _chunks(ids, 150):
            _safe_exec(
                client.table("car_listings")
                .update({"missed_run_count": old_count + 1})
                .in_("id", chunk)
            )

    candidates = []
    first_miss = 0
    for r in missing:
        new_count = (r.get("missed_run_count") or 0) + 1
        if new_count >= MISS_GRACE_THRESHOLD:
            candidates.append({
                "id": r["id"],
                "external_id": r["external_id"],
                "url": r.get("url"),
                "missed_run_count": new_count,
            })
        else:
            first_miss += 1

    logger.info(
        f"[{source}] mark_missing: total_active={len(all_stored)}, "
        f"missing={len(missing)} "
        f"(first_miss={first_miss}, verify_candidates={len(candidates)})"
    )
    return {
        "total_active": len(all_stored),
        "missing": len(missing),
        "first_miss": first_miss,
        "candidates": candidates,
    }


def soft_delete_verified(
    client: Client, source: str, dead_external_ids: set[str],
    total_active: Optional[int] = None,
) -> int:
    """
    Soft-delete listings whose URL was confirmed dead by the scraper. Applies
    the SOFT_DELETE_SAFETY_THRESHOLD against `total_active` if provided
    (caller should pass the value returned by mark_missing for accuracy).
    Returns count of deleted listings.
    """
    if not dead_external_ids:
        return 0

    now = datetime.now(timezone.utc).isoformat()

    to_delete: list[dict] = []
    for chunk in _chunks(list(dead_external_ids), 100):
        rows = _safe_exec(
            client.table("car_listings")
            .select("id, external_id")
            .eq("source", source)
            .eq("is_active", True)
            .in_("external_id", chunk)
        ).data or []
        to_delete.extend(rows)

    if not to_delete:
        return 0

    if total_active and total_active > 50:
        ratio = len(to_delete) / total_active
        if ratio > SOFT_DELETE_SAFETY_THRESHOLD:
            logger.error(
                f"[{source}] SAFETY ABORT: {len(to_delete)}/{total_active} "
                f"({ratio:.0%}) verified-dead would be soft-deleted — exceeds "
                f"{SOFT_DELETE_SAFETY_THRESHOLD:.0%} threshold. No deletions."
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

    logger.info(f"[{source}] soft_delete_verified: deleted {deleted} listings")
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

    def _short(v, max_len: int = 80) -> str:
        """Render any field value compactly for the digest."""
        if v is None:
            return "∅"
        if isinstance(v, (list, tuple, set)):
            n = len(v)
            return f"[{n} item{'s' if n != 1 else ''}]"
        if isinstance(v, dict):
            return f"{{{len(v)} keys}}"
        s = str(v).replace("\n", " ").strip()
        return s if len(s) <= max_len else s[: max_len - 1] + "…"

    def _fmt_field_diff(field: str, old, new) -> str:
        if field == "price_aed":
            try:
                pct = (float(new) - float(old)) / float(old) * 100
                return f"price: {_money(old)} → {_money(new)} ({pct:+.1f}%)"
            except (TypeError, ValueError, ZeroDivisionError):
                return f"price: {_money(old)} → {_money(new)}"
        if field == "mileage_km":
            try:
                return f"mileage: {int(float(old)):,} km → {int(float(new)):,} km"
            except (TypeError, ValueError):
                return f"mileage: {old} → {new}"
        return f"{field}: {_short(old)} → {_short(new)}"

    def _fmt_new(c: dict) -> str:
        l = listings[c["listing_id"]]
        bits = [f"• {_header(l)}"]
        if l.get("price_aed") is not None:
            bits.append(_money(l["price_aed"]))
        loc = _loc(l)
        if loc:
            bits.append(f"({loc})")
        line = " ".join(bits) + f"\n  id: {l['id']}"
        if l.get("url"):
            line += f"\n  {l['url']}"
        return line

    def _fmt_update(c: dict) -> str:
        l = listings[c["listing_id"]]
        cf = c.get("changed_fields") or {}
        line = f"• {_header(l)}\n  id: {l['id']}"
        if cf:
            # Sort price/mileage first since those matter most to sales.
            order = sorted(
                cf.keys(),
                key=lambda k: (k != "price_aed", k != "mileage_km", k),
            )
            for f in order:
                entry = cf[f] or {}
                line += "\n  " + _fmt_field_diff(f, entry.get("old"), entry.get("new"))
        if l.get("url"):
            line += f"\n  {l['url']}"
        return line

    def _fmt_delete(c: dict) -> str:
        l = listings[c["listing_id"]]
        bits = [f"• {_header(l)}"]
        if l.get("price_aed") is not None:
            bits.append(f"(was {_money(l['price_aed'])})")
        line = " ".join(bits) + f"\n  id: {l['id']}"
        if l.get("url"):
            line += f"\n  {l['url']}"
        return line

    news     = [c for c in changes if c["change_type"] == "created"]
    updates  = [c for c in changes if c["change_type"] == "updated"]
    deletes  = [c for c in changes if c["change_type"] == "deleted"]
    relisted = [c for c in changes if c["change_type"] == "relisted"]

    sections: list[str] = []
    if news:
        lines = [_fmt_new(c) for c in news[:limit_per_section]]
        if len(news) > limit_per_section:
            lines.append(f"  …and {len(news) - limit_per_section} more")
        sections.append(f"🆕 {len(news)} new:\n" + "\n".join(lines))
    if relisted:
        # Relisted = listing reappeared after a prior soft-delete (often a
        # bookkeeping artefact: scrape coverage flicker first soft-deleted
        # the listing, the next run found it alive again).
        lines = [_fmt_new(c) for c in relisted[:limit_per_section]]
        if len(relisted) > limit_per_section:
            lines.append(f"  …and {len(relisted) - limit_per_section} more")
        sections.append(f"♻️ {len(relisted)} relisted:\n" + "\n".join(lines))
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
    # Source-specific catch-all + dealer phone — both filled by the DubiCars
    # detail scraper and would otherwise only persist via the final upsert.
    "attributes", "seller_phone",
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
    # Watched fields for change-detection logging. `color` is intentionally
    # excluded: it's owned by the Groq vision extractor (scripts/extract_colors.py),
    # not the scraper, so a list-scrape diff against it is meaningless.
    WATCH = [
        "price_aed", "price_aed_max", "mileage_km", "description",
        "seller_phone", "condition", "image_urls", "area", "emirate"
    ]
    # Skip fields where the new value is None — that means the scraper didn't
    # extract this field this run (e.g. `color` missing from list-page JSON-LD).
    # The update payload also strips Nones, so the DB value is preserved; we
    # don't want to log a phantom "white → ∅" change for it either.
    return {
        f: {"old": old.get(f), "new": new.get(f)}
        for f in WATCH
        if old.get(f) != new.get(f) and new.get(f) is not None
    }


def _chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
