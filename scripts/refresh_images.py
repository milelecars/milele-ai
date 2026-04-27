"""
scripts/refresh_images.py — re-extract image_urls from detail pages only.

Lighter-weight sibling of backfill_details.py. Used when the detail image
extractor has been fixed and existing rows carry contaminated image_urls
that need scrubbing (e.g. photos leaked in from the "Similar Cars"
carousel under a different extractor rule).

Skips accordion expansion + "See more" clicks — those are only needed for
features, which this script does not touch. Saves ~1 s per listing vs a
full detail re-scrape.

Only `image_urls` is written. `detail_scraped_at` is NOT touched, so you
keep your record of when each row was last fully enriched.

Usage:
    python -m scripts.refresh_images            # default 500 rows
    python -m scripts.refresh_images 2000       # up to 2000 this run

Env:
    SUPABASE_URL, SUPABASE_SERVICE_KEY   required
    DUBIZZLE_HEADLESS                    optional (default "0")
    DUBIZZLE_DETAIL_ROTATE_EVERY         optional (default 25)
    DUBIZZLE_PROXY / SCRAPER_PROXY       optional residential proxy

Stops early on 10 consecutive failures (likely Imperva block).
"""

import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from scrapers.dubizzle import DubizzleScraper, _DETAIL_JS, _parse_proxy
from utils.db import _safe_exec, get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("refresh_images")

CONSECUTIVE_FAIL_LIMIT = 10
# A new listing whose stored image_urls already has at least this many entries
# is considered "rich" and skipped by the inline post-scrape refresh — saves
# the Playwright cost on records the detail-enrichment phase already nailed.
RICH_IMAGE_THRESHOLD = 3


def _fetch_images(scraper: DubizzleScraper, page, url: str) -> list:
    """Goto the detail URL, run the extractor, return normalised image URLs.
    Skips accordion / See-more expansion — images don't depend on those."""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    except Exception as e:
        logger.warning(f"  goto failed: {e}")
        return []

    scraper._human_wait(page, 4_000, 8_000)

    if scraper._is_blocked(page):
        logger.warning("  page blocked by Imperva")
        return []

    try:
        raw = page.evaluate(_DETAIL_JS)
    except Exception as e:
        logger.warning(f"  JS eval failed: {e}")
        return []

    if not isinstance(raw, dict):
        return []

    return DubizzleScraper._normalise_detail_images(raw.get("images") or [])


def _run_refresh(scraper: DubizzleScraper, rows: list[dict], db_client) -> dict:
    """
    Open a fresh Playwright/stealth session and refresh image_urls for each
    row in `rows`. Each row must have id, external_id, url, and (optionally)
    image_urls (used for length-preservation: never overwrite a richer DB
    list with a thinner re-fetch).

    Returns {"ok": int, "fail": int, "attempted": int}.
    """
    if not rows:
        return {"ok": 0, "fail": 0, "attempted": 0}

    headless = os.environ.get("DUBIZZLE_HEADLESS", "0") == "1"
    proxy_url = (
        os.environ.get("DUBIZZLE_PROXY")
        or os.environ.get("SCRAPER_PROXY")
        or ""
    )
    proxy_dict = _parse_proxy(proxy_url)
    try:
        rotate_every = int(os.environ.get("DUBIZZLE_DETAIL_ROTATE_EVERY", "25"))
    except ValueError:
        rotate_every = 25

    ok = 0
    fail = 0
    consecutive_empty = 0
    total = len(rows)

    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=headless)
        context = scraper._new_context(browser, proxy_dict)
        page = context.new_page()

        # Warm-up pass on the home page to establish cookies.
        try:
            page.goto(
                "https://uae.dubizzle.com/",
                wait_until="domcontentloaded",
                timeout=45_000,
            )
            scraper._human_wait(page, 5_000, 10_000)
            if scraper._is_blocked(page):
                logger.warning(
                    "Home page served Imperva interstitial — expect many ✗ "
                    "results; consider waiting 30 min and retrying."
                )
        except Exception as e:
            logger.warning(f"warmup error: {e}")

        try:
            for i, row in enumerate(rows, 1):
                # Periodic context rotation — same pattern as backfill_details.
                if i > 1 and (i - 1) % rotate_every == 0:
                    page = scraper._rotate_detail_context(
                        page, browser, proxy_dict,
                        reason=f"periodic at {i}/{total}",
                    )
                    consecutive_empty = 0

                url = row.get("url")
                ext_id = row["external_id"]
                tag = f"{ext_id[:8]}"
                if not url:
                    logger.info(f"[{i}/{total}] {tag}: no URL, skip")
                    fail += 1
                    continue

                images = _fetch_images(scraper, page, url)

                if not images:
                    fail += 1
                    consecutive_empty += 1
                    logger.warning(
                        f"[{i}/{total}] ✗ {tag} (no images extracted)"
                    )
                    if consecutive_empty >= 5:
                        page = scraper._rotate_detail_context(
                            page, browser, proxy_dict,
                            reason=f"{consecutive_empty} consecutive empties",
                            cooldown=(30, 60),
                        )
                        consecutive_empty = 0
                else:
                    consecutive_empty = 0
                    # Length-preservation: never let a thin refresh overwrite
                    # a richer set already in DB.
                    existing = row.get("image_urls") or []
                    if len(images) < len(existing):
                        logger.info(
                            f"[{i}/{total}] ↺ {tag} kept DB's "
                            f"{len(existing)} images (refresh returned only "
                            f"{len(images)})"
                        )
                        ok += 1
                    else:
                        try:
                            _safe_exec(
                                db_client.table("car_listings")
                                .update({
                                    "image_urls": images,
                                    "last_changed_at": datetime.now(timezone.utc).isoformat(),
                                })
                                .eq("id", row["id"])
                            )
                            ok += 1
                            logger.info(
                                f"[{i}/{total}] ✓ {tag} → {len(images)} images | {url}"
                            )
                        except Exception as e:
                            fail += 1
                            logger.warning(
                                f"[{i}/{total}] ⚠ {tag} persist failed: {e}"
                            )

                if consecutive_empty >= CONSECUTIVE_FAIL_LIMIT:
                    logger.error(
                        f"{consecutive_empty} consecutive failures — aborting."
                    )
                    break

                scraper._human_wait(page, 2_000, 5_000)
        finally:
            try:
                browser.close()
            except Exception:
                pass

    return {"ok": ok, "fail": fail, "attempted": ok + fail}


def refresh_images_for_external_ids(
    db_client, source: str, external_ids,
) -> dict:
    """
    Visit each candidate listing's detail page in a fresh Playwright session
    and refresh image_urls. Filters to rows whose stored image_urls is below
    RICH_IMAGE_THRESHOLD — already-rich listings are skipped to bound cost.

    Designed to be called from main.py post-upsert for newly-inserted
    listings whose inline detail-enrichment didn't capture enough images
    (cap'd by DUBIZZLE_DETAIL_BATCH_SIZE, or transient Imperva block, etc).
    Failures are non-fatal: a row left thin gets picked up by the standalone
    `python -m scripts.refresh_images` backfill cron.
    """
    ext_ids = list(external_ids) if external_ids else []
    if not ext_ids:
        return {"ok": 0, "fail": 0, "attempted": 0}

    rows: list = []
    for i in range(0, len(ext_ids), 100):
        chunk = ext_ids[i : i + 100]
        try:
            data = _safe_exec(
                db_client.table("car_listings")
                .select("id, external_id, url, image_urls")
                .eq("source", source)
                .eq("is_active", True)
                .in_("external_id", chunk)
            ).data or []
            rows.extend(data)
        except Exception as e:
            logger.warning(f"[{source}] image-refresh row fetch failed: {e}")

    # Skip rows that already have a rich image set — the detail-enrichment
    # phase nailed those, no need to re-visit.
    thin_rows = [
        r for r in rows
        if len(r.get("image_urls") or []) < RICH_IMAGE_THRESHOLD
    ]
    if not thin_rows:
        return {"ok": 0, "fail": 0, "attempted": 0}

    logger.info(
        f"[{source}] refresh_images: {len(thin_rows)} new listings need "
        f"image refresh (of {len(rows)} new total; rest already rich)"
    )
    scraper = DubizzleScraper()
    return _run_refresh(scraper, thin_rows, db_client)


def _fetch_thin_rows(client, limit: int) -> list[dict]:
    """Pull every active dubizzle row, filter Python-side to those with fewer
    than RICH_IMAGE_THRESHOLD images, return up to `limit` of them. PostgREST
    can't filter by array_length directly, so we page through and filter."""
    rows: list = []
    PAGE = 1000
    offset = 0
    while len(rows) < limit:
        batch = _safe_exec(
            client.table("car_listings")
            .select("id, external_id, url, image_urls")
            .eq("source", "dubizzle")
            .eq("is_active", True)
            .order("first_seen_at")
            .range(offset, offset + PAGE - 1)
        ).data or []
        if not batch:
            break
        thin = [
            r for r in batch
            if len(r.get("image_urls") or []) < RICH_IMAGE_THRESHOLD
        ]
        rows.extend(thin)
        if len(batch) < PAGE:
            break
        offset += len(batch)
    return rows[:limit]


def _fetch_cutoff_rows(client, limit: int, cutoff: str) -> list[dict]:
    """Legacy CLI mode: rows whose last_changed_at is older than `cutoff`."""
    rows: list = []
    PAGE = 1000
    offset = 0
    while len(rows) < limit:
        batch_size = min(PAGE, limit - len(rows))
        batch = _safe_exec(
            client.table("car_listings")
            .select("id, external_id, url, last_changed_at, image_urls")
            .eq("source", "dubizzle")
            .eq("is_active", True)
            .lt("last_changed_at", cutoff)
            .order("last_changed_at")
            .range(offset, offset + batch_size - 1)
        ).data or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < batch_size:
            break
        offset += len(batch)
    return rows


def main():
    """
    Usage:
        python -m scripts.refresh_images                # 500 rows by cutoff
        python -m scripts.refresh_images 1000           # 1000 rows by cutoff
        python -m scripts.refresh_images --thin         # all thin rows
        python -m scripts.refresh_images --thin 200     # up to 200 thin rows
    """
    args = sys.argv[1:]
    thin_only = "--thin" in args
    args = [a for a in args if not a.startswith("--")]
    try:
        limit = int(args[0]) if args else (10_000 if thin_only else 500)
    except ValueError:
        limit = 10_000 if thin_only else 500

    sb_url = os.environ.get("SUPABASE_URL")
    sb_key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not (sb_url and sb_key):
        logger.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
        sys.exit(1)

    client = get_client(sb_url, sb_key)

    if thin_only:
        rows = _fetch_thin_rows(client, limit)
        logger.info(
            f"--thin mode: {len(rows)} active dubizzle rows have fewer than "
            f"{RICH_IMAGE_THRESHOLD} images"
        )
    else:
        # Legacy time-cutoff mode (rows untouched since the last extractor fix).
        # Cutoff: 2026-04-24 20:00 Asia/Dubai (UTC+4) == 2026-04-24 16:00 UTC.
        cutoff = os.environ.get(
            "DUBIZZLE_REFRESH_CUTOFF", "2026-04-24T16:00:00+00:00"
        )
        rows = _fetch_cutoff_rows(client, limit, cutoff)

    if not rows:
        logger.info("No active rows to refresh.")
        return

    logger.info(f"Refreshing image_urls for {len(rows)} listings")
    scraper = DubizzleScraper()
    summary = _run_refresh(scraper, rows, client)
    logger.info(
        f"Done. ok={summary['ok']} fail={summary['fail']} "
        f"attempted={summary['attempted']}"
    )


if __name__ == "__main__":
    main()
