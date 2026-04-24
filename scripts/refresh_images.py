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


def main():
    try:
        limit = int(sys.argv[1]) if len(sys.argv) > 1 else 500
    except ValueError:
        limit = 500

    sb_url = os.environ.get("SUPABASE_URL")
    sb_key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not (sb_url and sb_key):
        logger.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
        sys.exit(1)

    client = get_client(sb_url, sb_key)

    rows = _safe_exec(
        client.table("car_listings")
        .select("id, external_id, url")
        .eq("source", "dubizzle")
        .eq("is_active", True)
        .order("first_seen_at")
        .limit(limit)
    ).data or []

    if not rows:
        logger.info("No active rows to refresh.")
        return

    logger.info(f"Refreshing image_urls for {len(rows)} listings")

    scraper = DubizzleScraper()
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
                        reason=f"periodic at {i}/{len(rows)}",
                    )
                    consecutive_empty = 0

                url = row.get("url")
                ext_id = row["external_id"]
                tag = f"{ext_id[:8]}"
                if not url:
                    logger.info(f"[{i}/{len(rows)}] {tag}: no URL, skip")
                    fail += 1
                    continue

                images = _fetch_images(scraper, page, url)

                if not images:
                    fail += 1
                    consecutive_empty += 1
                    logger.warning(
                        f"[{i}/{len(rows)}] ✗ {tag} (no images extracted)"
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
                    try:
                        _safe_exec(
                            client.table("car_listings")
                            .update({"image_urls": images})
                            .eq("id", row["id"])
                        )
                        ok += 1
                        logger.info(
                            f"[{i}/{len(rows)}] ✓ {tag} → {len(images)} images | {url}"
                        )
                    except Exception as e:
                        fail += 1
                        logger.warning(
                            f"[{i}/{len(rows)}] ⚠ {tag} persist failed: {e}"
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

    logger.info(
        f"Done. ok={ok} fail={fail} attempted={ok + fail}"
    )


if __name__ == "__main__":
    main()
