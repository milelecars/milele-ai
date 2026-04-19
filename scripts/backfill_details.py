"""
scripts/backfill_details.py — detail-enrich pending rows only.

Skips the list-scrape phase entirely. Pulls `detail_scraped_at IS NULL` rows
from Supabase, walks through their stored URLs one by one with Playwright,
and commits each listing's detail fields immediately. Saves the ~30-min list
scrape on every run when you're only trying to drain the backfill queue.

Usage:
    python -m scripts.backfill_details          # default limit 500
    python -m scripts.backfill_details 200      # only try 200 this run

Reuses DubizzleScraper's context + fetch logic (same rotation, image
blocking, human-timing) — only the list-scrape loop is skipped.
"""

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from scrapers.dubizzle import DubizzleScraper, _parse_proxy
from utils.db import _safe_exec, get_client, update_detail_fields

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("backfill")


def main():
    try:
        limit = int(sys.argv[1]) if len(sys.argv) > 1 else 500
    except ValueError:
        limit = 500

    sb_url = os.environ.get("SUPABASE_URL")
    sb_key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not sb_url or not sb_key:
        logger.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
        sys.exit(1)

    client = get_client(sb_url, sb_key)

    rows = _safe_exec(
        client.table("car_listings")
        .select("external_id, url")
        .eq("source", "dubizzle")
        .eq("is_active", True)
        .is_("detail_scraped_at", "null")
        .order("first_seen_at")
        .limit(limit)
    ).data or []

    if not rows:
        logger.info("Nothing pending. Backfill is complete.")
        return

    logger.info(f"Backfilling {len(rows)} detail pages")

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

    succeeded = 0
    failed = 0
    consecutive_empties = 0

    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=headless)
        context = scraper._new_context(browser, proxy_dict)
        page = context.new_page()

        # Cold-start warmup on the home page — establishes cookies and makes
        # the subsequent detail fetches look like organic navigation.
        try:
            page.goto(
                "https://uae.dubizzle.com/",
                wait_until="domcontentloaded",
                timeout=45_000,
            )
            scraper._human_wait(page, 5_000, 10_000)
            if scraper._is_blocked(page):
                logger.warning(
                    "Home page served Imperva interstitial even before fetches. "
                    "Expect a lot of ✗ results — consider waiting 30 min and retrying."
                )
        except Exception as e:
            logger.warning(f"warmup error: {e}")

        try:
            for i, row in enumerate(rows, 1):
                # Periodic rotation — spreads Imperva fingerprinting across
                # multiple browser sessions within the same run.
                if i > 1 and (i - 1) % rotate_every == 0:
                    page = scraper._rotate_detail_context(
                        page, browser, proxy_dict,
                        reason=f"periodic rotation at {i}/{len(rows)}",
                    )
                    consecutive_empties = 0

                ext_id = row["external_id"]
                url = row.get("url")
                if not url:
                    continue

                detail = scraper._fetch_detail(page, url)
                meaningful = {}
                if detail:
                    meaningful = {
                        k: v for k, v in detail.items()
                        if v is not None and v != {} and v != []
                    }

                if not meaningful:
                    failed += 1
                    consecutive_empties += 1
                    logger.warning(
                        f"✗ {i}/{len(rows)} {ext_id} (fetch empty)"
                    )
                    if consecutive_empties >= 5:
                        page = scraper._rotate_detail_context(
                            page, browser, proxy_dict,
                            reason=f"{consecutive_empties} consecutive empties",
                            cooldown=(30, 60),
                        )
                        consecutive_empties = 0
                else:
                    consecutive_empties = 0
                    ok = update_detail_fields(
                        client, "dubizzle", ext_id, meaningful
                    )
                    if ok:
                        succeeded += 1
                        logger.info(
                            f"✓ {i}/{len(rows)} {ext_id} "
                            f"(fields={len(meaningful)})"
                        )
                    else:
                        failed += 1
                        logger.warning(
                            f"⚠ {i}/{len(rows)} {ext_id} persist failed"
                        )

                scraper._human_wait(page, 2_000, 5_000)
        finally:
            try:
                browser.close()
            except Exception:
                pass

    logger.info(
        f"Done. succeeded={succeeded} failed={failed} attempted={len(rows)}"
    )


if __name__ == "__main__":
    main()
