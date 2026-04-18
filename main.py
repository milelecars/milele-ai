"""
main.py — Orchestrator

Usage:
    python main.py                         # all sources
    python main.py --source dubizzle       # single source
    python main.py --dry-run               # scrape only, no DB writes
    python main.py --stagger 120           # wait N seconds between sources (anti-block)

Env vars:
    SUPABASE_URL            required
    SUPABASE_SERVICE_KEY    required (service_role key)
    APIFY_TOKEN             optional — enables Apify-first mode for supported scrapers
    ALERT_WEBHOOK_URL       optional — POST failure summary to this URL (Slack/Teams/etc)
    TELEGRAM_BOT_TOKEN      optional — send daily summary to Telegram (pair with _CHAT_ID)
    TELEGRAM_CHAT_ID        optional — recipient chat for Telegram summary
    DUBIZZLE_DETAIL_BATCH_SIZE
                            optional — max detail-page fetches per run for
                            Dubizzle (default 100). Controls how fast the
                            initial ~900-listing backfill completes.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Ensure the project root is on sys.path regardless of how/where this script
# is invoked (python main.py, IDE run button, absolute path, etc.)
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Load .env file so SUPABASE_URL, APIFY_TOKEN etc. are available via os.environ
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / ".env")

import requests as _requests

from utils.db import (
    get_client, upsert_listings, soft_delete_missing, log_run, get_detail_plan,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")

SCRAPERS = {
    "dubizzle":   ("scrapers.dubizzle",    "DubizzleScraper"),
    "dubicars":   ("scrapers.dubicars",    "DubiCarsScraper"),
    "yallamotor": ("scrapers.yallamotor",  "YallaMotorScraper"),
    "carswitch":  ("scrapers.carswitch",   "CarSwitchScraper"),
    "sellanycar": ("scrapers.sellanycar",  "SellAnyCarScraper"),
}

# Maximum seconds a single source is allowed to run before we forcibly stop
# and log a partial result. Prevents a single hung scraper from blocking others.
SOURCE_TIMEOUT_SECONDS = 55 * 60  # 55 minutes


def load_scraper(name: str):
    import importlib
    module_path, class_name = SCRAPERS[name]
    module = importlib.import_module(module_path)
    return getattr(module, class_name)()


def run_source(name: str, client, dry_run: bool) -> dict:
    """Run one source. Returns result summary dict."""
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}▶ {name}")
    start = time.time()
    counts = {"found": 0, "new": 0, "updated": 0, "skipped": 0, "deleted": 0}
    status = "success"
    error_msg = None

    try:
        scraper = load_scraper(name)

        # Build a detail-scrape plan for scrapers that support per-listing
        # detail enrichment. Only runs on real (non-dry) runs so we have a DB.
        detail_plan = None
        if client and not dry_run and getattr(scraper, "SUPPORTS_DETAIL", False):
            try:
                batch_size = int(
                    os.environ.get(f"{name.upper()}_DETAIL_BATCH_SIZE", "100")
                )
            except ValueError:
                batch_size = 100
            try:
                detail_plan = get_detail_plan(client, name, batch_size=batch_size)
                detail_plan["batch_size"] = batch_size
                logger.info(
                    f"[{name}] detail plan — known={len(detail_plan['known_external_ids'])}, "
                    f"backfill_queued={len(detail_plan['backfill_external_ids'])}, "
                    f"batch={batch_size}"
                )
            except Exception as e:
                logger.warning(f"[{name}] detail plan failed ({e}); list-only run")
                detail_plan = None

        run_kwargs: dict = {}
        if detail_plan is not None:
            run_kwargs["detail_plan"] = detail_plan
        if client and not dry_run:
            # Give the scraper the DB client so it can incrementally persist
            # list data (after list scrape) and detail data (per listing).
            # Limits data loss if the run crashes mid-way.
            run_kwargs["db_client"] = client
        listings = scraper.run(**run_kwargs)
        counts["found"] = len(listings)

        if dry_run:
            logger.info(f"[DRY RUN] {name}: {counts['found']} found, not writing")
            for l in listings[:3]:
                logger.info(f"  → {l.get('year')} {l.get('make')} {l.get('model')} — AED {l.get('price_aed')}")
            return {"source": name, "status": "dry_run", "counts": counts}

        if counts["found"] == 0:
            logger.warning(f"{name}: 0 listings — check if scraper is broken")
            log_run(client, name, "partial", counts, time.time() - start, "0 listings scraped")
            return {"source": name, "status": "partial", "counts": counts}

        # If the scraper already did an intermediate upsert (incremental
        # persistence for crash safety), reuse those counts. Otherwise run
        # the upsert now. The final pass is still important when present —
        # incremental-enrichment DB updates change detail fields only, not
        # list-level fields that might have shifted during this run.
        intermediate = getattr(scraper, "_intermediate_upsert_counts", None)
        if intermediate is not None:
            counts.update(intermediate)
            logger.info(f"{name}: using intermediate upsert counts {intermediate}")
        else:
            upsert_counts = upsert_listings(client, listings)
            counts.update(upsert_counts)

        live_ids = {l["external_id"] for l in listings}
        counts["deleted"] = soft_delete_missing(client, name, live_ids)

        duration = time.time() - start
        logger.info(
            f"✓ {name} [{duration:.0f}s] — "
            f"new={counts['new']} updated={counts['updated']} "
            f"unchanged={counts['skipped']} deleted={counts['deleted']}"
        )
        log_run(client, name, "success", counts, duration)
        return {"source": name, "status": "success", "counts": counts}

    except Exception as e:
        duration = time.time() - start
        error_msg = str(e)
        logger.error(f"✗ {name} FAILED [{duration:.0f}s]: {e}", exc_info=True)
        status = "failed"
        if not dry_run and client:
            log_run(client, name, "failed", counts, duration, error_msg)
        return {"source": name, "status": "failed", "counts": counts, "error": error_msg}


def _suggest_fix(source: str, status: str, error: Optional[str]) -> Optional[str]:
    """Return a short suggested next action given a failed/partial run, or None."""
    err = (error or "").lower()
    if status == "failed":
        if "supabase" in err and any(x in err for x in ("unauthorized", "401", "403", "jwt")):
            return "Supabase auth — SUPABASE_SERVICE_KEY must be the service_role key (not anon)."
        if "supabase" in err or "postgres" in err or "relation" in err:
            return "DB error — verify schema matches schema.sql and Supabase project is up."
        if "playwright" in err and ("executable" in err or "install" in err):
            return "Chromium missing — CI must run `playwright install --with-deps chromium` before main.py."
        if "timeout" in err or "timed out" in err:
            return "Transient timeout — re-trigger the workflow once."
        if "telegram" in err:
            return "Ignore: delivery itself had an issue."
        return "See the full error above and the run logs."
    if status == "partial":
        if source == "dubizzle":
            return (
                "Imperva likely blocked every retry. Add DUBIZZLE_PROXY "
                "(UAE residential, e.g. IPRoyal PAYG ~$2/GB) as a repo secret."
            )
        return "Site returned 0 listings. Possible layout change or rate-limit — check scraper logs."
    return None


def _build_notification(results: list[dict]) -> tuple[str, bool]:
    """Build a per-run summary. Returns (text, has_issues)."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    failed = [r for r in results if r["status"] == "failed"]
    partial = [r for r in results if r["status"] == "partial"]
    has_issues = bool(failed or partial)

    header = "🚨 Scrape issues" if has_issues else "✅ Scrape complete"
    lines = [f"{header} — {ts}"]
    for r in results:
        source = r["source"]
        status = r["status"]
        c = r.get("counts", {})
        if status == "failed":
            err = (r.get("error") or "unknown error")[:200]
            lines.append(f"❌ {source} — {err}")
        elif status == "partial":
            lines.append(f"⚠️ {source} — 0 listings found")
        elif status == "dry_run":
            lines.append(f"🔍 {source} (dry run) — {c.get('found', 0)} found")
        else:
            lines.append(
                f"✅ {source} — found={c.get('found', 0)} "
                f"new={c.get('new', 0)} updated={c.get('updated', 0)} "
                f"deleted={c.get('deleted', 0)}"
            )
        fix = _suggest_fix(source, status, r.get("error"))
        if fix:
            lines.append(f"💡 Fix: {fix}")
    return "\n".join(lines), has_issues


def send_telegram(bot_token: str, chat_id: str, text: str):
    """Send a plain-text message via the Telegram Bot API."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = _requests.post(
            url,
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(
                f"Telegram send failed: HTTP {resp.status_code}: {resp.text[:200]}"
            )
    except Exception as e:
        logger.warning(f"Telegram send error: {e}")


def send_alert(webhook_url: str, text: str):
    """POST a summary to a generic webhook (Slack/Teams/etc)."""
    try:
        _requests.post(webhook_url, json={"text": text}, timeout=10)
    except Exception as e:
        logger.warning(f"Failed to send webhook alert: {e}")


def main():
    parser = argparse.ArgumentParser(description="Car listing scraper")
    parser.add_argument("--source", nargs="+", choices=list(SCRAPERS.keys()),
                        help="Sources to run (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scrape without writing to DB")
    parser.add_argument("--stagger", type=int, default=0,
                        help="Seconds to wait between sources (reduces block risk when running all)")
    args = parser.parse_args()

    sources = args.source or list(SCRAPERS.keys())

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")
    webhook_url  = os.environ.get("ALERT_WEBHOOK_URL")

    if not args.dry_run:
        if not supabase_url or not supabase_key:
            logger.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
            sys.exit(1)
        client = get_client(supabase_url, supabase_key)
    else:
        client = None

    logger.info(f"Sources: {sources} | dry_run={args.dry_run} | stagger={args.stagger}s")
    results = []

    for i, source in enumerate(sources):
        result = run_source(source, client, args.dry_run)
        results.append(result)
        if args.stagger and i < len(sources) - 1:
            logger.info(f"Stagger: waiting {args.stagger}s before next source...")
            time.sleep(args.stagger)

    # Summary
    logger.info("─" * 60)
    for r in results:
        c = r["counts"]
        logger.info(
            f"{r['source']:12} {r['status']:8} "
            f"found={c['found']:5} new={c.get('new',0):4} "
            f"updated={c.get('updated',0):4} deleted={c.get('deleted',0):4}"
        )

    summary_text, has_issues = _build_notification(results)

    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    tg_chat = os.environ.get("TELEGRAM_CHAT_ID")
    if tg_token and tg_chat:
        send_telegram(tg_token, tg_chat, summary_text)

    if webhook_url and has_issues:
        send_alert(webhook_url, summary_text)

    failed_sources = [r["source"] for r in results if r["status"] == "failed"]
    if failed_sources:
        logger.error(f"Failed sources: {failed_sources}")
        sys.exit(1)

    logger.info("All sources complete.")


if __name__ == "__main__":
    main()