#!/usr/bin/env python
"""
scripts/health_check.py — Quick smoke test for all scrapers

Fetches the FIRST listing only from each source and prints a one-line summary.
No DB writes. Useful for:
  - Manually verifying a scraper still works after a site layout change
  - Diagnosing which source is failing in production
  - Pre-deploy sanity check

Usage:
    python scripts/health_check.py                    # all sources
    python scripts/health_check.py dubizzle dubicars  # specific sources
    python scripts/health_check.py --timeout 60       # custom per-source timeout (seconds)

Exit code 0 = all sources healthy
Exit code 1 = one or more sources failed
"""

import argparse
import importlib
import logging
import sys
import os
import time
import signal
from typing import Optional

# Resolve project root regardless of invocation style
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.WARNING,    # suppress debug noise
    format="%(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

SCRAPERS = {
    "dubizzle":   ("scrapers.dubizzle",    "DubizzleScraper"),
    "dubicars":   ("scrapers.dubicars",    "DubiCarsScraper"),
    "yallamotor": ("scrapers.yallamotor",  "YallaMotorScraper"),
    "carswitch":  ("scrapers.carswitch",   "CarSwitchScraper"),
    "sellanycar": ("scrapers.sellanycar",  "SellAnyCarScraper"),
}

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


class TimeoutError(Exception):
    pass


def _timeout_handler(signum, frame):
    raise TimeoutError("Timed out")


def check_source(name: str, timeout: int) -> dict:
    """Fetch one listing from a source. Returns result dict."""
    result = {"source": name, "ok": False, "listing": None, "error": None, "elapsed": 0}
    start = time.time()

    # Set alarm (Unix only — GitHub Actions runs on Linux, so this is fine)
    if hasattr(signal, "SIGALRM"):
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(timeout)

    try:
        mod_path, cls_name = SCRAPERS[name]
        mod = importlib.import_module(mod_path)
        cls = getattr(mod, cls_name)
        scraper = cls()

        # Grab first URL only
        first_url = None
        for url in scraper.listing_urls():
            first_url = url
            break

        if not first_url:
            result["error"] = "No listing URLs returned (pagination may be broken)"
            return result

        html = scraper.fetch(first_url)
        if not html:
            result["error"] = f"fetch() returned None for {first_url}"
            return result

        listing = scraper.parse_listing(first_url, html)
        if not listing:
            result["error"] = f"parse_listing() returned None for {first_url}"
            return result

        # Validate required fields
        missing = [f for f in ("external_id", "make", "price_aed") if not listing.get(f)]
        if missing:
            result["error"] = f"Missing required fields: {missing} — listing may be partially parsed"
            result["listing"] = listing  # still return so we can inspect
            result["ok"] = False
        else:
            result["ok"] = True
            result["listing"] = listing

    except TimeoutError:
        result["error"] = f"Timed out after {timeout}s"
    except Exception as e:
        result["error"] = str(e)
    finally:
        if hasattr(signal, "SIGALRM"):
            signal.alarm(0)
        result["elapsed"] = round(time.time() - start, 1)

    return result


def format_result(r: dict) -> str:
    src = f"{BOLD}{r['source']:12}{RESET}"
    elapsed = f"[{r['elapsed']}s]"

    if r["ok"] and r["listing"]:
        l = r["listing"]
        price = f"AED {l['price_aed']:,.0f}" if l.get("price_aed") else "no price"
        mileage = f"{l['mileage_km']:,} km" if l.get("mileage_km") else "no mileage"
        detail = f"{l.get('year','')} {l.get('make','')} {l.get('model','')} — {price} — {mileage}"
        return f"{GREEN}✓{RESET} {src} {elapsed:8}  {detail}"
    else:
        warn = f"{YELLOW}⚠{RESET}" if r.get("listing") else f"{RED}✗{RESET}"
        return f"{warn} {src} {elapsed:8}  {r.get('error', 'unknown error')}"


def main():
    parser = argparse.ArgumentParser(description="Car scraper health check")
    parser.add_argument("sources", nargs="*", choices=list(SCRAPERS.keys()),
                        help="Sources to check (default: all)")
    parser.add_argument("--timeout", type=int, default=120,
                        help="Per-source timeout in seconds (default: 120)")
    args = parser.parse_args()

    sources = args.sources or list(SCRAPERS.keys())

    print(f"\n{BOLD}Car Scraper Health Check{RESET} — {len(sources)} source(s), {args.timeout}s timeout each\n")
    print(f"{'─' * 70}")

    results = []
    for source in sources:
        print(f"  Checking {source}...", end="", flush=True)
        r = check_source(source, args.timeout)
        results.append(r)
        print(f"\r{format_result(r)}")

    print(f"{'─' * 70}")

    ok_count = sum(1 for r in results if r["ok"])
    fail_count = len(results) - ok_count
    status_color = GREEN if fail_count == 0 else RED
    print(f"\n{status_color}{BOLD}{ok_count}/{len(results)} sources healthy{RESET}")

    if fail_count > 0:
        print(f"\n{YELLOW}Failed sources:{RESET}")
        for r in results:
            if not r["ok"]:
                print(f"  • {r['source']}: {r['error']}")
        sys.exit(1)

    print()


if __name__ == "__main__":
    main()
    