"""
scripts/test_detail.py — smoke-test Dubizzle detail extraction on ONE URL.

Usage:
    python -m scripts.test_detail <detail_url>

Example:
    python -m scripts.test_detail https://dubai.dubizzle.com/motors/used-cars/.../ID/

Prints:
    - whether the page loaded without hitting the Imperva interstitial
    - every key the JS extractor returned (with value previews)
    - the normalised dict that would be written to DB

Does not touch Supabase.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from scrapers.dubizzle import DubizzleScraper, _DETAIL_JS


def main():
    if len(sys.argv) != 2:
        print("usage: python -m scripts.test_detail <url>", file=sys.stderr)
        sys.exit(1)
    url = sys.argv[1]

    scraper = DubizzleScraper()

    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=False)
        context = scraper._new_context(browser, proxy_dict=None)
        page = context.new_page()

        print(f"→ loading {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(8_000)

        if scraper._is_blocked(page):
            print("✗ page is Imperva interstitial — extraction impossible")
            browser.close()
            sys.exit(2)

        print("✓ page not blocked — expanding accordions + See more toggles")
        try:
            page.evaluate(
                "() => document.querySelectorAll("
                "'[role=\"button\"][aria-expanded=\"false\"]').forEach(b => b.click())"
            )
            page.wait_for_timeout(500)
            page.evaluate(
                "() => document.querySelectorAll("
                "'[data-testid=\"feature-toggle\"]').forEach(b => b.click())"
            )
            page.wait_for_timeout(400)
        except Exception as e:
            print(f"(accordion expand error: {e})")

        raw = page.evaluate(_DETAIL_JS)
        print()
        print("=== raw JS extraction ===")
        print(json.dumps(raw, indent=2, default=str)[:3000])

        normalised = scraper._normalise_detail(raw)
        print()
        print("=== normalised (what gets written to DB) ===")
        for k, v in normalised.items():
            if v is None or v == {} or v == []:
                print(f"  {k:30s} None / empty")
            else:
                preview = str(v)[:80]
                print(f"  {k:30s} {preview}")

        non_null = sum(
            1 for v in normalised.values()
            if v is not None and v != {} and v != []
        )
        print()
        print(f"meaningful fields extracted: {non_null}/{len(normalised)}")
        if non_null == 0:
            print("✗ nothing meaningful — extractor selectors don't match this page")
        elif non_null < 5:
            print("⚠ very sparse extraction — page structure may differ from reference")
        else:
            print("✓ looks healthy")

        browser.close()


if __name__ == "__main__":
    main()
