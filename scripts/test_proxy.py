"""
scripts/test_proxy.py — verify DUBIZZLE_PROXY works against Dubizzle.

Runs 3 requests through Playwright with the proxy and prints:
  - the external IP reported by ipinfo.io (should differ between runs
    if the proxy rotates)
  - whether Dubizzle's homepage loads without the Imperva interstitial

Usage:
    export DUBIZZLE_PROXY='http://user:pass_country-ae@geo.iproyal.com:12321'
    python -m scripts.test_proxy
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from scrapers.dubizzle import DubizzleScraper, _parse_proxy


def main():
    raw = os.environ.get("DUBIZZLE_PROXY") or os.environ.get("SCRAPER_PROXY")
    if not raw:
        print("✗ DUBIZZLE_PROXY is not set. Export it first, then re-run.", file=sys.stderr)
        sys.exit(1)

    proxy_dict = _parse_proxy(raw)
    if not proxy_dict:
        print(f"✗ Couldn't parse proxy URL: {raw!r}", file=sys.stderr)
        sys.exit(1)

    print(f"→ Using proxy server={proxy_dict.get('server')}")
    print(f"  username={'set' if proxy_dict.get('username') else 'none'}")
    print(f"  password={'set' if proxy_dict.get('password') else 'none'}")
    print()

    scraper = DubizzleScraper()

    seen_ips = []
    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=False)

        # 3 fresh contexts → see if proxy returns a different IP each time
        for trial in range(1, 4):
            ctx = scraper._new_context(browser, proxy_dict)
            page = ctx.new_page()
            try:
                page.goto("https://ipinfo.io/json", timeout=30_000)
                body = page.evaluate("() => document.body.innerText")
                print(f"[trial {trial}] ipinfo response (first 200 chars):")
                print(f"  {body[:200]}")
                # quick IP extract
                import re as _re
                m = _re.search(r'"ip":\s*"([^"]+)"', body)
                cc = _re.search(r'"country":\s*"([^"]+)"', body)
                if m:
                    seen_ips.append(m.group(1))
                    tag = f"ip={m.group(1)} country={cc.group(1) if cc else '?'}"
                    print(f"  → {tag}")
            except Exception as e:
                print(f"[trial {trial}] ipinfo failed: {e}")
            print()
            ctx.close()

        # Final trial: hit Dubizzle directly and see if interstitial shows
        print("=== Dubizzle home test ===")
        ctx = scraper._new_context(browser, proxy_dict)
        page = ctx.new_page()
        try:
            page.goto("https://uae.dubizzle.com/", wait_until="domcontentloaded", timeout=45_000)
            page.wait_for_timeout(8_000)
            blocked = scraper._is_blocked(page)
            title = page.title()
            print(f"  title: {title!r}")
            print(f"  blocked: {blocked}")
            if blocked:
                print("  ✗ Imperva interstitial served even with proxy — proxy IP may not be residential, or region wrong")
            else:
                print("  ✓ Dubizzle home loaded cleanly through proxy")
        except Exception as e:
            print(f"  ✗ goto failed: {e}")

        browser.close()

    # Summary
    print()
    print("=== SUMMARY ===")
    print(f"IPs seen in 3 trials: {seen_ips}")
    unique = len(set(seen_ips))
    if unique == 0:
        print("✗ No IPs collected — proxy is broken. Check URL format and credentials.")
        sys.exit(2)
    elif unique == 1:
        print("⚠ Same IP on all 3 trials — your proxy is in 'sticky' mode.")
        print("  For our use case we want rotating. Reconfigure the endpoint.")
    else:
        print(f"✓ {unique} different IPs — rotating proxy is working.")
    # country hint
    if seen_ips and any(ip.startswith("82.") or ip.startswith("5.32.") for ip in seen_ips):
        print("  (Looks like UAE-range IPs — good.)")


if __name__ == "__main__":
    main()
