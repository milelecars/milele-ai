"""
scrapers/dubizzle.py — Dubizzle UAE new cars (Playwright + stealth)

Dubizzle sits behind an Imperva/Distil JS challenge that returns "Pardon Our
Interruption" to any non-browser client. We drive a real Chromium via Playwright
with playwright-stealth to pass the challenge, then extract listings from the
JSON-LD island on each search results page (`mainEntity.itemListElement`).

Anti-detection layers applied:
    * Residential proxy (DUBIZZLE_PROXY / SCRAPER_PROXY) routed through the
      browser context — avoids datacenter IP blocks.
    * Block-page detection ("Pardon Our Interruption" interstitial). On hit:
      close context, cool down, rotate UA + viewport + proxy session, retry.
    * Randomised human-like waits and scrolls instead of fixed timeouts.

Env vars:
    DUBIZZLE_HEADLESS      "1" to run headless (default: 0 — visible browser
                           has a higher success rate against Imperva).
    DUBIZZLE_MAX_PAGES     pagination cap (default: 60).
    DUBIZZLE_SEARCH_URL    override search URL (default: new cars, 2015+,
                           price 1..100000 AED).
    DUBIZZLE_PROXY         residential proxy URL for this scraper only,
                           e.g. http://user:pass@gate.provider.com:22225.
    SCRAPER_PROXY          project-wide proxy fallback if DUBIZZLE_PROXY unset.

Requires: `pip install playwright playwright-stealth` and
          `playwright install chromium`.
"""

import logging
import os
import random
import re
import time
from typing import Generator, Optional
from urllib.parse import urlparse

from utils.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


DEFAULT_SEARCH_URL = (
    "https://uae.dubizzle.com/motors/new-cars/"
    "?price__lte=100000&price__gte=1"
    "&year__gte=2015&year__lte=2027"
)

_LISTINGS_JS = """
() => {
  const scripts = document.querySelectorAll('script[type="application/ld+json"]');
  for (const s of scripts) {
    try {
      const d = JSON.parse(s.textContent);
      if (d && d.mainEntity && d.mainEntity.itemListElement) {
        return d.mainEntity.itemListElement;
      }
    } catch (e) {}
  }
  return [];
}
"""

# Realistic recent-Chrome desktop user agents. Rotated per browser context.
_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
]

_VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1680, "height": 1050},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
]

# Signals that specifically identify the Imperva interstitial (not just the
# presence of Imperva on the site — distil_referrer / _Incapsula_Resource
# appear in every legit Dubizzle response too).
_BLOCK_TITLE = "Pardon Our Interruption"
_BLOCK_DOM_SELECTOR = "#interstitial-inprogress"


def _parse_proxy(url: str) -> Optional[dict]:
    """Convert 'scheme://user:pass@host:port' into Playwright's proxy dict."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        if not parsed.hostname:
            return None
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        proxy: dict = {"server": f"{parsed.scheme or 'http'}://{parsed.hostname}:{port}"}
        if parsed.username:
            proxy["username"] = parsed.username
        if parsed.password:
            proxy["password"] = parsed.password
        return proxy
    except Exception as e:
        logger.warning(f"[dubizzle] failed to parse proxy URL: {e}")
        return None


class DubizzleScraper(BaseScraper):
    SOURCE = "dubizzle"
    BASE_URL = "https://uae.dubizzle.com"
    APIFY_ACTOR_ID = None

    def listing_urls(self) -> Generator[str, None, None]:
        return iter([])

    def parse_listing(self, url: str, html: str) -> Optional[dict]:
        return None

    MAX_BLOCK_RETRIES = 3

    def _new_context(self, browser, proxy_dict: Optional[dict]):
        kwargs: dict = {
            "viewport": random.choice(_VIEWPORTS),
            "user_agent": random.choice(_USER_AGENTS),
            "locale": "en-AE",
            "timezone_id": "Asia/Dubai",
        }
        if proxy_dict:
            kwargs["proxy"] = proxy_dict
        return browser.new_context(**kwargs)

    def _is_blocked(self, page) -> bool:
        try:
            if _BLOCK_TITLE in (page.title() or ""):
                return True
        except Exception:
            pass
        try:
            if page.query_selector(_BLOCK_DOM_SELECTOR) is not None:
                return True
        except Exception:
            pass
        return False

    def _human_wait(self, page, lo_ms: int, hi_ms: int):
        base = random.uniform(lo_ms, hi_ms)
        jitter = random.gauss(0, (hi_ms - lo_ms) * 0.12)
        delay = max(lo_ms * 0.7, base + jitter)
        if random.random() < 0.05:  # occasional reading pause
            delay += random.uniform(5_000, 15_000)
        page.wait_for_timeout(int(delay))

    def _scroll_like_human(self, page):
        try:
            height = page.evaluate("document.body.scrollHeight") or 3000
        except Exception:
            height = 3000
        steps = random.randint(3, 6)
        for i in range(1, steps + 1):
            y = int(height * i / steps) + random.randint(-50, 50)
            try:
                page.evaluate(f"window.scrollTo(0, {y})")
            except Exception:
                return
            page.wait_for_timeout(random.randint(500, 1200))

    def run(self) -> list[dict]:
        try:
            from playwright.sync_api import sync_playwright
            from playwright_stealth import Stealth
        except ImportError as e:
            logger.error(
                f"[dubizzle] Playwright not installed: {e}. "
                f"Run: pip install playwright playwright-stealth && playwright install chromium"
            )
            return []

        headless = os.environ.get("DUBIZZLE_HEADLESS", "0") == "1"
        try:
            max_pages = int(os.environ.get("DUBIZZLE_MAX_PAGES", "60"))
        except ValueError:
            max_pages = 60
        search_url = os.environ.get("DUBIZZLE_SEARCH_URL") or DEFAULT_SEARCH_URL
        proxy_url = (
            os.environ.get("DUBIZZLE_PROXY")
            or os.environ.get("SCRAPER_PROXY")
            or ""
        )
        proxy_dict = _parse_proxy(proxy_url)

        logger.info(
            f"[dubizzle] Playwright run — headless={headless}, "
            f"max_pages={max_pages}, proxy={'yes' if proxy_dict else 'no'}"
        )

        results: list[dict] = []
        seen_ids: set[str] = set()

        with Stealth().use_sync(sync_playwright()) as p:
            browser = p.chromium.launch(headless=headless)
            context = self._new_context(browser, proxy_dict)
            page = context.new_page()

            def load_page(url: str, cold: bool) -> bool:
                """Navigate to url; on Imperva interstitial, rotate context and retry."""
                nonlocal context, page
                for attempt in range(self.MAX_BLOCK_RETRIES):
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    except Exception as e:
                        logger.warning(f"[dubizzle] goto error: {e}")
                        self._human_wait(page, 5_000, 15_000)
                        continue

                    if cold:
                        page.wait_for_timeout(15_000)  # let Imperva challenge clear
                    else:
                        self._human_wait(page, 5_000, 12_000)

                    if not self._is_blocked(page):
                        return True

                    logger.warning(
                        f"[dubizzle] blocked by Imperva "
                        f"(attempt {attempt + 1}/{self.MAX_BLOCK_RETRIES}) — rotating session"
                    )
                    try:
                        context.close()
                    except Exception:
                        pass
                    cooldown = random.uniform(15, 45)
                    logger.info(f"[dubizzle] cooldown {cooldown:.0f}s before retry")
                    time.sleep(cooldown)
                    context = self._new_context(browser, proxy_dict)
                    page = context.new_page()
                    cold = True  # treat post-rotation load as a cold start
                return False

            try:
                logger.info("[dubizzle] Loading page 1")
                if not load_page(search_url, cold=True):
                    logger.error(
                        "[dubizzle] could not bypass Imperva on page 1 after "
                        f"{self.MAX_BLOCK_RETRIES} attempts — aborting"
                    )
                    return []

                for page_num in range(1, max_pages + 1):
                    if page_num > 1:
                        next_url = f"{search_url}&page={page_num}"
                        logger.info(f"[dubizzle] Loading page {page_num}")
                        if not load_page(next_url, cold=False):
                            logger.warning(
                                f"[dubizzle] gave up on page {page_num} "
                                f"after {self.MAX_BLOCK_RETRIES} block retries"
                            )
                            break
                        self._scroll_like_human(page)

                    items = page.evaluate(_LISTINGS_JS) or []
                    logger.info(f"[dubizzle] page {page_num}: {len(items)} items")

                    if not items:
                        logger.warning(
                            f"[dubizzle] page {page_num} returned 0 items — stopping"
                        )
                        break

                    new_on_page = 0
                    for wrapper in items:
                        listing = self._normalise(wrapper)
                        if not listing:
                            continue
                        ext = listing["external_id"]
                        if ext in seen_ids:
                            continue
                        seen_ids.add(ext)
                        results.append(listing)
                        new_on_page += 1

                    if new_on_page == 0:
                        logger.info(
                            f"[dubizzle] page {page_num} had only duplicates — stopping"
                        )
                        break
            finally:
                try:
                    browser.close()
                except Exception:
                    pass

        logger.info(f"[dubizzle] complete — {len(results)} unique listings")
        return results

    def _normalise(self, wrapper: dict) -> Optional[dict]:
        try:
            car = wrapper.get("item") if isinstance(wrapper, dict) and "item" in wrapper else wrapper
            if not isinstance(car, dict):
                return None

            url = self._pick_url(car)
            external_id = self._external_id(url, car)
            if not external_id:
                return None

            name = self._pick_str(car.get("name"))
            year = self.clean_int(car.get("vehicleModelDate"))
            if not year and name:
                m = re.search(r"\b(19|20)\d{2}\b", name)
                if m:
                    year = int(m.group())

            make = self._pick_nested_name(car.get("brand")) or self._pick_str(car.get("manufacturer"))
            model = self._pick_str(car.get("model"))

            offers = car.get("offers")
            if isinstance(offers, list) and offers:
                offers = offers[0]
            offers = offers if isinstance(offers, dict) else {}

            price_raw = car.get("price") or offers.get("price")
            price, price_max, _ = self.clean_price(price_raw)

            area_served = offers.get("areaServed") or car.get("areaServed") or {}
            address = area_served.get("address") if isinstance(area_served, dict) else {}
            address = address if isinstance(address, dict) else {}
            emirate = self._pick_str(address.get("addressRegion")) or "Dubai"
            area = self._pick_str(address.get("addressLocality"))

            offered_by = offers.get("offeredBy") or {}
            offered_by = offered_by if isinstance(offered_by, dict) else {}
            seller_name = self._pick_str(offered_by.get("name"))
            additional = str(offered_by.get("additionalType") or "").lower()
            seller_type = "dealer" if "dealer" in additional or seller_name else "private"

            mileage_raw = car.get("mileageFromOdometer")
            if isinstance(mileage_raw, dict):
                mileage_km = self.clean_int(mileage_raw.get("value"))
            else:
                mileage_km = self.clean_int(mileage_raw)
            if mileage_km is None:
                mileage_km = 0

            condition_raw = str(car.get("itemCondition") or "").lower()
            condition = "used" if "used" in condition_raw else "new"

            images = self._collect_images(car)

            return {
                "source": self.SOURCE,
                "url": url,
                "external_id": external_id,
                "make": make,
                "model": model,
                "variant": None,
                "year": year,
                "body_type": self._pick_str(car.get("bodyType")),
                "condition": condition,
                "mileage_km": mileage_km,
                "fuel_type": self._pick_str(car.get("fuelType")),
                "transmission": self._pick_str(car.get("vehicleTransmission")),
                "engine_cc": self.clean_int(
                    car.get("vehicleEngine", {}).get("engineDisplacement")
                    if isinstance(car.get("vehicleEngine"), dict) else None
                ),
                "cylinders": None,
                "color": self._pick_str(car.get("color")),
                "doors": self.clean_int(car.get("numberOfDoors")),
                "price_aed": price,
                "price_aed_max": price_max,
                "price_negotiable": False,
                "emirate": emirate,
                "area": area,
                "seller_type": seller_type,
                "seller_name": seller_name,
                "seller_phone": None,
                "image_urls": images,
                "description": self._pick_str(car.get("description")),
                "specs": {},
            }
        except Exception as e:
            logger.error(f"[dubizzle] normalise error: {e}")
            return None

    def _pick_url(self, car: dict) -> str:
        raw = car.get("url")
        if not raw:
            absu = car.get("absolute_url")
            if isinstance(absu, dict):
                raw = absu.get("en") or absu.get("ar")
            elif isinstance(absu, str):
                raw = absu
        raw = raw or ""
        if raw and not raw.startswith("http"):
            raw = self.BASE_URL + (raw if raw.startswith("/") else "/" + raw)
        return raw

    def _external_id(self, url: str, car: dict) -> str:
        for key in ("id", "listing_id", "adId", "objectID", "sku", "productID"):
            v = car.get(key)
            if v:
                return str(v)
        if url:
            # Dubizzle listing URLs end with ".../slug---<32-char hex>/"
            m = re.search(r"---([0-9a-f]{16,})/?$", url)
            if m:
                return m.group(1)
            # Fallback: last non-empty path segment.
            parts = [seg for seg in url.rstrip("/").split("/") if seg]
            if parts:
                return parts[-1]
            return url
        return ""

    def _pick_str(self, v) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, str):
            return v or None
        if isinstance(v, dict):
            return v.get("en") or v.get("name") or v.get("@value") or None
        return str(v)

    def _pick_nested_name(self, v) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, dict):
            name = v.get("name")
            return self._pick_str(name)
        if isinstance(v, str):
            return v
        return None

    def _collect_images(self, car: dict) -> list[str]:
        raw: list = []
        img = car.get("image")
        if isinstance(img, list):
            raw.extend(img)
        elif isinstance(img, str):
            raw.append(img)
        elif isinstance(img, dict):
            raw.append(img.get("url") or img.get("contentUrl") or "")
        photos = car.get("photos")
        if isinstance(photos, dict):
            for v in photos.values():
                if isinstance(v, str):
                    raw.append(v)
                elif isinstance(v, list):
                    raw.extend(v)
        elif isinstance(photos, list):
            raw.extend(photos)
        normalised: list[str] = []
        for u in raw:
            if isinstance(u, dict):
                u = u.get("url") or u.get("contentUrl") or ""
            if isinstance(u, str):
                normalised.append(u)
        return self.filter_images(normalised)
