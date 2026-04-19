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
from datetime import date, datetime, timezone
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


# JS extractor for the detail page. Runs in the browser context, returns a
# JSON-serialisable dict: overview k/v, features by category, all images, and
# title/description/location/dealer.
_DETAIL_JS = r"""
() => {
  const text = (sel) => {
    const el = document.querySelector(sel);
    return el ? el.textContent.trim() : null;
  };

  // Overview: every element like <... data-testid="overview-<key>-value">
  const overview = {};
  document.querySelectorAll('[data-testid^="overview-"][data-testid$="-value"]')
    .forEach(el => {
      const k = el.getAttribute('data-testid')
                   .replace(/^overview-/, '').replace(/-value$/, '');
      overview[k] = el.textContent.trim();
    });

  // Features: 4 accordion sections. For each, find the accordion container,
  // then gather all rendered feature strings under it. Clicking "See more"
  // before calling this (done from Python) reveals collapsed items.
  const features = {};
  const cats = [
    'driver_assistance_and_safety',
    'entertainment_and_technology',
    'comfort_and_convenience',
    'exterior_features',
  ];
  for (const cat of cats) {
    const label = document.querySelector(`[data-testid="details-label-${cat}"]`);
    if (!label) continue;
    const accordion = label.closest('.MuiPaper-root') ||
                      label.closest('[class*="Accordion"]') ||
                      label.parentElement;
    const scope = accordion || document;
    const nodes = scope.querySelectorAll(
      '[data-testid^="feature-item-value-"] .MuiListItemText-primary, ' +
      '[data-testid^="feature-item-value-"] span'
    );
    const seen = new Set();
    const out = [];
    for (const n of nodes) {
      const t = (n.textContent || '').trim();
      if (t && !seen.has(t)) { seen.add(t); out.push(t); }
    }
    features[cat] = out;
  }

  // Images: dedupe all <img> tags pointing at the dbz-images CDN.
  // Skip dealer profile logos and any non-listing images.
  const imgs = [];
  const seenImg = new Set();
  document.querySelectorAll('img[src*="dbz-images.dubizzle.com/images/"]')
    .forEach(el => {
      const s = el.getAttribute('src') || '';
      if (!s) return;
      if (s.includes('/profiles/')) return;
      const base = s.split('?')[0];
      if (seenImg.has(base)) return;
      seenImg.add(base);
      imgs.push(s);
    });

  // Also try __NEXT_DATA__ for the full gallery (usually has 10-20 photos).
  try {
    const nd = document.getElementById('__NEXT_DATA__');
    if (nd) {
      const data = JSON.parse(nd.textContent);
      const walk = (obj, depth) => {
        if (!obj || depth > 8) return;
        if (Array.isArray(obj)) { obj.forEach(v => walk(v, depth + 1)); return; }
        if (typeof obj !== 'object') return;
        for (const [key, val] of Object.entries(obj)) {
          if ((key === 'photos' || key === 'images' || key === 'gallery')
              && Array.isArray(val)) {
            for (const p of val) {
              const u = typeof p === 'string'
                ? p
                : (p && (p.url || p.contentUrl || p.src)) || null;
              if (u && u.includes('dbz-images.dubizzle.com')) {
                const base = u.split('?')[0];
                if (!seenImg.has(base)) { seenImg.add(base); imgs.push(u); }
              }
            }
          }
          walk(val, depth + 1);
        }
      };
      walk(data, 0);
    }
  } catch (e) {}

  const dealerLogoEl = document.querySelector('[data-testid="logo"] img');

  return {
    overview,
    features,
    images: imgs,
    title: text('[data-testid="listing-name"]'),
    description: text('[data-testid="description"]'),
    postedOn: text('[data-testid="posted-on"]'),
    locationText: text('[data-testid="listing-location-map"]'),
    dealerName: text('[data-testid="name"]'),
    dealerLogoUrl: dealerLogoEl ? (dealerLogoEl.getAttribute('src') || null) : null,
  };
}
"""

_POSTED_ON_RE = re.compile(
    r"(\d{1,2})\s*(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})"
)
_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_posted_on(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    m = _POSTED_ON_RE.search(raw)
    if not m:
        return None
    day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
    month = _MONTHS.get(month_name)
    if not month:
        return None
    try:
        return date(int(year), month, int(day)).isoformat()
    except Exception:
        return None


def _yesno_to_bool(raw: Optional[str]) -> Optional[bool]:
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if s in ("yes", "y", "true", "1"):
        return True
    if s in ("no", "n", "false", "0"):
        return False
    return None


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
    SUPPORTS_DETAIL = True

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

    def run(
        self,
        detail_plan: Optional[dict] = None,
        db_client=None,
        **_,
    ) -> list[dict]:
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

                # Intermediate commit: persist list-level data for every
                # listing BEFORE the long detail phase so that a crash during
                # enrichment can't destroy list-scrape results.
                if db_client and results:
                    try:
                        from utils.db import upsert_listings
                        self._intermediate_upsert_counts = upsert_listings(
                            db_client, results
                        )
                        logger.info(
                            f"[{self.SOURCE}] intermediate list upsert: "
                            f"{self._intermediate_upsert_counts}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"[{self.SOURCE}] intermediate upsert failed ({e}); "
                            f"new listings at risk if detail phase crashes"
                        )

                if detail_plan and results:
                    self._enrich_with_detail(
                        page, results, detail_plan, db_client=db_client
                    )
            finally:
                try:
                    browser.close()
                except Exception:
                    pass

        logger.info(f"[dubizzle] complete — {len(results)} unique listings")
        return results

    # ── Detail enrichment ───────────────────────────────────────────────────

    def _enrich_with_detail(
        self,
        page,
        results: list[dict],
        plan: dict,
        db_client=None,
    ):
        known = plan.get("known_external_ids") or set()
        backfill = plan.get("backfill_external_ids") or set()
        try:
            batch_size = int(plan.get("batch_size") or 100)
        except (TypeError, ValueError):
            batch_size = 100

        new_ids = {r["external_id"] for r in results} - known
        # Prioritise new > backfill, cap to batch_size.
        ordered_ids: list[str] = list(new_ids)
        ordered_ids += [rid for rid in backfill if rid not in new_ids]
        ordered_ids = ordered_ids[:batch_size]
        to_detail = set(ordered_ids)

        if not to_detail:
            logger.info(f"[{self.SOURCE}] detail: nothing to enrich this run")
        else:
            n_new = len(new_ids & to_detail)
            n_backfill = len(to_detail) - n_new
            logger.info(
                f"[{self.SOURCE}] detail: enriching {len(to_detail)} "
                f"(new={n_new}, backfill={n_backfill})"
            )

        by_id = {r["external_id"]: r for r in results}
        succeeded: set[str] = set()
        for i, ext_id in enumerate(ordered_ids, 1):
            listing = by_id.get(ext_id)
            if not listing or not listing.get("url"):
                continue
            detail = self._fetch_detail(page, listing["url"])

            # Distinguish "fetch failed" (None) from "fetch returned an empty
            # extraction" (dict full of Nones). Both are unusable; the second
            # used to falsely log as ✓ and silently drop data.
            meaningful = {}
            if detail:
                meaningful = {
                    k: v for k, v in detail.items()
                    if v is not None and v != {} and v != []
                }

            if not meaningful:
                logger.warning(
                    f"[{self.SOURCE}] detail {i}/{len(to_detail)} ✗ {ext_id} "
                    f"(fetch empty — extractor found nothing meaningful)"
                )
                self._human_wait(page, 2_000, 5_000)
                continue

            for k, v in meaningful.items():
                listing[k] = v
            listing["detail_scraped_at"] = datetime.now(timezone.utc).isoformat()
            succeeded.add(ext_id)

            # Incremental commit. Non-fatal on error.
            wrote = False
            if db_client:
                try:
                    from utils.db import update_detail_fields
                    wrote = update_detail_fields(
                        db_client, self.SOURCE, ext_id, meaningful
                    )
                except Exception as e:
                    logger.warning(
                        f"[{self.SOURCE}] incremental commit failed "
                        f"for {ext_id}: {e}"
                    )

            logger.info(
                f"[{self.SOURCE}] detail {i}/{len(to_detail)} ✓ {ext_id} "
                f"(fields={len(meaningful)}, persisted={wrote})"
            )
            self._human_wait(page, 2_000, 5_000)

        # For known-but-not-enriched-this-run listings, drop list-level fields
        # that would otherwise overwrite the richer detail data stored in DB
        # from a previous run.
        for r in results:
            if r["external_id"] in known and r["external_id"] not in succeeded:
                r.pop("image_urls", None)
                r.pop("description", None)

    def _fetch_detail(self, page, url: str) -> Optional[dict]:
        try:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            except Exception as e:
                logger.warning(f"[{self.SOURCE}] detail goto failed {url}: {e}")
                return None

            self._human_wait(page, 4_000, 8_000)

            if self._is_blocked(page):
                logger.warning(f"[{self.SOURCE}] detail blocked: {url}")
                return None

            # Expand all collapsed accordions + click every "See more" toggle
            # so all feature items materialise in the DOM before we extract.
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
            except Exception:
                pass

            raw = page.evaluate(_DETAIL_JS)
            if not raw:
                return None
            return self._normalise_detail(raw)
        except Exception as e:
            logger.warning(f"[{self.SOURCE}] detail parse failed for {url}: {e}")
            return None

    def _normalise_detail(self, raw: dict) -> dict:
        overview = raw.get("overview") or {}

        images = self._normalise_detail_images(raw.get("images") or [])

        return {
            "trim": self._pick_str(overview.get("motors_trim")),
            "horsepower_text": self._pick_str(overview.get("horsepower")),
            "engine_capacity_cc_text": self._pick_str(overview.get("engine_capacity_cc")),
            "seating_capacity_text": self._pick_str(overview.get("seating_capacity")),
            "interior_color": self._pick_str(overview.get("interior_color")),
            "target_market": self._pick_str(overview.get("target_market")),
            "warranty": _yesno_to_bool(overview.get("warranty")),
            "posted_at": _parse_posted_on(raw.get("postedOn")),
            "features": raw.get("features") or {},
            "dealer_name": self._pick_str(raw.get("dealerName")),
            "dealer_logo_url": self._pick_str(raw.get("dealerLogoUrl")),
            "image_urls": images,
            "description": self._pick_str(raw.get("description")),
            "body_type": self._pick_str(overview.get("body_type")),
            "fuel_type": self._pick_str(overview.get("fuel_type")),
            "cylinders": self.clean_int(overview.get("no_of_cylinders")),
            "doors": self.clean_int(overview.get("doors")),
            "area": self._pick_str(raw.get("locationText")),
        }

    @staticmethod
    def _normalise_detail_images(urls: list) -> list[str]:
        """Dedupe + prefer the `impolicy=dpv` CDN size (better for Telegram)."""
        seen: set[str] = set()
        out: list[str] = []
        for u in urls:
            if not isinstance(u, str) or not u.startswith("http"):
                continue
            # Normalise CDN size policy: prefer 'dpv' (larger) over 'carousel'.
            if "impolicy=" in u:
                u = re.sub(r"impolicy=[^&]+", "impolicy=dpv", u)
            base = u.split("?")[0]
            if base in seen:
                continue
            seen.add(base)
            out.append(u)
        return out[:20]

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
