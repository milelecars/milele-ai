"""
utils/base_scraper.py — Base class for all scrapers

Anti-detection layers (legitimate browser emulation):
  1. HTTP/2   — httpx negotiates H2 via ALPN, same as Chrome/Firefox.
                requests uses HTTP/1.1 which is a primary bot signal.

  2. Browser profiles — each profile is a real browser/OS combination with
                ALL headers that browser actually sends, in the order it sends them:
                UA + Sec-CH-UA + Sec-CH-UA-Mobile + Sec-CH-UA-Platform + Accept.
                Mismatched UA/CH-UA (e.g. Chrome UA without CH-UA headers) is a
                primary Cloudflare signal. Firefox profiles correctly omit CH-UA.

  3. Header ordering — httpx preserves dict insertion order. Chrome sends
                headers in a specific sequence; we replicate that sequence.

  4. Referer chain — sec-fetch-site transitions from 'none' (cold start) to
                'same-origin' (navigating within site), matching real browser behaviour.

  5. Cookie persistence — httpx client maintains cookie jar across requests
                within a session, same as a browser.

  6. Human timing — variable delays with gaussian jitter, occasional longer pauses
                to simulate reading time, not uniform mechanical intervals.

  7. Session warm-up — hits homepage before any listing requests to establish
                cookies, build session context, and look like organic traffic.

  8. Proxy support — SCRAPER_PROXY env var accepted (residential proxy URL).
                Rotated per session rebuild. Residential IPs bypass datacenter blacklists.

  9. Session rotation — client rebuilt every ~80-120 requests with a new profile
                to prevent long-lived session fingerprinting.

 10. Apify-first — for Cloudflare-protected sites (Dubizzle, DubiCars, YallaMotor)
                the scraper delegates to an Apify actor running headless Chrome with
                stealth patches. This is the correct solution for JS-challenge sites.
"""

import logging
import os
import random
import re
import time
from abc import ABC, abstractmethod
from typing import Generator, Optional

import httpx
from bs4 import BeautifulSoup
from tenacity import (
    RetryError,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Browser profiles
# Each entry is a complete, self-consistent browser identity.
# UA version, Sec-CH-UA version, and platform must all match.
# Firefox profiles omit Sec-CH-UA (Firefox does not send it).
# ─────────────────────────────────────────────────────────────────────────────

_BROWSER_PROFILES = [
    {
        "name": "chrome_124_win",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
    },
    {
        "name": "chrome_124_mac",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
    },
    {
        "name": "chrome_123_win",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="123", "Google Chrome";v="123", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
    },
    {
        "name": "chrome_123_mac",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="123", "Google Chrome";v="123", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
    },
    {
        "name": "edge_124_win",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
        "sec-ch-ua": '"Chromium";v="124", "Microsoft Edge";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9,ar;q=0.7",
    },
    {
        "name": "firefox_124_win",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "sec-ch-ua": None,
        "sec-ch-ua-mobile": None,
        "sec-ch-ua-platform": None,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.5",
    },
    {
        "name": "firefox_124_mac",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0",
        "sec-ch-ua": None,
        "sec-ch-ua-mobile": None,
        "sec-ch-ua-platform": None,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.5",
    },
    {
        "name": "firefox_123_win",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "sec-ch-ua": None,
        "sec-ch-ua-mobile": None,
        "sec-ch-ua-platform": None,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.5",
    },
]


def random_ua() -> str:
    """Return a UA string from a random profile (backward-compatibility)."""
    return random.choice(_BROWSER_PROFILES)["ua"]


def _random_profile() -> dict:
    return random.choice(_BROWSER_PROFILES)


def _build_headers(profile: dict, referer: Optional[str] = None) -> dict:
    """
    Build an ordered header dict matching what that browser actually sends.
    Chrome sends Sec-CH-UA before User-Agent — we preserve that order.
    Firefox omits Sec-CH-UA entirely.
    sec-fetch-site is 'none' on cold start, 'same-origin' once navigating.
    """
    h = {}
    if profile.get("sec-ch-ua"):
        h["sec-ch-ua"]                 = profile["sec-ch-ua"]
        h["sec-ch-ua-mobile"]          = profile["sec-ch-ua-mobile"]
        h["sec-ch-ua-platform"]        = profile["sec-ch-ua-platform"]
        h["upgrade-insecure-requests"] = "1"
    h["user-agent"]      = profile["ua"]
    h["accept"]          = profile["accept"]
    h["accept-language"] = profile["accept-language"]
    h["accept-encoding"] = "gzip, deflate, br, zstd"
    if referer:
        h["referer"]        = referer
        h["sec-fetch-site"] = "same-origin"
    else:
        h["sec-fetch-site"] = "none"
    h["sec-fetch-mode"] = "navigate"
    h["sec-fetch-user"] = "?1"
    h["sec-fetch-dest"] = "document"
    return h


# ─────────────────────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────────────────────

class ScraperHTTPError(Exception):
    def __init__(self, status_code: int, url: str):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code} on {url}")


# ─────────────────────────────────────────────────────────────────────────────
# Base scraper
# ─────────────────────────────────────────────────────────────────────────────

class BaseScraper(ABC):
    SOURCE: str = ""
    BASE_URL: str = ""
    REQUEST_DELAY = (2.5, 5.5)
    PAGE_DELAY    = (4.0, 8.0)
    TIMEOUT       = 28
    APIFY_ACTOR_ID: Optional[str] = None

    def __init__(self):
        self._client: Optional[httpx.Client] = None
        self._profile: Optional[dict] = None
        self._last_url: str = ""
        self._request_count: int = 0

    # ── Abstract ────────────────────────────────────────────────────────────

    @abstractmethod
    def listing_urls(self) -> Generator[str, None, None]: ...

    @abstractmethod
    def parse_listing(self, url: str, html: str) -> Optional[dict]: ...

    # ── HTTP client ─────────────────────────────────────────────────────────

    def _build_client(self) -> httpx.Client:
        self._profile = _random_profile()
        proxy_url = os.environ.get("SCRAPER_PROXY") or None
        client = httpx.Client(
            http2=True,
            headers=_build_headers(self._profile),
            follow_redirects=True,
            timeout=self.TIMEOUT,
            proxy=proxy_url,
        )
        logger.debug(
            f"[{self.SOURCE}] client built — profile={self._profile['name']} "
            f"proxy={'yes' if proxy_url else 'no'}"
        )
        return client

    def _warm_up(self):
        try:
            resp = self._client.get(self.BASE_URL, timeout=self.TIMEOUT)
            logger.debug(f"[{self.SOURCE}] warm-up {resp.status_code} {resp.http_version}")
            # 403 on warm-up = Cloudflare protected — expected, not fatal.
            # Scraper will use Apify path or fail gracefully on direct fetch.
        except Exception as e:
            logger.debug(f"[{self.SOURCE}] warm-up skipped: {e}")
        time.sleep(random.uniform(1.5, 3.0))

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            self._client = self._build_client()
            self._warm_up()
        return self._client

    def _rebuild_client(self, reason: str = ""):
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
        self._client = None
        self._last_url = ""
        self._request_count = 0
        logger.info(f"[{self.SOURCE}] client rebuilt — {reason}")
        _ = self.client   # trigger build + warm-up immediately

    # ── Human-like timing ───────────────────────────────────────────────────

    def _human_delay(self, band: tuple):
        lo, hi = band
        base   = random.uniform(lo, hi)
        jitter = random.gauss(0, (hi - lo) * 0.15)
        delay  = max(lo * 0.7, base + jitter)
        if random.random() < 0.05:           # 5% chance of a long reading pause
            delay += random.uniform(5, 15)
        time.sleep(delay)

    # ── Fetch ────────────────────────────────────────────────────────────────

    def fetch(self, url: str) -> Optional[str]:
        try:
            return self._fetch_with_retry(url)
        except RetryError:
            logger.error(f"[{self.SOURCE}] all retries exhausted: {url}")
            return None
        except Exception as e:
            logger.error(f"[{self.SOURCE}] fetch error: {e}")
            return None

    @retry(
        retry=retry_if_exception_type(ScraperHTTPError),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=6, max=90),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=False,
    )
    def _fetch_with_retry(self, url: str) -> Optional[str]:
        self._human_delay(self.REQUEST_DELAY)
        self._request_count += 1

        # Rotate session every ~80-120 requests
        if self._request_count > random.randint(80, 120):
            self._rebuild_client("session rotation")

        # Guard: profile can be None if client was never fully built
        if self._profile is None:
            self._profile = _random_profile()
        request_headers = _build_headers(self._profile, referer=self._last_url or None)
        resp = self.client.get(url, headers=request_headers)
        self._last_url = url

        if resp.status_code == 429:
            wait = random.uniform(30, 60)
            logger.warning(f"[{self.SOURCE}] 429 — backing off {wait:.0f}s")
            time.sleep(wait)
            raise ScraperHTTPError(429, url)

        if resp.status_code in (403, 503):
            logger.warning(f"[{self.SOURCE}] {resp.status_code} — rebuilding client")
            self._rebuild_client(f"HTTP {resp.status_code}")
            time.sleep(random.uniform(10, 20))
            raise ScraperHTTPError(resp.status_code, url)

        if resp.status_code == 404:
            return None

        resp.raise_for_status()
        return resp.text

    # ── Apify ────────────────────────────────────────────────────────────────

    def fetch_via_apify(self, actor_id: str, run_input: dict, apify_token: str,
                         max_items: int = 5000, timeout_secs: int = 3600) -> list[dict]:
        """
        Run an Apify actor and return its dataset items.

        Args:
            actor_id:     Apify actor ID, e.g. 'epctex/dubizzle-scraper'
            run_input:    Input dict passed to the actor (WITHOUT maxItems — use max_items param)
            apify_token:  Apify API token from APIFY_TOKEN env var
            max_items:    Max results to fetch (passed as call() param per apify-client 2.x API)
            timeout_secs: Max seconds to wait for actor to finish (default 1 hour)
        """
        try:
            from apify_client import ApifyClient
            c = ApifyClient(apify_token)
            logger.info(f"[{self.SOURCE}] Apify actor start: {actor_id} (max_items={max_items})")

            # In apify-client 2.x, max_items is a call() param NOT inside run_input
            run = c.actor(actor_id).call(
                run_input=run_input,
                max_items=max_items,
                timeout_secs=timeout_secs,
            )

            # run can be None if actor failed to start or timed out
            if run is None:
                logger.error(f"[{self.SOURCE}] Apify run returned None — actor may have failed to start")
                return []

            dataset_id = run.get("defaultDatasetId")
            if not dataset_id:
                logger.error(f"[{self.SOURCE}] Apify run has no defaultDatasetId: {run.get('status')}")
                return []

            items = list(c.dataset(dataset_id).iterate_items())
            logger.info(f"[{self.SOURCE}] Apify returned {len(items)} items (status={run.get('status')})")
            return items

        except Exception as e:
            err = str(e)
            if "not found" in err.lower() or "does not exist" in err.lower() or "404" in err:
                logger.error(
                    f"[{self.SOURCE}] Apify actor '{actor_id}' not found. "
                    f"Go to https://apify.com/store, search for '{self.SOURCE}', "
                    f"copy the actor ID and update APIFY_ACTOR_ID in scrapers/{self.SOURCE}.py"
                )
            elif "unauthorized" in err.lower() or "401" in err or "forbidden" in err.lower():
                logger.error(
                    f"[{self.SOURCE}] Apify token invalid or expired. "
                    f"Check APIFY_TOKEN in .env — get token from: "
                    f"https://console.apify.com/account/integrations"
                )
            elif "timeout" in err.lower():
                logger.error(f"[{self.SOURCE}] Apify actor timed out after {timeout_secs}s")
            else:
                logger.error(f"[{self.SOURCE}] Apify failed: {e}")
            return []

    # ── Data cleaning ────────────────────────────────────────────────────────

    def clean_price(self, raw) -> tuple[Optional[float], Optional[float], bool]:
        if not raw:
            return None, None, False
        raw = str(raw).strip()
        range_m = re.search(r"([\d,]+(?:\.\d+)?)\s*[-\u2013to]+\s*([\d,]+(?:\.\d+)?)", raw)
        if range_m:
            return self._parse_num(range_m.group(1)), self._parse_num(range_m.group(2)), True
        return self._parse_num(raw), None, False

    def clean_int(self, raw) -> Optional[int]:
        if raw is None:
            return None
        digits = re.sub(r"[^\d]", "", str(raw))
        return int(digits) if digits else None

    def _parse_num(self, s: str) -> Optional[float]:
        digits = re.sub(r"[^\d.]", "", str(s).replace(",", ""))
        try:
            return float(digits) if digits else None
        except ValueError:
            return None

    def filter_images(self, urls: list) -> list[str]:
        return [
            u for u in (urls or [])
            if isinstance(u, str) and u.startswith(("http://", "https://"))
        ][:20]

    def soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "lxml")

    # ── Run ──────────────────────────────────────────────────────────────────

    def run(self) -> list[dict]:
        results = []
        for url in self.listing_urls():
            html = self.fetch(url)
            if not html:
                continue
            try:
                listing = self.parse_listing(url, html)
            except Exception as e:
                logger.error(f"[{self.SOURCE}] parse error on {url}: {e}", exc_info=True)
                listing = None
            if listing:
                listing["source"] = self.SOURCE
                listing["url"]    = url
                results.append(listing)
            self._human_delay(self.PAGE_DELAY)
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
        logger.info(f"[{self.SOURCE}] complete — {len(results)} listings")
        return results