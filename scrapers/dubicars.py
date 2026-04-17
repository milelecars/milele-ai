"""
scrapers/dubicars.py — DubiCars UAE new cars (direct HTTP)

Scrapes: https://www.dubicars.com/new-cars-for-sale
Uses JSON-LD structured data where available, falls back to HTML spec table.
No Apify — direct HTTP with browser profile emulation.
"""

import json
import logging
import re
import time
from typing import Generator, Optional

from utils.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class DubiCarsScraper(BaseScraper):
    SOURCE = "dubicars"
    BASE_URL = "https://www.dubicars.com"
    APIFY_ACTOR_ID = None  # Using direct HTTP

    # New cars only
    SEARCH_URL = "https://www.dubicars.com/new-cars-for-sale?page={page}&sort=date_desc"

    def listing_urls(self) -> Generator[str, None, None]:
        page = 1
        while True:
            html = self.fetch(self.SEARCH_URL.format(page=page))
            if not html:
                break
            soup = self.soup(html)
            cards = soup.select("a[href*='/new-cars/'], a[href*='/cars/new/'], a[href*='/new/']")
            # Also try generic listing card pattern
            if not cards:
                cards = soup.select(".car-card a, .listing-card a, article a, [class*='card'] a")

            links = list(dict.fromkeys([
                c["href"] for c in cards
                if c.get("href") and re.search(r"/cars?/[^?#]", c.get("href", ""))
                and not c.get("href", "").endswith("/new-cars-for-sale")
                and "?" not in c.get("href", "").split("/")[-1]
            ]))

            if not links:
                logger.warning(f"[{self.SOURCE}] No listings on page {page} — stopping")
                break

            for href in links:
                yield href if href.startswith("http") else self.BASE_URL + href

            if not soup.select_one("a[rel='next'], .pagination .next, [aria-label='Next']"):
                break
            page += 1
            time.sleep(4)

    def parse_listing(self, url: str, html: str) -> Optional[dict]:
        soup = self.soup(html)
        ld = self._extract_jsonld(soup)

        def spec(keys: list) -> Optional[str]:
            for row in soup.select("li.spec-item, .car-specs li, .details-list li, dl dt, table tr, [class*='spec'] li"):
                label_el = row.select_one("span:first-child, dt, td:first-child, .label")
                val_el   = row.select_one("span:last-child, dd, td:last-child, .value, strong")
                if not (label_el and val_el):
                    continue
                label = label_el.get_text(strip=True).lower()
                val   = val_el.get_text(strip=True)
                for k in keys:
                    if label == k:
                        return val
            # fuzzy pass
            for row in soup.select("li.spec-item, .car-specs li, table tr"):
                label_el = row.select_one("span:first-child, dt, td:first-child, .label")
                val_el   = row.select_one("span:last-child, dd, td:last-child, .value")
                if not (label_el and val_el):
                    continue
                label = label_el.get_text(strip=True).lower()
                val   = val_el.get_text(strip=True)
                for k in keys:
                    if k in label:
                        return val
            return None

        m = re.search(r"/cars?/([^/?#]+)", url)
        external_id = m.group(1) if m else url

        h1 = soup.select_one("h1")
        title_text = h1.get_text(strip=True) if h1 else ld.get("name", "")
        year_m = re.search(r"\b(19|20)\d{2}\b", title_text)

        offers = ld.get("offers") or {}
        price_raw = offers.get("price") or spec(["price", "asking price"])
        if not price_raw:
            el = soup.select_one("[class*='price'], .car-price")
            if el:
                price_raw = el.get_text(strip=True)
        price, price_max, _ = self.clean_price(price_raw)

        images = []
        if ld.get("image"):
            imgs = ld["image"] if isinstance(ld["image"], list) else [ld["image"]]
            images = self.filter_images([str(i) for i in imgs])
        if not images:
            images = self.filter_images([
                t.get("src") or t.get("data-src")
                for t in soup.select(".gallery img, [class*='carousel'] img, [class*='photo'] img")
            ])

        seller_section = soup.select_one("[class*='dealer'], [class*='seller'], [class*='contact']")
        seller_type = "dealer"
        if seller_section:
            st = seller_section.get_text().lower()
            cls = " ".join(seller_section.get("class", []))
            if "private" in st and "dealer" not in st and "dealer" not in cls:
                seller_type = "private"

        desc_el = soup.select_one("[class*='description'], .car-desc, #description")

        return {
            "external_id": external_id,
            "make": ld.get("brand", {}).get("name") or spec(["make", "brand"]),
            "model": ld.get("model") or spec(["model"]),
            "variant": spec(["trim", "variant", "version", "edition"]),
            "year": (int(year_m.group()) if year_m else None) or self.clean_int(spec(["year", "model year"])),
            "body_type": ld.get("bodyType") or spec(["body type", "body style"]),
            "condition": "new",
            "mileage_km": 0,
            "fuel_type": ld.get("fuelType") or spec(["fuel type", "fuel"]),
            "transmission": spec(["transmission", "gearbox"]),
            "engine_cc": self.clean_int(spec(["engine size", "engine cc", "displacement", "cc"])),
            "cylinders": self.clean_int(spec(["cylinders", "no. of cylinders"])),
            "color": ld.get("color") or spec(["color", "colour", "exterior color"]),
            "doors": self.clean_int(spec(["doors", "no. of doors"])),
            "price_aed": price,
            "price_aed_max": price_max,
            "price_negotiable": False,
            "emirate": spec(["emirate", "city"]) or "Dubai",
            "area": spec(["area", "location", "neighbourhood"]),
            "seller_type": seller_type,
            "seller_name": (ld.get("seller") or {}).get("name") or spec(["dealer name", "seller name"]),
            "seller_phone": spec(["phone", "contact number", "tel"]),
            "image_urls": images,
            "description": desc_el.get_text(separator=" ", strip=True) if desc_el else ld.get("description"),
            "specs": {},
        }

    def _extract_jsonld(self, soup) -> dict:
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(tag.string or "")
                if isinstance(data, dict) and data.get("@type") in ("Car", "Vehicle", "Product"):
                    return data
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("@type") in ("Car", "Vehicle", "Product"):
                            return item
            except Exception:
                pass
        return {}