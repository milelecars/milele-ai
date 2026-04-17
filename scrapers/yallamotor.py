"""
scrapers/yallamotor.py — YallaMotor UAE new cars (direct HTTP)

Scrapes: https://uae.yallamotor.com/new-cars
Uses JSON-LD structured data, falls back to HTML spec parsing.
No Apify — direct HTTP with browser profile emulation.
"""

import json
import logging
import re
import time
from typing import Generator, Optional

from utils.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class YallaMotorScraper(BaseScraper):
    SOURCE = "yallamotor"
    BASE_URL = "https://uae.yallamotor.com"
    APIFY_ACTOR_ID = None  # Using direct HTTP

    # New cars only
    SEARCH_URL = "https://uae.yallamotor.com/new-cars?page={page}&sort_by=date_desc"

    def listing_urls(self) -> Generator[str, None, None]:
        page = 1
        while True:
            html = self.fetch(self.SEARCH_URL.format(page=page))
            if not html:
                break
            soup = self.soup(html)
            cards = soup.select("a[href*='/new-cars/']")
            links = list(dict.fromkeys([
                c["href"] for c in cards
                if re.search(r"/new-cars/[^?#]", c.get("href", ""))
                and "/new-cars?" not in c.get("href", "")
                and c.get("href", "") != "/new-cars"
                and c.get("href", "") != "/new-cars/"
            ]))

            if not links:
                logger.warning(f"[{self.SOURCE}] No listings on page {page} — stopping")
                break

            for href in links:
                yield href if href.startswith("http") else self.BASE_URL + href

            if not soup.select_one("a[rel='next'], [aria-label='Next']"):
                break
            page += 1
            time.sleep(4)

    def parse_listing(self, url: str, html: str) -> Optional[dict]:
        soup = self.soup(html)
        ld = self._extract_jsonld(soup)

        def spec(keys: list) -> Optional[str]:
            for row in soup.select(".car-info li, .spec-item, .car-spec-item, table tr, [class*='spec'] li"):
                label_el = row.select_one("span.label, .spec-name, td:first-child, dt, strong")
                val_el   = row.select_one("span.value, .spec-value, td:last-child, dd")
                if not (label_el and val_el):
                    continue
                label = label_el.get_text(strip=True).lower()
                val   = val_el.get_text(strip=True)
                for k in keys:
                    if label == k:
                        return val
            # fuzzy
            for row in soup.select(".car-info li, .spec-item, table tr"):
                t = row.get_text(" ", strip=True).lower()
                val_el = row.select_one("span:last-child, td:last-child, dd, strong")
                for k in keys:
                    if k in t and val_el:
                        return val_el.get_text(strip=True)
            return None

        m = re.search(r"/new-cars/([^/?#]+)", url)
        raw_id = m.group(1) if m else url
        external_id = raw_id.split("-")[-1] if re.search(r"\d", raw_id) else raw_id

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
                for t in soup.select(".car-gallery img, .slider img, [class*='photo'] img")
            ])

        seller_type = "dealer"
        seller_ld = ld.get("seller") or {}
        if seller_ld.get("@type") == "AutoDealer":
            seller_type = "dealer"

        desc_el = soup.select_one("[class*='description'], .car-description, #about")

        return {
            "external_id": external_id,
            "make": ld.get("brand", {}).get("name") or spec(["make", "brand"]),
            "model": ld.get("model") or spec(["model"]),
            "variant": spec(["trim", "variant", "edition", "version"]),
            "year": (int(year_m.group()) if year_m else None) or self.clean_int(spec(["year", "model year"])),
            "body_type": ld.get("bodyType") or spec(["body type", "body style"]),
            "condition": "new",
            "mileage_km": 0,
            "fuel_type": ld.get("fuelType") or spec(["fuel type", "fuel"]),
            "transmission": spec(["transmission", "gearbox"]),
            "engine_cc": self.clean_int(spec(["engine size", "displacement", "cc", "engine cc"])),
            "cylinders": self.clean_int(spec(["cylinders", "no. of cylinders"])),
            "color": ld.get("color") or spec(["color", "colour", "exterior color"]),
            "doors": self.clean_int(spec(["doors", "no. of doors"])),
            "price_aed": price,
            "price_aed_max": price_max,
            "price_negotiable": False,
            "emirate": spec(["emirate", "city", "location"]) or "Dubai",
            "area": spec(["area", "region", "district"]),
            "seller_type": seller_type,
            "seller_name": seller_ld.get("name") or spec(["dealer name", "seller"]),
            "seller_phone": spec(["phone", "contact", "tel"]),
            "image_urls": images,
            "description": desc_el.get_text(separator=" ", strip=True) if desc_el else ld.get("description"),
            "specs": {},
        }

    def _extract_jsonld(self, soup) -> dict:
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(tag.string or "")
                if isinstance(data, dict) and data.get("@type") in ("Car", "Vehicle"):
                    return data
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("@type") in ("Car", "Vehicle"):
                            return item
            except Exception:
                pass
        return {}