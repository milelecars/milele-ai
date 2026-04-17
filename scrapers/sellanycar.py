"""
scrapers/sellanycar.py — SellAnyCar UAE (sellanycar.com)

NOTE: SellAnyCar only has used cars — no new car inventory.

SellAnyCar is a fixed-price dealer (never negotiable).
Next.js app — uses __NEXT_DATA__ JSON-LD hybrid.
"""

import json
import logging
import re
import time
from typing import Generator, Optional

from utils.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class SellAnyCarScraper(BaseScraper):
    SOURCE = "sellanycar"
    BASE_URL = "https://uae.sellanycar.com"
    APIFY_ACTOR_ID = None

    SEARCH_URL = "https://uae.sellanycar.com/buy-used-cars?page={page}"

    def listing_urls(self) -> Generator[str, None, None]:
        page = 1
        while True:
            html = self.fetch(self.SEARCH_URL.format(page=page))
            if not html:
                break
            soup = self.soup(html)
            # Try Next.js data first
            nd = self._next_data(soup)
            if nd:
                cars = (
                    nd.get("props", {}).get("pageProps", {}).get("cars")
                    or nd.get("props", {}).get("pageProps", {}).get("listings")
                    or nd.get("props", {}).get("pageProps", {}).get("vehicles")
                    or []
                )
                if not cars:
                    break
                for car in cars:
                    slug = car.get("slug") or car.get("id")
                    if slug:
                        yield f"{self.BASE_URL}/buy-used-cars/{slug}"
                pagination = nd.get("props", {}).get("pageProps", {}).get("pagination") or {}
                if page >= (pagination.get("totalPages") or pagination.get("last_page") or 1):
                    break
            else:
                cards = soup.select(
                    "a[href*='/buy-used-cars/'], a[href*='/car/'], a[href*='/listing/'], a[href*='/vehicle/'], a[href*='/used-car/']"
                )
                links = list(dict.fromkeys([
                    c["href"] for c in cards
                    if re.search(r"/(buy-used-cars|car|listing|vehicle|used-car)/[^?#]", c.get("href", ""))
                    and not c.get("href", "").endswith("/buy-used-cars")
                    and "?" not in c.get("href", "").split("/")[-1]
                ]))
                if not links:
                    break
                for href in links:
                    yield href if href.startswith("http") else self.BASE_URL + href
                if not soup.select_one("a[rel='next'], .pagination .next, [aria-label='Next page']"):
                    break
            page += 1
            time.sleep(4)

    def parse_listing(self, url: str, html: str) -> Optional[dict]:
        soup = self.soup(html)
        nd = self._next_data(soup)
        car = {}
        if nd:
            car = (
                nd.get("props", {}).get("pageProps", {}).get("car")
                or nd.get("props", {}).get("pageProps", {}).get("vehicle")
                or nd.get("props", {}).get("pageProps", {}).get("listing")
                or {}
            )

        # JSON-LD fallback
        ld = {}
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict) and data.get("@type") in ("Car", "Vehicle", "Product"):
                    ld = data
                    break
            except Exception:
                pass

        def nv(*keys):
            for k in keys:
                if car.get(k) is not None:
                    return car[k]
            return None

        def spec(keys: list) -> Optional[str]:
            for row in soup.select(".car-specs li, .spec-row, .details-item, table tr, [class*='spec'] li"):
                label_el = row.select_one("span:first-child, td:first-child, dt, .label, strong")
                val_el   = row.select_one("span:last-child, td:last-child, dd, .value")
                if not (label_el and val_el):
                    continue
                label = label_el.get_text(strip=True).lower()
                val   = val_el.get_text(strip=True)
                for k in keys:
                    if label == k:
                        return val
            # fuzzy
            for row in soup.select(".car-specs li, table tr"):
                t = row.get_text(" ", strip=True).lower()
                val_el = row.select_one("span:last-child, td:last-child, dd")
                for k in keys:
                    if k in t and val_el:
                        return val_el.get_text(strip=True)
            return None

        m = re.search(r"/(buy-used-cars|car|listing|vehicle|used-car)/([^/?#]+)", url)
        external_id = str(nv("id") or (m.group(2) if m else url))

        h1 = soup.select_one("h1")
        title_text = h1.get_text(strip=True) if h1 else nv("title", "name") or ""
        year_m = re.search(r"\b(19|20)\d{2}\b", title_text)

        price_raw = (
            nv("price", "asking_price", "salePrice", "sale_price")
            or ld.get("offers", {}).get("price")
            or spec(["price", "asking price"])
        )
        if not price_raw:
            el = soup.select_one("[class*='price'], .car-price, .listing-price")
            if el:
                price_raw = el.get_text(strip=True)
        price, price_max, _ = self.clean_price(price_raw)

        images = nv("images", "photos", "media") or []
        if images and isinstance(images[0], dict):
            images = [i.get("url") or i.get("src") or i.get("original", "") for i in images]
        if not images:
            images = [
                t.get("src") or t.get("data-src")
                for t in soup.select(".gallery img, .car-image img, [class*='photo'] img")
            ]
        images = self.filter_images(images)

        from utils.db import SPECS_WHITELIST
        whitelisted_specs = {
            k.lower().replace(" ", "_"): v
            for k, v in car.items()
            if k.lower().replace(" ", "_") in SPECS_WHITELIST
        }

        desc_el = soup.select_one("[class*='description'], .car-about, #description")

        return {
            "external_id": external_id,
            "make": nv("make", "brand") or ld.get("brand", {}).get("name") or spec(["make", "brand"]),
            "model": nv("model") or ld.get("model") or spec(["model"]),
            "variant": nv("variant", "trim") or spec(["trim", "variant"]),
            "year": nv("year", "modelYear", "model_year") or (int(year_m.group()) if year_m else None),
            "body_type": nv("bodyType", "body_type") or spec(["body type"]),
            "condition": "used",
            "mileage_km": self.clean_int(str(nv("mileage", "km", "odometer") or spec(["mileage", "km"]) or "")),
            "fuel_type": nv("fuelType", "fuel_type", "fuel") or spec(["fuel type", "fuel"]),
            "transmission": nv("transmission") or spec(["transmission", "gearbox"]),
            "engine_cc": self.clean_int(str(nv("engineSize", "engine_cc") or spec(["engine size", "cc"]) or "")),
            "cylinders": nv("cylinders") or self.clean_int(str(spec(["cylinders"]) or "")),
            "color": nv("color", "colour") or ld.get("color") or spec(["color", "colour"]),
            "doors": nv("doors") or self.clean_int(str(spec(["doors"]) or "")),
            "price_aed": price,
            "price_aed_max": price_max,
            "price_negotiable": False,  # SellAnyCar is always fixed price
            "emirate": nv("emirate", "city") or spec(["emirate", "city"]) or "Dubai",
            "area": nv("area", "location") or spec(["area", "location"]),
            "seller_type": "dealer",  # SellAnyCar is always dealer
            "seller_name": "SellAnyCar",
            "seller_phone": None,
            "image_urls": images,
            "description": desc_el.get_text(separator=" ", strip=True) if desc_el else nv("description"),
            "specs": whitelisted_specs,
        }

    def _next_data(self, soup) -> Optional[dict]:
        tag = soup.find("script", id="__NEXT_DATA__")
        if tag:
            try:
                return json.loads(tag.string)
            except Exception:
                pass
        return None