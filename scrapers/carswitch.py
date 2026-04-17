"""
scrapers/carswitch.py — CarSwitch UAE (uae.carswitch.com)

Next.js app — primary data source is __NEXT_DATA__ JSON.
specs JSONB uses whitelist only (no full pageProps blob).
"""

import json
import logging
import os
import re
import time
from typing import Generator, Optional

from utils.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class CarSwitchScraper(BaseScraper):
    SOURCE = "carswitch"
    BASE_URL = "https://www.carswitch.com"
    APIFY_ACTOR_ID = None  # No known public actor — use HTML/Next.js

    SEARCH_URL = "https://www.carswitch.com/en/buy-new-cars-in-uae?page={page}&sort=newest"

    def listing_urls(self) -> Generator[str, None, None]:
        page = 1
        while True:
            html = self.fetch(self.SEARCH_URL.format(page=page))
            if not html:
                break
            soup = self.soup(html)
            nd = self._next_data(soup)
            if nd:
                cars = (
                    nd.get("props", {}).get("pageProps", {}).get("cars")
                    or nd.get("props", {}).get("pageProps", {}).get("listings")
                    or []
                )
                if not cars:
                    break
                for car in cars:
                    slug = car.get("slug") or car.get("id")
                    if slug:
                        yield f"{self.BASE_URL}/en/buy-new-cars-in-uae/{slug}"
                # Check pagination in Next.js data
                pagination = nd.get("props", {}).get("pageProps", {}).get("pagination") or {}
                if page >= (pagination.get("totalPages") or pagination.get("last_page") or 1):
                    break
            else:
                cards = soup.select("a[href*='/buy-new-cars']")
                links = list(dict.fromkeys([
                    c["href"] for c in cards
                    if re.search(r"/buy-new-cars[^?#]+[-\d]", c.get("href", ""))
                ]))
                if not links:
                    break
                for href in links:
                    yield href if href.startswith("http") else self.BASE_URL + href
                if not soup.select_one("a[rel='next'], [aria-label='Next']"):
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
                or nd.get("props", {}).get("pageProps", {}).get("listing")
                or nd.get("props", {}).get("pageProps", {}).get("vehicle")
                or {}
            )

        def nv(*keys):
            for k in keys:
                if car.get(k) is not None:
                    return car[k]
            return None

        m = re.search(r"/buy-new-cars[^?#]*/([^/?#]+)", url)
        external_id = str(nv("id") or (m.group(1) if m else url))

        h1 = soup.select_one("h1")
        title_text = h1.get_text(strip=True) if h1 else nv("title", "name") or ""
        year_m = re.search(r"\b(19|20)\d{2}\b", title_text)

        price_raw = nv("price", "asking_price", "listingPrice", "sale_price")
        price, price_max, _ = self.clean_price(price_raw)

        images = nv("images", "photos", "media") or []
        if images and isinstance(images[0], dict):
            images = [i.get("url") or i.get("src") or i.get("original", "") for i in images]
        images = self.filter_images(images)

        # Whitelist specs from Next.js data
        from utils.db import SPECS_WHITELIST
        whitelisted_specs = {
            k.lower().replace(" ", "_"): v
            for k, v in car.items()
            if k.lower().replace(" ", "_") in SPECS_WHITELIST
        }

        return {
            "external_id": external_id,
            "make": nv("make", "brand", "manufacturer"),
            "model": nv("model"),
            "variant": nv("variant", "trim", "version"),
            "year": nv("year", "modelYear", "model_year") or (int(year_m.group()) if year_m else None),
            "body_type": nv("bodyType", "body_type", "carType", "car_type"),
            "condition": "new",
            "mileage_km": 0,
            "fuel_type": nv("fuelType", "fuel_type", "fuel"),
            "transmission": nv("transmission", "gearbox"),
            "engine_cc": self.clean_int(str(nv("engineSize", "engine_cc", "displacement") or "")),
            "cylinders": nv("cylinders", "noOfCylinders", "no_of_cylinders"),
            "color": nv("color", "colour", "exteriorColor", "exterior_color"),
            "doors": nv("doors", "noOfDoors", "no_of_doors"),
            "price_aed": price,
            "price_aed_max": price_max,
            "price_negotiable": bool(nv("negotiable", "price_negotiable", "is_negotiable")),
            "emirate": nv("emirate", "city", "location") or "Dubai",
            "area": nv("area", "district", "neighbourhood"),
            "seller_type": nv("sellerType", "seller_type") or "dealer",
            "seller_name": nv("sellerName", "dealer", "seller_name", "dealerName"),
            "seller_phone": nv("phone", "contact_phone", "seller_phone"),
            "image_urls": images,
            "description": nv("description", "about"),
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