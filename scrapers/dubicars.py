"""scrapers/dubicars.py — DubiCars (httpx + bs4).

DubiCars sits behind Cloudflare but its search and detail pages are
server-rendered with most fields exposed as JSON-encoded `data-*` attributes
on each `<li class="serp-list-item">`, so a realistic browser-profile UA
(handled by BaseScraper) is enough to fetch the HTML.

Two-phase scrape, mirroring the Dubizzle module:
  1. List phase — paginate the search results, parse the four overlapping
     JSON blobs on each card into a single listing dict, intermediate-upsert
     so list-level data is safe before detail enrichment starts.
  2. Detail phase — for new + updated + backfill listings only, fetch the
     ad page, extract the JSON-LD Car/Product block, the specifications
     section, the full image gallery, and the dealer phone. Incremental
     commits keep enrichment crash-safe.

If Cloudflare starts rejecting httpx requests in production we swap the
fetch layer for Playwright while keeping every parser in this module
unchanged.

Env vars:
    DUBICARS_MAX_PAGES        (default 100) — pagination cap.
    DUBICARS_SEARCH_URL       (debug only) — single search-path override.
    DUBICARS_DETAIL_BATCH_SIZE  honoured via main.py orchestrator.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

from utils.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


_BASE = "https://www.dubicars.com"

# Default search: condition=new, UAE, year >= 2015, price <= 100,000 AED, 30/page.
# Mirrors the Dubizzle target so the two sources cover the same inventory band.
_DEFAULT_SEARCH = (
    "/search?o=&did=&gen=&trg=&c=new&ul=AE&cr=AED&k=&mg=&"
    "yf=2015&yt=&set=bu&pf=&pt=100000&emif=&emit=&kf=&kt=&noi=30"
)

# Card-level JSON blobs on each <li class="serp-list-item">. Listed in
# priority order — entries later in the tuple override earlier ones on
# key conflicts, so clevertap (the most complete/canonical source) wins.
_CARD_DATA_ATTRS = (
    "data-mixpanel-detail",
    "data-sp-item",
    "data-ddetail",
    "data-clevertap-detail",
)

# schema.org URL → human-readable mapping. JSON-LD on the detail page emits
# these as full URLs (e.g. https://schema.org/FrontWheelDriveConfiguration);
# we strip to the last path segment and translate.
_SCHEMA_DRIVE = {
    "FrontWheelDriveConfiguration": "Front Wheel Drive",
    "RearWheelDriveConfiguration":  "Rear Wheel Drive",
    "AllWheelDriveConfiguration":   "All Wheel Drive",
    "FourWheelDriveConfiguration":  "Four Wheel Drive",
}
_SCHEMA_STEERING = {
    "LeftHandDriving":  "Left hand",
    "RightHandDriving": "Right hand",
}
_SCHEMA_CONDITION = {
    "NewCondition":  "new",
    "UsedCondition": "used",
}


class DubiCarsScraper(BaseScraper):
    SOURCE = "dubicars"
    BASE_URL = _BASE
    SUPPORTS_DETAIL = True

    # DubiCars tolerates faster pacing than Dubizzle.
    REQUEST_DELAY = (1.5, 3.5)
    PAGE_DELAY    = (3.0, 6.0)

    def __init__(self):
        super().__init__()
        self._intermediate_upsert_counts: Optional[dict] = None

    # BaseScraper declares these abstract; we override run() entirely (same
    # shape as DubizzleScraper) so they're never called. Stubs satisfy ABC.
    def listing_urls(self) -> Generator[str, None, None]:
        return iter(())

    def parse_listing(self, url: str, html: str) -> Optional[dict]:
        return None

    # ── Run ─────────────────────────────────────────────────────────────────

    def run(self, db_client=None, detail_plan=None, **_) -> list[dict]:
        listings = list(self._scrape_search())
        logger.info(
            f"[{self.SOURCE}] list phase complete: {len(listings)} unique cards"
        )

        # Intermediate upsert — persist list-level data BEFORE the long
        # detail phase so a crash mid-enrichment can't lose the list scrape.
        if db_client and listings:
            try:
                from utils.db import upsert_listings
                self._intermediate_upsert_counts = upsert_listings(db_client, listings)
                logger.info(
                    f"[{self.SOURCE}] intermediate list upsert: "
                    f"{self._intermediate_upsert_counts}"
                )
                # Updated rows go to the detail queue too — same logic as
                # the Dubizzle scraper so price changes get fresh images.
                if detail_plan is not None:
                    updated_ids = (
                        self._intermediate_upsert_counts.get("updated_external_ids")
                        or set()
                    )
                    if updated_ids:
                        detail_plan["backfill_external_ids"] = (
                            (detail_plan.get("backfill_external_ids") or set())
                            | updated_ids
                        )
                        logger.info(
                            f"[{self.SOURCE}] detail queue: +{len(updated_ids)} "
                            f"updated rows queued for image refresh"
                        )
            except Exception as e:
                logger.warning(
                    f"[{self.SOURCE}] intermediate upsert failed ({e}); "
                    f"new listings at risk if detail phase crashes"
                )

        if detail_plan and listings:
            self._enrich_with_detail(listings, detail_plan, db_client=db_client)

        return listings

    # ── List phase ──────────────────────────────────────────────────────────

    def _scrape_search(self) -> Generator[dict, None, None]:
        try:
            max_pages = int(os.environ.get("DUBICARS_MAX_PAGES", "100"))
        except ValueError:
            max_pages = 100

        seen_ids: set[str] = set()
        for page in range(1, max_pages + 1):
            url = self._page_url(page)
            logger.info(f"[{self.SOURCE}] page {page} — {url}")
            html = self.fetch(url)
            if not html:
                logger.warning(
                    f"[{self.SOURCE}] page {page}: fetch failed, stopping pagination"
                )
                break

            cards = list(self._parse_search_page(html))
            new_on_page = 0
            for c in cards:
                ext = c.get("external_id")
                if not ext or ext in seen_ids:
                    continue
                seen_ids.add(ext)
                yield c
                new_on_page += 1

            logger.info(
                f"[{self.SOURCE}] page {page}: {len(cards)} cards, "
                f"+{new_on_page} new (total unique so far: {len(seen_ids)})"
            )
            if not cards:
                logger.info(f"[{self.SOURCE}] page {page} empty — end of results")
                break
            if new_on_page == 0:
                logger.info(
                    f"[{self.SOURCE}] page {page} all-duplicates — end of results"
                )
                break

    def _page_url(self, page: int) -> str:
        override = os.environ.get("DUBICARS_SEARCH_URL")
        base_path = override or _DEFAULT_SEARCH
        suffix = f"&page={page}" if page > 1 else ""
        return urljoin(_BASE, base_path + suffix)

    def _parse_search_page(self, html: str) -> Generator[dict, None, None]:
        soup = self.soup(html)
        for li in soup.select("li.serp-list-item"):
            try:
                listing = self._normalise_card(li)
            except Exception as e:
                logger.warning(f"[{self.SOURCE}] card parse error: {e}")
                continue
            if listing:
                yield listing

    # ── Card-level normalisation ────────────────────────────────────────────

    def _merge_card_data(self, li) -> dict:
        merged: dict = {}
        for attr in _CARD_DATA_ATTRS:
            raw = li.get(attr)
            if not raw:
                continue
            try:
                merged.update(json.loads(raw))
            except (json.JSONDecodeError, TypeError):
                continue
        return merged

    def _normalise_card(self, li) -> Optional[dict]:
        merged = self._merge_card_data(li)

        ext_id = li.get("data-item-id") or merged.get("item_id")
        if not ext_id:
            return None
        ext_id = str(ext_id)

        anchor = li.select_one("a.image-container") or li.select_one("a[href]")
        href = anchor.get("href") if anchor else None
        url = urljoin(_BASE, href) if href else None
        if not url:
            return None

        # Pricing: prefer price_discounted when it's a real lower offer.
        price = merged.get("price_local")
        discount = merged.get("price_discounted")
        try:
            if (discount and price and float(discount) > 0
                    and float(discount) < float(price)):
                price = discount
        except (TypeError, ValueError):
            pass

        condition = merged.get("condition")
        if isinstance(condition, str):
            condition = condition.strip().lower() or None

        listing = {
            "source":           self.SOURCE,
            "external_id":      ext_id,
            "url":              url,
            "make":             merged.get("make") or merged.get("car_make"),
            "model":            merged.get("model") or merged.get("car_model"),
            "trim":             merged.get("trim") or None,
            "year":             self.clean_int(merged.get("year")),
            "body_type":        merged.get("body_type"),
            "condition":        condition,
            "mileage_km":       self.clean_int(
                merged.get("mileage") or li.get("data-item-kilometers")
            ),
            "fuel_type":        merged.get("fuel_type"),
            "transmission":     merged.get("transmission_type"),
            "cylinders":        self.clean_int(merged.get("cylinders")),
            "doors":            self.clean_int(merged.get("doo_count")),
            "color":            merged.get("color_exterior"),
            "interior_color":   merged.get("color_interior"),
            "target_market":    merged.get("regional_specs"),
            "price_aed":        self._to_float(price),
            "price_aed_max":    None,
            "price_negotiable": False,
            "emirate":          merged.get("location"),
            "area":             None,
            "seller_type":      merged.get("seller_type"),
            "seller_name":      merged.get("seller_name"),
            "warranty":         self._to_bool(merged.get("is_warranty")),
            "image_urls":       (
                [merged["image_url"]] if merged.get("image_url") else []
            ),
            "attributes":       self._build_attributes(merged),
        }
        return listing

    def _build_attributes(self, merged: dict) -> dict:
        """Pack every DubiCars-specific field that doesn't map to a column.
        IDs kept for cross-source reconciliation later."""
        attrs: dict = {}

        ID_KEYS = (
            "make_id", "model_id", "body_type_id", "fuel_type_id",
            "transmission_type_id", "drive_type_id",
            "color_exterior_id", "color_interior_id",
            "regional_specs_id", "location_id",
            "steering_side_id", "cylinder_id",
        )
        EXTRA_KEYS = (
            "drive_type", "steering_side", "regional_specs",
            "ad_type", "image_count",
            "is_warranty", "is_inspected", "is_financed",
            "is_360", "is_video",
            "verified", "highly_responsive", "exclusive",
            "item_wheels", "status_export",
            "price_local", "price_discounted", "price_export",
            "seller_id",
        )
        for k in ID_KEYS + EXTRA_KEYS:
            if merged.get(k) is not None:
                attrs[k] = merged[k]
        return attrs

    # ── Detail phase ────────────────────────────────────────────────────────

    def _enrich_with_detail(self, results, plan, db_client=None) -> None:
        known    = plan.get("known_external_ids") or set()
        backfill = plan.get("backfill_external_ids") or set()
        try:
            batch_size = int(plan.get("batch_size") or 100)
        except (TypeError, ValueError):
            batch_size = 100

        new_ids = {r["external_id"] for r in results} - known
        ordered: list[str] = list(new_ids)
        ordered += [b for b in backfill if b not in new_ids]
        ordered = ordered[:batch_size]
        if not ordered:
            logger.info(f"[{self.SOURCE}] detail: nothing to enrich this run")
            return
        n_new = len(new_ids & set(ordered))
        n_back = len(ordered) - n_new
        logger.info(
            f"[{self.SOURCE}] detail: enriching {len(ordered)} "
            f"(new={n_new}, backfill={n_back})"
        )

        by_id = {r["external_id"]: r for r in results}
        ok = 0
        fail = 0
        for i, ext_id in enumerate(ordered, 1):
            listing = by_id.get(ext_id)
            if not listing or not listing.get("url"):
                fail += 1
                continue
            html = self.fetch(listing["url"])
            if not html:
                fail += 1
                logger.warning(
                    f"[{self.SOURCE}] detail [{i}/{len(ordered)}] "
                    f"{ext_id}: fetch failed"
                )
                continue
            try:
                detail = self._parse_detail_page(html)
            except Exception as e:
                fail += 1
                logger.warning(
                    f"[{self.SOURCE}] detail [{i}/{len(ordered)}] "
                    f"{ext_id}: parse error: {e}"
                )
                continue

            self._merge_detail(listing, detail)
            listing["detail_scraped_at"] = datetime.now(timezone.utc).isoformat()
            ok += 1
            logger.info(
                f"[{self.SOURCE}] detail [{i}/{len(ordered)}] ✓ {ext_id} → "
                f"{len(listing.get('image_urls') or [])} images | {listing['url']}"
            )

            if db_client:
                try:
                    from utils.db import update_detail_fields
                    update_detail_fields(
                        db_client, self.SOURCE, ext_id, listing
                    )
                except Exception as e:
                    logger.warning(
                        f"[{self.SOURCE}] detail commit {ext_id} failed: {e}"
                    )
        logger.info(
            f"[{self.SOURCE}] detail phase done: ok={ok} fail={fail}"
        )

    def _parse_detail_page(self, html: str) -> dict:
        soup = self.soup(html)
        result: dict = {
            "clevertap":   {},
            "jsonld":      {},
            "specs":       {},
            "images":      [],
            "phone":       None,
            "dealer_slug": None,
        }

        # 1. Re-read the clevertap blob — on detail pages it carries
        # price_export / status_export, which the search card omits.
        clev_node = soup.select_one("[data-clevertap-detail]")
        if clev_node:
            try:
                result["clevertap"] = json.loads(
                    clev_node.get("data-clevertap-detail") or "{}"
                )
            except (json.JSONDecodeError, TypeError):
                pass

        # 2. JSON-LD: look for an entry whose @type contains Car/Vehicle/Product.
        # Prefer the entry that has a description or name — that's the listing,
        # not the dealer / breadcrumb / website entry.
        for script in soup.select('script[type="application/ld+json"]'):
            txt = script.string or script.get_text() or ""
            if not txt.strip():
                continue
            try:
                data = json.loads(txt)
            except json.JSONDecodeError:
                continue
            graph = data.get("@graph") if isinstance(data, dict) else None
            entries = graph if isinstance(graph, list) else [data]
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                types = entry.get("@type")
                types = types if isinstance(types, list) else [types]
                if not any(
                    t in ("Car", "Vehicle", "Product")
                    for t in types if isinstance(t, str)
                ):
                    continue
                if entry.get("description") or entry.get("name"):
                    result["jsonld"] = entry
                    break
            if result["jsonld"]:
                break

        # 3. Specs section — label/value list. DubiCars uses several class
        # names across templates; broaden the selector to cover them.
        specs: dict = {}
        spec_rows = soup.select(
            ".specifications li, .vehicle-specifications li, "
            ".specs li, .features-list li, [class*='specification'] li"
        )
        for row in spec_rows:
            label_el = row.find(class_=re.compile("label|key|title", re.I))
            value_el = row.find(class_=re.compile("value|detail", re.I))
            if label_el and value_el:
                label = label_el.get_text(strip=True).lower()
                value = value_el.get_text(strip=True)
                if label and value:
                    specs[label] = value
                continue
            # Fallback: first two non-empty text fragments.
            frags = [t.strip() for t in row.stripped_strings]
            if len(frags) >= 2:
                specs[frags[0].lower()] = frags[1]
        result["specs"] = specs

        # 4. Gallery — only the high-res variant (w_1300x760), deduped.
        seen: set[str] = set()
        for img in soup.select("img[src*='dubicars.com/images/']"):
            src = (img.get("src") or "").strip()
            if not src or "w_1300x760" not in src:
                continue
            base = src.split("?")[0]
            if base in seen:
                continue
            seen.add(base)
            result["images"].append(src)

        # 5. Phone — whatsapp:// or tel: links, digits-only.
        for a in soup.select("a[href^='whatsapp://send'], a[href^='tel:']"):
            digits = re.sub(r"[^\d]", "", a.get("href") or "")
            if len(digits) >= 8:
                result["phone"] = digits
                break

        # 6. Dealer slug from /dealers/{slug}.
        dealer_a = soup.select_one("a[href*='/dealers/']")
        if dealer_a:
            m = re.search(r"/dealers/([^/?#]+)", dealer_a.get("href") or "")
            if m:
                result["dealer_slug"] = m.group(1)

        return result

    def _merge_detail(self, listing: dict, detail: dict) -> None:
        """Mutate `listing` in-place with detail-page enrichments."""
        clev = detail.get("clevertap") or {}
        ld = detail.get("jsonld") or {}
        specs = detail.get("specs") or {}
        attrs = listing.setdefault("attributes", {})

        # Description from JSON-LD — the seller-written ad copy. Search-card
        # has no description at all, so this is purely additive.
        desc = (ld.get("description") or "").strip()
        if desc:
            listing["description"] = desc

        # Drive type — promote schema.org URL to a human label.
        drive = ld.get("driveWheelConfiguration")
        if isinstance(drive, str):
            tail = drive.rstrip("/").split("/")[-1]
            human = _SCHEMA_DRIVE.get(tail)
            if human:
                attrs.setdefault("drive_type", human)

        # Steering position
        steer = ld.get("steeringPosition")
        if isinstance(steer, str):
            tail = steer.rstrip("/").split("/")[-1]
            human = _SCHEMA_STEERING.get(tail)
            if human:
                attrs.setdefault("steering_side", human)

        # Condition fallback — only if the card didn't set it.
        cond = ld.get("itemCondition")
        if isinstance(cond, str) and not listing.get("condition"):
            tail = cond.rstrip("/").split("/")[-1]
            human = _SCHEMA_CONDITION.get(tail)
            if human:
                listing["condition"] = human

        # Specs section → attributes. Horsepower also gets the dedicated
        # horsepower_text column.
        for label, val in specs.items():
            if "engine capacity" in label:
                attrs["engine_capacity_text"] = val
            elif "horsepower" in label:
                attrs["horsepower_text"] = val
                listing["horsepower_text"] = val
            elif "wheel size" in label:
                attrs["wheel_size"] = val
            elif "service history" in label:
                attrs["service_history"] = val
            elif "updated on" in label:
                attrs["specs_updated_on"] = val

        # Detail-only pricing.
        if clev.get("price_export") is not None:
            attrs["price_export_aed"] = clev["price_export"]
        if clev.get("status_export") is not None:
            attrs["export_status"] = clev["status_export"]

        # Phone, dealer slug, dealer name (mirror to dealer_name column).
        if detail.get("phone"):
            listing["seller_phone"] = detail["phone"]
        if detail.get("dealer_slug"):
            attrs["dealer_slug"] = detail["dealer_slug"]
        if listing.get("seller_name") and not listing.get("dealer_name"):
            listing["dealer_name"] = listing["seller_name"]

        # Replace single cover image with the full gallery if we got one.
        if detail.get("images"):
            listing["image_urls"] = detail["images"][:20]

    # ── Helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _to_float(v) -> Optional[float]:
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_bool(v) -> Optional[bool]:
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("true", "1", "yes", "y"):
                return True
            if s in ("false", "0", "no", "n"):
                return False
        return None
