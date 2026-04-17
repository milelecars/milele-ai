"""
tests/test_suite.py — Full unit test suite

Run:
    pip install pytest
    pytest tests/test_suite.py -v

No network calls, no Supabase connection required.
All tests use synthetic HTML / mock data.
"""

import json
import sys
import os
import pytest

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.db import (
    compute_hash,
    _sanitise,
    _diff,
    _chunks,
    SPECS_WHITELIST,
    SOFT_DELETE_SAFETY_THRESHOLD,
)
from utils.base_scraper import BaseScraper, ScraperHTTPError, random_ua, _BROWSER_PROFILES, _build_headers, _random_profile


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

class _TestScraper(BaseScraper):
    """Minimal concrete scraper for testing base class methods."""
    SOURCE = "test"
    BASE_URL = "http://test.example.com"

    def listing_urls(self):
        return iter([])

    def parse_listing(self, url, html):
        return None


@pytest.fixture
def scraper():
    return _TestScraper()


def _listing(**overrides):
    base = {
        "price_aed": 45000.0,
        "price_aed_max": None,
        "mileage_km": 50000,
        "description": "Good clean car",
        "seller_phone": "+971501234567",
        "image_urls": ["https://cdn.example.com/1.jpg", "https://cdn.example.com/2.jpg"],
        "is_active": True,
    }
    base.update(overrides)
    return base


# ─────────────────────────────────────────────────────────────────────────────
# compute_hash
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeHash:
    def test_same_input_same_hash(self):
        l = _listing()
        assert compute_hash(l) == compute_hash(dict(l))

    def test_price_change_changes_hash(self):
        assert compute_hash(_listing(price_aed=45000)) != compute_hash(_listing(price_aed=44000))

    def test_mileage_change_changes_hash(self):
        assert compute_hash(_listing(mileage_km=50000)) != compute_hash(_listing(mileage_km=51000))

    def test_description_change_changes_hash(self):
        assert compute_hash(_listing(description="Good")) != compute_hash(_listing(description="Bad"))

    def test_image_reorder_does_not_change_hash(self):
        l1 = _listing(image_urls=["https://a.com/1.jpg", "https://a.com/2.jpg"])
        l2 = _listing(image_urls=["https://a.com/2.jpg", "https://a.com/1.jpg"])
        assert compute_hash(l1) == compute_hash(l2)

    def test_none_price_vs_value(self):
        assert compute_hash(_listing(price_aed=None)) != compute_hash(_listing(price_aed=45000.0))

    def test_returns_32_char_hex(self):
        h = compute_hash(_listing())
        assert len(h) == 32
        assert all(c in "0123456789abcdef" for c in h)


# ─────────────────────────────────────────────────────────────────────────────
# _sanitise
# ─────────────────────────────────────────────────────────────────────────────

class TestSanitise:
    def _raw(self, **overrides):
        base = {
            "source": "dubizzle",
            "external_id": "12345",
            "url": "https://dubai.dubizzle.com/motors/used-cars/12345/",
            "content_hash": "abc123",
            "price_aed": "45,000",
            "price_aed_max": None,
            "image_urls": [
                "https://cdn.site.com/a.jpg",
                "data:image/gif;base64,R0lGOD",
                "/relative/path.jpg",
                "https://cdn.site.com/b.jpg",
            ],
            "specs": {
                "regional_specs": "GCC",
                "sunroof": "Yes",
                "massive_blob": "x" * 10000,
                "unknown_internal": "val",
            },
        }
        base.update(overrides)
        return base

    def test_does_not_mutate_original(self):
        raw = self._raw()
        original_price = raw["price_aed"]
        original_imgs = list(raw["image_urls"])
        _sanitise(raw)
        assert raw["price_aed"] == original_price
        assert raw["image_urls"] == original_imgs

    def test_price_cast_to_float(self):
        s = _sanitise(self._raw(price_aed="45,000"))
        assert s["price_aed"] == 45000.0

    def test_price_none_stays_none(self):
        s = _sanitise(self._raw(price_aed=None))
        assert s["price_aed"] is None

    def test_image_filter_removes_non_http(self):
        s = _sanitise(self._raw())
        assert s["image_urls"] == ["https://cdn.site.com/a.jpg", "https://cdn.site.com/b.jpg"]

    def test_image_filter_caps_at_20(self):
        raw = self._raw(image_urls=[f"https://x.com/{i}.jpg" for i in range(30)])
        s = _sanitise(raw)
        assert len(s["image_urls"]) == 20

    def test_specs_whitelist_applied(self):
        s = _sanitise(self._raw())
        assert "regional_specs" in s["specs"]
        assert "sunroof" in s["specs"]
        assert "massive_blob" not in s["specs"]
        assert "unknown_internal" not in s["specs"]

    def test_specs_non_dict_becomes_empty(self):
        s = _sanitise(self._raw(specs="not a dict"))
        assert s["specs"] == {}

    def test_specs_with_non_serialisable_values_becomes_empty(self):
        import datetime
        s = _sanitise(self._raw(specs={"regional_specs": datetime.datetime.now()}))
        # datetime is not JSON serialisable — entire specs should be cleared
        assert s["specs"] == {}


# ─────────────────────────────────────────────────────────────────────────────
# _diff
# ─────────────────────────────────────────────────────────────────────────────

class TestDiff:
    def test_detects_price_change(self):
        d = _diff({"price_aed": 50000}, {"price_aed": 45000})
        assert "price_aed" in d
        assert d["price_aed"] == {"old": 50000, "new": 45000}

    def test_no_diff_on_equal(self):
        old = {"price_aed": 45000, "mileage_km": 30000, "color": "White"}
        assert _diff(old, dict(old)) == {}

    def test_multiple_changes(self):
        old = {"price_aed": 50000, "mileage_km": 30000, "color": "White"}
        new = {"price_aed": 45000, "mileage_km": 35000, "color": "White"}
        d = _diff(old, new)
        assert set(d.keys()) == {"price_aed", "mileage_km"}

    def test_no_false_positives_on_unchanged(self):
        old = {"price_aed": 45000, "emirate": "Dubai", "color": "Silver"}
        new = {"price_aed": 45000, "emirate": "Dubai", "color": "Silver"}
        assert _diff(old, new) == {}

    def test_none_to_value(self):
        d = _diff({"price_aed": None}, {"price_aed": 45000.0})
        assert "price_aed" in d

    def test_ignores_non_watched_fields(self):
        d = _diff({"make": "Toyota"}, {"make": "Honda"})
        assert d == {}  # 'make' is not in WATCH list


# ─────────────────────────────────────────────────────────────────────────────
# _chunks
# ─────────────────────────────────────────────────────────────────────────────

class TestChunks:
    def test_even_split(self):
        chunks = list(_chunks(list(range(1000)), 500))
        assert len(chunks) == 2
        assert all(len(c) == 500 for c in chunks)

    def test_uneven_split(self):
        chunks = list(_chunks(list(range(1250)), 500))
        assert len(chunks) == 3
        assert len(chunks[2]) == 250

    def test_preserves_all_items(self):
        data = list(range(777))
        flat = [x for c in _chunks(data, 100) for x in c]
        assert flat == data

    def test_empty_list(self):
        assert list(_chunks([], 100)) == []

    def test_chunk_larger_than_list(self):
        chunks = list(_chunks([1, 2, 3], 100))
        assert chunks == [[1, 2, 3]]


# ─────────────────────────────────────────────────────────────────────────────
# BaseScraper.clean_price
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanPrice:
    def test_simple_value(self, scraper):
        assert scraper.clean_price("45000") == (45000.0, None, False)

    def test_aed_prefix(self, scraper):
        assert scraper.clean_price("AED 45,000") == (45000.0, None, False)

    def test_range_dash(self, scraper):
        price, max_, is_range = scraper.clean_price("AED 45,000 - 50,000")
        assert price == 45000.0
        assert max_ == 50000.0
        assert is_range is True

    def test_range_en_dash(self, scraper):
        price, max_, is_range = scraper.clean_price("45,000–50,000")
        assert is_range is True
        assert price == 45000.0

    def test_range_to_keyword(self, scraper):
        price, max_, is_range = scraper.clean_price("45000 to 50000")
        assert is_range is True

    def test_none_input(self, scraper):
        assert scraper.clean_price(None) == (None, None, False)

    def test_empty_string(self, scraper):
        assert scraper.clean_price("") == (None, None, False)

    def test_non_numeric(self, scraper):
        assert scraper.clean_price("Call for price") == (None, None, False)

    def test_integer_input(self, scraper):
        assert scraper.clean_price(45000) == (45000.0, None, False)

    def test_large_price(self, scraper):
        price, _, _ = scraper.clean_price("AED 1,250,000")
        assert price == 1250000.0


# ─────────────────────────────────────────────────────────────────────────────
# BaseScraper.clean_int
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanInt:
    def test_plain_number(self, scraper):
        assert scraper.clean_int("45000") == 45000

    def test_with_commas(self, scraper):
        assert scraper.clean_int("45,000") == 45000

    def test_with_unit(self, scraper):
        assert scraper.clean_int("45,000 km") == 45000

    def test_na(self, scraper):
        assert scraper.clean_int("N/A") is None

    def test_none(self, scraper):
        assert scraper.clean_int(None) is None

    def test_zero(self, scraper):
        assert scraper.clean_int(0) == 0

    def test_integer_input(self, scraper):
        assert scraper.clean_int(50000) == 50000

    def test_new_car_zero(self, scraper):
        assert scraper.clean_int("0") == 0


# ─────────────────────────────────────────────────────────────────────────────
# BaseScraper.filter_images
# ─────────────────────────────────────────────────────────────────────────────

class TestFilterImages:
    def test_keeps_http(self, scraper):
        assert "http://x.com/a.jpg" in scraper.filter_images(["http://x.com/a.jpg"])

    def test_keeps_https(self, scraper):
        assert "https://x.com/a.jpg" in scraper.filter_images(["https://x.com/a.jpg"])

    def test_strips_data_uri(self, scraper):
        assert scraper.filter_images(["data:image/gif;base64,abc"]) == []

    def test_strips_relative(self, scraper):
        assert scraper.filter_images(["/relative/path.jpg"]) == []

    def test_strips_none(self, scraper):
        assert scraper.filter_images([None, "https://ok.com/a.jpg"]) == ["https://ok.com/a.jpg"]

    def test_strips_empty_string(self, scraper):
        assert scraper.filter_images(["", "https://ok.com/a.jpg"]) == ["https://ok.com/a.jpg"]

    def test_caps_at_20(self, scraper):
        urls = [f"https://x.com/{i}.jpg" for i in range(30)]
        assert len(scraper.filter_images(urls)) == 20

    def test_empty_input(self, scraper):
        assert scraper.filter_images([]) == []

    def test_none_input(self, scraper):
        assert scraper.filter_images(None) == []


# ─────────────────────────────────────────────────────────────────────────────
# Browser profiles & header system
# ─────────────────────────────────────────────────────────────────────────────

class TestBrowserProfiles:
    """Validates that every browser profile is internally consistent and
    that _build_headers produces correctly ordered, correctly scoped headers."""

    def test_profile_count(self):
        assert len(_BROWSER_PROFILES) >= 8

    def test_all_profiles_have_required_keys(self):
        required = {"name", "ua", "sec-ch-ua", "sec-ch-ua-mobile",
                    "sec-ch-ua-platform", "accept", "accept-language"}
        for p in _BROWSER_PROFILES:
            assert required.issubset(p.keys()), f"{p['name']} missing keys"

    def test_chrome_profiles_have_matching_version_in_ch_ua(self):
        for p in _BROWSER_PROFILES:
            if p.get("sec-ch-ua") and "Chrome/" in p["ua"]:
                v = p["ua"].split("Chrome/")[1].split(".")[0]
                if "Edg/" in p["ua"]:
                    assert f'Microsoft Edge";v="{v}"' in p["sec-ch-ua"],                         f"{p['name']}: Edge version mismatch"
                else:
                    assert f'Google Chrome";v="{v}"' in p["sec-ch-ua"],                         f"{p['name']}: Chrome version mismatch"

    def test_firefox_profiles_omit_sec_ch_ua(self):
        ff_profiles = [p for p in _BROWSER_PROFILES if "firefox" in p["name"]]
        assert len(ff_profiles) >= 2
        for p in ff_profiles:
            assert p["sec-ch-ua"] is None, f"{p['name']} must not have sec-ch-ua"
            assert p["sec-ch-ua-mobile"] is None
            assert p["sec-ch-ua-platform"] is None

    def test_all_uas_start_with_mozilla(self):
        for p in _BROWSER_PROFILES:
            assert p["ua"].startswith("Mozilla/5.0"), f"{p['name']} bad UA"

    def test_profile_names_unique(self):
        names = [p["name"] for p in _BROWSER_PROFILES]
        assert len(names) == len(set(names)), "Duplicate profile names"

    def test_random_profile_returns_dict(self):
        p = _random_profile()
        assert isinstance(p, dict)
        assert "ua" in p and "name" in p

    def test_random_profile_variety(self):
        names = {_random_profile()["name"] for _ in range(80)}
        assert len(names) >= 4, f"Expected >=4 unique profiles, got {len(names)}"


class TestBuildHeaders:
    """Validates _build_headers output for both Chrome and Firefox profiles."""

    def _chrome_profile(self):
        return next(p for p in _BROWSER_PROFILES if "chrome" in p["name"])

    def _firefox_profile(self):
        return next(p for p in _BROWSER_PROFILES if "firefox" in p["name"])

    def test_chrome_cold_start_has_sec_ch_ua(self):
        h = _build_headers(self._chrome_profile())
        assert "sec-ch-ua" in h
        assert "sec-ch-ua-mobile" in h
        assert "sec-ch-ua-platform" in h

    def test_chrome_cold_start_sec_fetch_site_none(self):
        h = _build_headers(self._chrome_profile(), referer=None)
        assert h["sec-fetch-site"] == "none"

    def test_chrome_with_referer_sec_fetch_site_same_origin(self):
        h = _build_headers(self._chrome_profile(),
                            referer="https://dubai.dubizzle.com/motors/used-cars/")
        assert h["sec-fetch-site"] == "same-origin"
        assert h["referer"] == "https://dubai.dubizzle.com/motors/used-cars/"

    def test_chrome_sec_ch_ua_before_user_agent(self):
        """Chrome sends Sec-CH-UA before User-Agent — header order matters."""
        h = _build_headers(self._chrome_profile())
        keys = list(h.keys())
        assert keys.index("sec-ch-ua") < keys.index("user-agent"),             "sec-ch-ua must come before user-agent in Chrome profile"

    def test_firefox_omits_sec_ch_ua(self):
        h = _build_headers(self._firefox_profile())
        assert "sec-ch-ua" not in h
        assert "sec-ch-ua-mobile" not in h
        assert "sec-ch-ua-platform" not in h
        assert "upgrade-insecure-requests" not in h

    def test_all_profiles_have_user_agent(self):
        for p in _BROWSER_PROFILES:
            h = _build_headers(p)
            assert "user-agent" in h

    def test_all_profiles_have_accept_encoding(self):
        for p in _BROWSER_PROFILES:
            h = _build_headers(p)
            assert "accept-encoding" in h
            assert "br" in h["accept-encoding"]

    def test_all_profiles_have_sec_fetch_dest(self):
        for p in _BROWSER_PROFILES:
            h = _build_headers(p)
            assert h.get("sec-fetch-dest") == "document"

    def test_does_not_mutate_profile(self):
        p = self._chrome_profile()
        original_ua = p["ua"]
        _build_headers(p, referer="https://x.com")
        assert p["ua"] == original_ua, "Profile dict must not be mutated"


class TestUARotation:
    """Kept for backward-compatibility — random_ua() is still used in tests."""

    def test_returns_string(self):
        assert isinstance(random_ua(), str)

    def test_minimum_length(self):
        assert len(random_ua()) > 40

    def test_variety(self):
        uas = {random_ua() for _ in range(50)}
        assert len(uas) >= 4, f"Expected >=4 unique UAs, got {len(uas)}"

    def test_contains_mozilla(self):
        assert random_ua().startswith("Mozilla/5.0")


# ─────────────────────────────────────────────────────────────────────────────
# ScraperHTTPError
# ─────────────────────────────────────────────────────────────────────────────

class TestScraperHTTPError:
    def test_status_code_attribute(self):
        err = ScraperHTTPError(429, "https://x.com")
        assert err.status_code == 429

    def test_is_exception(self):
        assert isinstance(ScraperHTTPError(403, "x"), Exception)

    def test_message_includes_status(self):
        err = ScraperHTTPError(503, "https://x.com/page")
        assert "503" in str(err)


# ─────────────────────────────────────────────────────────────────────────────
# SPECS_WHITELIST
# ─────────────────────────────────────────────────────────────────────────────

class TestSpecsWhitelist:
    def test_is_set(self):
        assert isinstance(SPECS_WHITELIST, set)

    def test_non_empty(self):
        assert len(SPECS_WHITELIST) >= 10

    def test_all_lowercase(self):
        assert all(k == k.lower() for k in SPECS_WHITELIST)

    def test_no_whitespace_in_keys(self):
        assert all("_" in k or k.isalpha() for k in SPECS_WHITELIST)

    def test_expected_keys_present(self):
        for key in ("regional_specs", "sunroof", "navigation", "service_history"):
            assert key in SPECS_WHITELIST, f"Expected '{key}' in SPECS_WHITELIST"


# ─────────────────────────────────────────────────────────────────────────────
# Safety threshold constant
# ─────────────────────────────────────────────────────────────────────────────

class TestSafetyThreshold:
    def test_is_float(self):
        assert isinstance(SOFT_DELETE_SAFETY_THRESHOLD, float)

    def test_reasonable_range(self):
        assert 0.10 < SOFT_DELETE_SAFETY_THRESHOLD < 0.60

    def test_currently_35_percent(self):
        assert SOFT_DELETE_SAFETY_THRESHOLD == 0.35


# ─────────────────────────────────────────────────────────────────────────────
# Scraper HTML parse — synthetic HTML smoke tests
# ─────────────────────────────────────────────────────────────────────────────

class TestDubizzleNormalise:
    """Test Dubizzle Apify output normalisation."""

    SAMPLE_ITEM = {
        "id": "12345",
        "url": "https://dubai.dubizzle.com/motors/new-cars/12345/",
        "make": "Toyota",
        "model": "Camry",
        "variant": "SE",
        "year": 2024,
        "price": "AED 85,000",
        "mileage": 0,
        "fuelType": "Petrol",
        "transmission": "Automatic",
        "color": "White",
        "bodyType": "Sedan",
        "emirate": "Dubai",
        "area": "Al Quoz",
        "sellerType": "dealer",
        "sellerName": "Al Futtaim Motors",
        "images": [
            {"url": "https://images.dubizzle.com/car1.jpg"},
            {"url": "https://images.dubizzle.com/car2.jpg"},
        ],
        "description": "Brand new Toyota Camry SE.",
    }

    def test_normalise_returns_dict(self):
        from scrapers.dubizzle import DubizzleScraper
        s = DubizzleScraper()
        result = s._normalise(self.SAMPLE_ITEM)
        assert result is not None
        assert isinstance(result, dict)

    def test_external_id(self):
        from scrapers.dubizzle import DubizzleScraper
        result = DubizzleScraper()._normalise(self.SAMPLE_ITEM)
        assert result["external_id"] == "12345"

    def test_price_parsed(self):
        from scrapers.dubizzle import DubizzleScraper
        result = DubizzleScraper()._normalise(self.SAMPLE_ITEM)
        assert result["price_aed"] == 85000.0

    def test_condition_is_new(self):
        from scrapers.dubizzle import DubizzleScraper
        result = DubizzleScraper()._normalise(self.SAMPLE_ITEM)
        assert result["condition"] == "new"

    def test_images_filtered(self):
        from scrapers.dubizzle import DubizzleScraper
        result = DubizzleScraper()._normalise(self.SAMPLE_ITEM)
        assert all(u.startswith("http") for u in result["image_urls"])
        assert len(result["image_urls"]) == 2

    def test_make_model_year(self):
        from scrapers.dubizzle import DubizzleScraper
        result = DubizzleScraper()._normalise(self.SAMPLE_ITEM)
        assert result["make"] == "Toyota"
        assert result["model"] == "Camry"
        assert result["year"] == 2024

    def test_missing_id_returns_none(self):
        from scrapers.dubizzle import DubizzleScraper
        result = DubizzleScraper()._normalise({})
        assert result is None


class TestJSONLDParse:
    """Smoke test for DubiCars/YallaMotor JSON-LD extraction."""

    JSON_LD_HTML = """
    <html><body>
    <script type="application/ld+json">
    {
      "@type": "Car",
      "brand": {"name": "BMW"},
      "model": "X5",
      "color": "Black",
      "fuelType": "Petrol",
      "mileageFromOdometer": {"value": "32000"},
      "offers": {"price": "280000"},
      "image": ["https://imgs.site.com/bmw1.jpg", "https://imgs.site.com/bmw2.jpg"],
      "description": "Excellent condition X5."
    }
    </script>
    <h1>2021 BMW X5</h1>
    </body></html>
    """

    def test_dubicars_json_ld(self):
        from scrapers.dubicars import DubiCarsScraper
        s = DubiCarsScraper()
        result = s.parse_listing("https://www.dubicars.com/cars/bmw-x5-2021-123", self.JSON_LD_HTML)
        assert result is not None
        assert result["make"] == "BMW"
        assert result["model"] == "X5"
        assert result["price_aed"] == 280000.0
        assert result["condition"] == "new"
        assert result["mileage_km"] == 0
        assert result["color"] == "Black"
        assert len(result["image_urls"]) == 2

    def test_yallamotor_json_ld(self):
        from scrapers.yallamotor import YallaMotorScraper
        s = YallaMotorScraper()
        result = s.parse_listing("https://uae.yallamotor.com/used-cars/bmw/x5/2021-bmw-x5-123", self.JSON_LD_HTML)
        assert result is not None
        assert result["make"] == "BMW"
        assert result["price_aed"] == 280000.0


class TestNextJSParse:
    """Smoke test for CarSwitch / SellAnyCar __NEXT_DATA__ extraction."""

    NEXT_DATA_HTML = """
    <html><body>
    <script id="__NEXT_DATA__" type="application/json">
    {
      "props": {
        "pageProps": {
          "car": {
            "id": "CS-98765",
            "make": "Mercedes-Benz",
            "model": "C200",
            "year": 2020,
            "price": 120000,
            "mileage": 28000,
            "color": "Silver",
            "transmission": "Automatic",
            "fuelType": "Petrol",
            "bodyType": "Sedan",
            "condition": "used",
            "emirate": "Dubai",
            "description": "GCC spec, full service history.",
            "images": [
              {"url": "https://cdn.carswitch.com/img1.jpg"},
              {"url": "https://cdn.carswitch.com/img2.jpg"}
            ]
          }
        }
      }
    }
    </script>
    <h1>2020 Mercedes-Benz C200</h1>
    </body></html>
    """

    def test_carswitch_next_data(self):
        from scrapers.carswitch import CarSwitchScraper
        s = CarSwitchScraper()
        result = s.parse_listing(
            "https://uae.carswitch.com/en/buy-used-cars-in-uae/mercedes-benz-c200-2020-CS-98765",
            self.NEXT_DATA_HTML,
        )
        assert result is not None
        assert result["make"] == "Mercedes-Benz"
        assert result["model"] == "C200"
        assert result["year"] == 2020
        assert result["price_aed"] == 120000.0
        assert result["condition"] == "new"
        assert result["mileage_km"] == 0
        assert result["external_id"] == "CS-98765"
        assert len(result["image_urls"]) == 2

    def test_sellanycar_next_data(self):
        from scrapers.sellanycar import SellAnyCarScraper
        s = SellAnyCarScraper()
        result = s.parse_listing(
            "https://www.sellanycar.com/buy-used-cars/mercedes-benz-c200-CS-98765",
            self.NEXT_DATA_HTML,
        )
        assert result is not None
        assert result["make"] == "Mercedes-Benz"
        assert result["price_aed"] == 120000.0
        assert result["seller_type"] == "dealer"
        assert result["price_negotiable"] is False


# ─────────────────────────────────────────────────────────────────────────────
# main.py registry
# ─────────────────────────────────────────────────────────────────────────────

class TestScrapersRegistry:
    def test_all_five_sources_registered(self):
        from main import SCRAPERS
        expected = {"dubizzle", "dubicars", "yallamotor", "carswitch", "sellanycar"}
        assert set(SCRAPERS.keys()) == expected

    def test_registry_source_matches_class_attribute(self):
        import importlib
        from main import SCRAPERS
        for name, (mod_path, cls_name) in SCRAPERS.items():
            mod = importlib.import_module(mod_path)
            cls = getattr(mod, cls_name)
            assert cls.SOURCE == name

    def test_all_scrapers_instantiable(self):
        import importlib
        from main import SCRAPERS
        for name, (mod_path, cls_name) in SCRAPERS.items():
            mod = importlib.import_module(mod_path)
            cls = getattr(mod, cls_name)
            instance = cls()
            assert instance.SOURCE == name