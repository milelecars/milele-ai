"""
scripts/extract_colors.py — fill the exterior `color` column using Gemini Vision.

Iterates rows with `color IS NULL`, downloads the first listing image, sends it
to `gemini-2.5-flash-lite` with a prompt asking for the body color as a single
lowercase word, writes the answer back to the row.

Throttled to stay inside the Gemini free-tier caps:
  * 15 RPM  → 4+ s gap between calls
  * 1000 RPD → script caps at 950 requests by default; re-run next day

Usage:
    python -m scripts.extract_colors            # default 200 rows
    python -m scripts.extract_colors 500        # up to 500 rows this run

Env vars (from .env or the shell):
    GEMINI_API_KEY         required
    SUPABASE_URL           required
    SUPABASE_SERVICE_KEY   required

Halts early if 10 calls fail in a row (likely rate-limit / quota hit).
"""

import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests as _requests
import google.generativeai as genai

from utils.db import _safe_exec, get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("extract_colors")

MODEL = "gemini-2.5-flash-lite"

PROMPT = (
    "What is the exterior paint color of this car? "
    "Reply with exactly one lowercase word (e.g. 'white', 'black', 'silver', "
    "'blue', 'red', 'grey', 'green', 'yellow', 'orange', 'brown', 'beige', "
    "'gold', 'bronze', 'purple', 'maroon', 'champagne'). "
    "If the car body is two-tone, pick the dominant color. "
    "If no car is visible or the angle is unclear, reply 'unknown'."
)

# Accept words that look like plausible car colors; normalise gray→grey.
_ACCEPTABLE = {
    "white", "black", "silver", "blue", "red", "grey", "green",
    "yellow", "orange", "brown", "beige", "gold", "bronze",
    "purple", "maroon", "champagne", "pearl", "burgundy",
    "navy", "turquoise", "teal", "cyan", "pink", "tan",
}

DEFAULT_LIMIT = 200
MAX_PER_RUN = 950  # leave headroom under the 1000 RPD quota
CONSECUTIVE_FAIL_LIMIT = 10


def _pick_first_image(urls) -> Optional[str]:
    if not urls:
        return None
    for u in urls:
        if isinstance(u, str) and u.startswith("http"):
            return u
    return None


def _fetch_image(url: str):
    try:
        r = _requests.get(url, timeout=15)
        r.raise_for_status()
        mime = (r.headers.get("content-type") or "image/jpeg").split(";")[0]
        if not mime.startswith("image/"):
            return None
        return r.content, mime
    except Exception as e:
        logger.warning(f"  image download failed ({e})")
        return None


def _extract_color(image_bytes: bytes, mime: str) -> Optional[str]:
    try:
        model = genai.GenerativeModel(MODEL)
        resp = model.generate_content(
            [PROMPT, {"mime_type": mime, "data": image_bytes}],
            generation_config={"temperature": 0.1},
        )
        text = (resp.text or "").strip().lower()
    except Exception as e:
        logger.warning(f"  gemini failed ({type(e).__name__}: {e})")
        return None

    if not text:
        return None
    # Strip punctuation, take first word only.
    first = text.replace(".", "").replace(",", "").replace("*", "").split()[0]
    if first == "unknown":
        return None
    if first == "gray":
        return "grey"
    if first in _ACCEPTABLE:
        return first
    # Fallback: accept any short plausible-looking word (2–15 letters, alpha).
    if first.isalpha() and 2 <= len(first) <= 15:
        return first
    return None


def main():
    try:
        limit = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_LIMIT
    except ValueError:
        limit = DEFAULT_LIMIT
    limit = min(limit, MAX_PER_RUN)

    api_key = os.environ.get("GEMINI_API_KEY")
    sb_url = os.environ.get("SUPABASE_URL")
    sb_key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not (api_key and sb_url and sb_key):
        logger.error("GEMINI_API_KEY, SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        sys.exit(1)

    genai.configure(api_key=api_key)
    client = get_client(sb_url, sb_key)

    rows = _safe_exec(
        client.table("car_listings")
        .select("id, external_id, make, model, image_urls")
        .eq("source", "dubizzle")
        .eq("is_active", True)
        .is_("color", "null")
        .order("first_seen_at")
        .limit(limit)
    ).data or []

    if not rows:
        logger.info("Nothing pending — every active row already has a color.")
        return

    logger.info(
        f"Processing up to {len(rows)} rows. Throttling ~15 RPM. "
        f"ETA ~{len(rows) * 5 // 60} min."
    )

    ok = 0
    fail = 0
    consecutive_fail = 0
    for i, row in enumerate(rows, 1):
        url = _pick_first_image(row.get("image_urls"))
        tag = f"{row.get('make')} {row.get('model')} ({row['external_id'][:8]})"

        if not url:
            logger.info(f"[{i}/{len(rows)}] {tag}: no image → skip")
            fail += 1
            consecutive_fail += 1
        else:
            got = _fetch_image(url)
            color = _extract_color(*got) if got else None
            if color:
                try:
                    _safe_exec(
                        client.table("car_listings")
                        .update({"color": color})
                        .eq("id", row["id"])
                    )
                    ok += 1
                    consecutive_fail = 0
                    logger.info(f"[{i}/{len(rows)}] {tag} → {color}")
                except Exception as e:
                    logger.warning(f"[{i}/{len(rows)}] {tag}: DB write failed ({e})")
                    fail += 1
                    consecutive_fail += 1
            else:
                logger.info(f"[{i}/{len(rows)}] {tag}: unable to determine")
                fail += 1
                consecutive_fail += 1

        if consecutive_fail >= CONSECUTIVE_FAIL_LIMIT:
            logger.error(
                f"{consecutive_fail} consecutive failures — likely rate-limited "
                f"or quota exhausted. Stopping after ok={ok} fail={fail}."
            )
            break

        # 15 RPM throttle with jitter → avg ~4.5 s per call.
        time.sleep(random.uniform(4.2, 5.5))

    logger.info(f"Done. ok={ok} fail={fail} attempted={ok + fail}")


if __name__ == "__main__":
    main()
