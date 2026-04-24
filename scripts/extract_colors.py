"""
scripts/extract_colors.py — fill the exterior `color` column via an LLM vision call.

Uses Groq's free-tier Llama 3.2 Vision by default. Groq's free tier is far
more generous than Gemini's (~14,400 RPD vs. 20 RPD) and no card required.

Iterates rows with `color IS NULL`, passes the first listing image URL to
the model, parses a single-word answer, writes it back.

Usage:
    python -m scripts.extract_colors            # default 200 rows
    python -m scripts.extract_colors 1500       # up to 1500 rows this run

Env vars (from .env or the shell):
    GROQ_API_KEY           required (grab one free at console.groq.com/keys)
    GROQ_MODEL             optional (default: llama-3.2-11b-vision-preview)
    SUPABASE_URL           required
    SUPABASE_SERVICE_KEY   required

Halts early if 10 calls fail in a row (likely rate-limit / quota hit).
"""

import base64
import io
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
from groq import Groq
from PIL import Image

from utils.db import _safe_exec, get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("extract_colors")

GROQ_MODEL = os.environ.get(
    "GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct"
)

PROMPT = (
    "Exterior body paint color of this car. "
    "Ignore background, windows, wheels. "
    "Two-tone → dominant. Silver = metallic; grey = matte. "
    "Reply one lowercase word, from: "
    "white, black, silver, grey, blue, red, green, yellow, orange, brown, "
    "beige, gold, bronze, purple, maroon, champagne, pearl, burgundy, navy, tan. "
    "Unclear → 'unknown'."
)

# Accept these specific colour words; normalise gray→grey.
_ACCEPTABLE = {
    "white", "black", "silver", "blue", "red", "grey", "green",
    "yellow", "orange", "brown", "beige", "gold", "bronze",
    "purple", "maroon", "champagne", "pearl", "burgundy",
    "navy", "turquoise", "teal", "cyan", "pink", "tan",
}

DEFAULT_LIMIT = 200
MAX_PER_RUN = 2000
CONSECUTIVE_FAIL_LIMIT = 10


def _pick_first_image(urls) -> Optional[str]:
    if not urls:
        return None
    for u in urls:
        if isinstance(u, str) and u.startswith("http"):
            return u
    return None


def _thumb_data_url(url: str, max_side: int = 400, quality: int = 75) -> Optional[str]:
    """Download, shrink to ~400px on longest edge, re-encode as JPEG, return a
    base64 data-URL. Drops vision-model token cost from ~7K/call to ~1K/call
    (Groq counts image tokens by pixel area)."""
    try:
        r = _requests.get(url, timeout=15)
        r.raise_for_status()
        img = Image.open(io.BytesIO(r.content))
        img.thumbnail((max_side, max_side), Image.LANCZOS)
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="JPEG", quality=quality)
        b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        return f"data:image/jpeg;base64,{b64}"
    except Exception as e:
        logger.warning(f"  image download/resize failed ({e})")
        return None


def _extract_color(client: Groq, image_url: str) -> Optional[str]:
    """Resize the image locally (keeps vision-token cost low), then ask Groq's
    vision model for the car's exterior color."""
    data_url = _thumb_data_url(image_url)
    if not data_url:
        return None
    try:
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": PROMPT},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
            temperature=0,
            max_tokens=8,
        )
        text = (resp.choices[0].message.content or "").strip().lower()
    except Exception as e:
        logger.warning(f"  groq failed ({type(e).__name__}: {e})")
        return None

    if not text:
        return None
    first = text.replace(".", "").replace(",", "").replace("*", "").split()[0]
    if first == "unknown":
        return None
    if first == "gray":
        return "grey"
    if first in _ACCEPTABLE:
        return first
    if first.isalpha() and 2 <= len(first) <= 15:
        return first
    return None


def main():
    try:
        limit = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_LIMIT
    except ValueError:
        limit = DEFAULT_LIMIT
    limit = min(limit, MAX_PER_RUN)

    groq_key = os.environ.get("GROQ_API_KEY")
    sb_url = os.environ.get("SUPABASE_URL")
    sb_key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not (groq_key and sb_url and sb_key):
        logger.error("GROQ_API_KEY, SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
        sys.exit(1)

    groq = Groq(api_key=groq_key)
    client = get_client(sb_url, sb_key)

    rows = _safe_exec(
        client.table("car_listings")
        .select("id, external_id, make, model, image_urls, url")
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
        f"Processing up to {len(rows)} rows via Groq ({GROQ_MODEL}). "
        f"Throttling ~30 RPM (~2 s per call)."
    )

    ok = 0
    fail = 0
    consecutive_fail = 0
    for i, row in enumerate(rows, 1):
        img_url = _pick_first_image(row.get("image_urls"))
        listing_url = row.get("url") or "(no url)"
        tag = f"{row.get('make')} {row.get('model')} ({row['external_id'][:8]})"

        if not img_url:
            logger.info(f"[{i}/{len(rows)}] {tag}: no image → skip | {listing_url}")
            fail += 1
            consecutive_fail += 1
        else:
            color = _extract_color(groq, img_url)
            if color:
                try:
                    _safe_exec(
                        client.table("car_listings")
                        .update({"color": color})
                        .eq("id", row["id"])
                    )
                    ok += 1
                    consecutive_fail = 0
                    logger.info(
                        f"[{i}/{len(rows)}] {tag} → {color} | {listing_url}"
                    )
                except Exception as e:
                    logger.warning(
                        f"[{i}/{len(rows)}] {tag}: DB write failed ({e}) | {listing_url}"
                    )
                    fail += 1
                    consecutive_fail += 1
            else:
                logger.info(
                    f"[{i}/{len(rows)}] {tag}: unable to determine | {listing_url}"
                )
                fail += 1
                consecutive_fail += 1

        if consecutive_fail >= CONSECUTIVE_FAIL_LIMIT:
            logger.error(
                f"{consecutive_fail} consecutive failures — likely rate-limited. "
                f"Stopping after ok={ok} fail={fail}."
            )
            break

        # Groq free tier is ~30 RPM → ~2 s gap with jitter.
        time.sleep(random.uniform(1.8, 2.6))

    logger.info(f"Done. ok={ok} fail={fail} attempted={ok + fail}")


if __name__ == "__main__":
    main()
