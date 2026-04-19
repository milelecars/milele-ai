"""
bot.py — Telegram bot for the car_listings DB.

User sends natural-language queries ("white toyota camry 2021+ under 90k in
dubai"). Gemini extracts a structured filter dict. Filters are validated
against a hardcoded whitelist of columns — unknown keys are silently dropped.
The validated filter dict is translated into a Supabase query, results are
formatted and sent back to Telegram.

Designed to run as a long-lived Railway service (HTTP long-polling against
Telegram's getUpdates). Uses the SAME Telegram bot as the scraper's daily
summary notifications — scraper only calls sendMessage (outgoing); this
process is the only consumer of getUpdates (incoming), so no conflict.

Env vars:
    TELEGRAM_BOT_TOKEN       required
    GEMINI_API_KEY           required
    SUPABASE_URL             required
    SUPABASE_SERVICE_KEY     required
    BOT_RESULT_LIMIT         optional, default 10
    BOT_ALLOWED_CHAT_IDS     optional, comma-separated allowlist. If set,
                             messages from other chats are silently ignored.
"""

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / ".env")

import requests as _requests
import google.generativeai as genai

from utils.db import _safe_exec, get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("bot")


# ────────────────────────────────────────────────────────────────────────────
# Filter whitelist. Keys the AI is ALLOWED to emit — everything else dropped.
# ────────────────────────────────────────────────────────────────────────────

TEXT_FIELDS = frozenset({
    "make", "model", "trim", "variant",
    "body_type", "fuel_type", "transmission",
    "color", "interior_color", "target_market",
    "horsepower_text", "engine_capacity_cc_text", "seating_capacity_text",
    "condition", "emirate", "area", "seller_type", "dealer_name",
})
INT_FIELDS = frozenset({"year", "doors", "cylinders"})
BOOL_FIELDS = frozenset({"warranty"})
# range_key → (column, postgrest op)
RANGE_FIELDS = {
    "year_min":    ("year",       "gte"),
    "year_max":    ("year",       "lte"),
    "price_min":   ("price_aed",  "gte"),
    "price_max":   ("price_aed",  "lte"),
    "mileage_max": ("mileage_km", "lte"),
}
ALLOWED_KEYS = TEXT_FIELDS | INT_FIELDS | BOOL_FIELDS | frozenset(RANGE_FIELDS)


# ────────────────────────────────────────────────────────────────────────────
# Gemini — natural language → filter dict
# ────────────────────────────────────────────────────────────────────────────

GEMINI_MODEL = "gemini-2.5-flash-lite"

SYSTEM_PROMPT = f"""
You are a filter extractor for a car marketplace database. Convert the
user's natural-language request into a strict JSON object of filters.

Allowed keys (use NOTHING else — unknown keys must be dropped):
  text (case-insensitive partial match):
      {sorted(TEXT_FIELDS)}
  integers (exact):
      {sorted(INT_FIELDS)}
  numeric ranges:
      {sorted(RANGE_FIELDS.keys())}
  booleans:
      {sorted(BOOL_FIELDS)}

Rules:
  - Output ONLY the JSON object. No prose, no markdown fence.
  - Correct obvious typos ("toyata" → "toyota").
  - "2020+" or "2020 onwards" → year_min=2020
  - "before 2019" → year_max=2018
  - "under 100k" (AED) → price_max=100000; "k" = thousand.
  - "over 50k" → price_min=50000
  - "low mileage" → mileage_max=30000
  - "under 50000 km" → mileage_max=50000
  - Use lowercase for text values ("suv", "petrol", "white", "dubai").
  - "new" / "used" → condition.
  - If a piece of the request doesn't match ANY allowed key, drop it silently.
    Do NOT invent new keys.
  - If no criteria can be extracted at all, return {{}}.

Examples:
  in: "white toyota camry 2021+ under 90k in dubai"
  out: {{"make":"toyota","model":"camry","color":"white","year_min":2021,"price_max":90000,"emirate":"dubai"}}

  in: "red porsche cayenne under 250k low mileage"
  out: {{"make":"porsche","model":"cayenne","color":"red","price_max":250000,"mileage_max":30000}}

  in: "new BMW with warranty from Al Qassim"
  out: {{"make":"bmw","condition":"new","warranty":true,"dealer_name":"al qassim"}}

  in: "show me some cool cars"
  out: {{}}
""".strip()


def extract_filters(user_text: str) -> dict:
    """Call Gemini, return a whitelisted filter dict. Never raises."""
    try:
        model = genai.GenerativeModel(GEMINI_MODEL)
        resp = model.generate_content(
            [SYSTEM_PROMPT, f"in: {user_text}\nout:"],
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.1,
            },
        )
        parsed = json.loads((resp.text or "").strip())
        if not isinstance(parsed, dict):
            return {}
        return {k: v for k, v in parsed.items() if k in ALLOWED_KEYS}
    except Exception as e:
        logger.warning(f"Gemini extraction failed: {e}")
        return {}


# ────────────────────────────────────────────────────────────────────────────
# DB query
# ────────────────────────────────────────────────────────────────────────────

def apply_filters(query, filters: dict):
    """Apply a validated filter dict to a PostgREST query builder."""
    for k, v in filters.items():
        if v is None or v == "":
            continue
        if k in TEXT_FIELDS:
            query = query.ilike(k, f"%{v}%")
        elif k in INT_FIELDS:
            try:
                query = query.eq(k, int(v))
            except (TypeError, ValueError):
                continue
        elif k in BOOL_FIELDS:
            query = query.eq(k, bool(v))
        elif k in RANGE_FIELDS:
            col, op = RANGE_FIELDS[k]
            try:
                n = float(v)
            except (TypeError, ValueError):
                continue
            query = query.gte(col, n) if op == "gte" else query.lte(col, n)
    return query


def search(client, filters: dict, limit: int = 10) -> tuple[list, int]:
    """Return (rows, total_count) for the given filters."""
    base = (
        client.table("car_listings")
        .select(
            "external_id,url,make,model,year,trim,color,body_type,"
            "fuel_type,mileage_km,price_aed,emirate,area,"
            "dealer_name,warranty,image_urls",
            count="exact",
        )
        .eq("is_active", True)
    )
    q = apply_filters(base, filters)
    q = q.order("first_seen_at", desc=True).limit(limit)
    resp = _safe_exec(q)
    rows = resp.data or []
    total = getattr(resp, "count", None)
    if total is None:
        total = len(rows)
    return rows, total


# ────────────────────────────────────────────────────────────────────────────
# Formatting
# ────────────────────────────────────────────────────────────────────────────

def format_listing(r: dict) -> str:
    year = r.get("year") or ""
    make = (r.get("make") or "").title()
    model = r.get("model") or ""
    trim = r.get("trim")
    title = f"{year} {make} {model}".strip() or "Listing"
    if trim:
        title += f" — {trim}"

    lines = [f"🚗 {title}"]
    price = r.get("price_aed")
    if price is not None:
        try:
            lines.append(f"💰 AED {int(float(price)):,}")
        except (TypeError, ValueError):
            pass
    km = r.get("mileage_km")
    if km is not None:
        try:
            km_i = int(km)
            if km_i > 0:
                lines.append(f"🛣 {km_i:,} km")
        except (TypeError, ValueError):
            pass
    loc = ", ".join(x for x in (r.get("area"), r.get("emirate")) if x)
    if loc:
        lines.append(f"📍 {loc}")
    dealer = r.get("dealer_name")
    if dealer:
        lines.append(f"🏢 {dealer}")
    url = r.get("url")
    if url:
        lines.append(url)
    return "\n".join(lines)


def format_reply(filters: dict, rows: list[dict], total: int) -> str:
    if not filters and not rows:
        return (
            "I couldn't extract any filters from that. Try:\n"
            "• white toyota camry 2021+ under 90k in dubai\n"
            "• red porsche cayenne under 250k low mileage\n"
            "• new bmw with warranty"
        )
    if not rows:
        return (
            f"❌ No matches.\n"
            f"Filters: {json.dumps(filters, ensure_ascii=False)}"
        )
    shown = len(rows)
    header = f"🔎 {total} match{'es' if total != 1 else ''} — showing {shown}"
    if filters:
        header += f"\nFilters: {json.dumps(filters, ensure_ascii=False)}"
    return header + "\n\n" + "\n\n".join(format_listing(r) for r in rows)


# ────────────────────────────────────────────────────────────────────────────
# Telegram HTTP
# ────────────────────────────────────────────────────────────────────────────

TG = "https://api.telegram.org/bot{token}/{method}"


def tg_get_updates(token: str, offset: int, long_poll_secs: int = 30) -> list[dict]:
    try:
        r = _requests.get(
            TG.format(token=token, method="getUpdates"),
            params={"offset": offset, "timeout": long_poll_secs,
                    "allowed_updates": '["message"]'},
            timeout=long_poll_secs + 10,
        )
        r.raise_for_status()
        data = r.json()
        return data.get("result", []) if data.get("ok") else []
    except Exception as e:
        logger.warning(f"getUpdates failed: {e}")
        return []


def tg_send(token: str, chat_id: int, text: str):
    # Telegram caps at 4096 chars. Truncate if needed.
    if len(text) > 4000:
        text = text[:3990] + "\n…(truncated)"
    try:
        _requests.post(
            TG.format(token=token, method="sendMessage"),
            json={"chat_id": chat_id, "text": text,
                  "disable_web_page_preview": False},
            timeout=15,
        )
    except Exception as e:
        logger.warning(f"sendMessage failed: {e}")


# ────────────────────────────────────────────────────────────────────────────
# Message handler
# ────────────────────────────────────────────────────────────────────────────

def handle_message(client, token: str, message: dict, limit: int):
    chat_id = message["chat"]["id"]
    text = (message.get("text") or "").strip()
    if not text:
        return

    if text.startswith(("/start", "/help")):
        tg_send(token, chat_id, (
            "👋 I'm the car listings bot. Ask me in plain English, e.g.:\n\n"
            "• white toyota camry 2021+ under 90k in dubai\n"
            "• any porsche under 300k low mileage\n"
            "• new honda civic with warranty\n\n"
            f"I filter the DB and show up to {limit} matches."
        ))
        return

    logger.info(f"[{chat_id}] query: {text!r}")
    filters = extract_filters(text)
    logger.info(f"[{chat_id}] filters: {filters}")

    try:
        rows, total = search(client, filters, limit=limit)
    except Exception as e:
        logger.exception("DB search failed")
        tg_send(token, chat_id, f"⚠️ DB error: {type(e).__name__}")
        return

    tg_send(token, chat_id, format_reply(filters, rows, total))


# ────────────────────────────────────────────────────────────────────────────
# Main loop
# ────────────────────────────────────────────────────────────────────────────

def _must_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        logger.error(f"Missing env var: {name}")
        sys.exit(1)
    return v


def _parse_allowlist(raw: Optional[str]) -> Optional[set[int]]:
    if not raw:
        return None
    out: set[int] = set()
    for x in raw.split(","):
        x = x.strip()
        if not x:
            continue
        try:
            out.add(int(x))
        except ValueError:
            logger.warning(f"BOT_ALLOWED_CHAT_IDS: ignoring non-int {x!r}")
    return out or None


def main():
    token = _must_env("TELEGRAM_BOT_TOKEN")
    api_key = _must_env("GEMINI_API_KEY")
    sb_url = _must_env("SUPABASE_URL")
    sb_key = _must_env("SUPABASE_SERVICE_KEY")

    try:
        limit = int(os.environ.get("BOT_RESULT_LIMIT", "10"))
    except ValueError:
        limit = 10

    allow = _parse_allowlist(os.environ.get("BOT_ALLOWED_CHAT_IDS"))

    genai.configure(api_key=api_key)
    client = get_client(sb_url, sb_key)

    logger.info(
        f"Bot starting — model={GEMINI_MODEL}, limit={limit}, "
        f"allowlist={'on' if allow else 'off'}"
    )

    offset = 0
    while True:
        try:
            updates = tg_get_updates(token, offset, long_poll_secs=30)
            for u in updates:
                offset = u["update_id"] + 1
                msg = u.get("message")
                if not msg:
                    continue
                chat_id = msg.get("chat", {}).get("id")
                if allow and chat_id not in allow:
                    logger.info(f"[{chat_id}] denied (not in allowlist)")
                    continue
                try:
                    handle_message(client, token, msg, limit)
                except Exception as e:
                    logger.exception(f"handle_message error: {e}")
        except Exception as e:
            logger.exception(f"main loop tick error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
