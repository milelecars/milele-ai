# Car Listing Scraper v2 — Milele Group

Scrapes **Dubizzle · DubiCars · YallaMotor · CarSwitch · SellAnyCar** daily.  
Stores all data in Supabase with full change history and a RAG-ready schema.

---

## Architecture

```
GitHub Actions (free, cron: 3 AM UAE daily)
    └── main.py  --stagger 90
         ├── scrapers/dubizzle.py    ← Apify-first → JSON API → HTML fallback
         ├── scrapers/dubicars.py    ← Apify-first → JSON-LD  → HTML fallback
         ├── scrapers/yallamotor.py  ← Apify-first → JSON-LD  → HTML fallback
         ├── scrapers/carswitch.py   ← __NEXT_DATA__ JSON
         └── scrapers/sellanycar.py  ← __NEXT_DATA__ JSON
              │
              └── utils/db.py ──► Supabase (free tier, separate project)
                    ├── car_listings          live + historical data
                    ├── car_listing_changes   field-level audit trail
                    └── scrape_runs           health log per run
```

**Change detection:** MD5 hash of `price_aed`, `mileage_km`, `description`, `seller_phone`,
`image_urls` (sorted), `is_active`. Each run:

| Scenario | Action |
|----------|--------|
| New listing | INSERT + log `created` |
| Hash changed | UPDATE changed fields only + log `updated` with before/after diff |
| Hash same | Touch `last_seen_at` only (no write amplification) |
| Listing gone | Soft-delete (`is_active=false`) + log `deleted` |
| >35% would be deleted | **ABORT** — almost certainly scraper breakage, not mass delistings |

---

## One-time Setup

### 1. Supabase

1. [supabase.com](https://supabase.com) → **New project** → name it `milele-cars`  
   *(keep separate from the trading bot project)*
2. **SQL Editor** → paste `schema.sql` → **Run**
3. **Project Settings → API** → copy:
   - **Project URL** → `SUPABASE_URL`
   - **service_role** key *(not anon)* → `SUPABASE_SERVICE_KEY`

### 2. GitHub Secrets

**Settings → Secrets and variables → Actions → New repository secret**

| Secret | Required | Value |
|--------|----------|-------|
| `SUPABASE_URL` | ✅ | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | ✅ | service_role key |
| `APIFY_TOKEN` | Optional | Apify API token (see below) |
| `ALERT_WEBHOOK_URL` | Optional | Slack/Teams/Discord webhook URL |

### 3. Apify (optional but strongly recommended)

Apify provides maintained actors for Dubizzle, DubiCars, and YallaMotor with
residential proxy rotation and JS rendering — eliminates the most common block vectors.

1. [apify.com](https://apify.com) → free tier (10 USD credit/month)
2. **Account → Integrations** → copy API token → add as `APIFY_TOKEN` secret
3. That's it — scrapers auto-detect the token and use Apify-first mode

Without `APIFY_TOKEN`: scrapers fall back to direct HTTP with UA rotation + session warm-up.

### 4. Alert webhook (optional)

Create a Slack incoming webhook at `api.slack.com/messaging/webhooks` and add it as
`ALERT_WEBHOOK_URL`. You'll get a message if any scrape run fails.

### 5. Push to GitHub

```bash
git init
git add .
git commit -m "Car scraper v2"
git remote add origin https://github.com/YOUR_ORG/milele-car-scraper.git
git push -u origin main
```

Workflow activates automatically at 3 AM UAE each day.

---

## Manual Runs

**GitHub UI:** Actions → Daily Car Scrape → Run workflow → pick source / dry run

**Local:**
```bash
pip install -r requirements.txt
cp .env.example .env   # fill in credentials

python main.py                      # all sources
python main.py --source dubizzle    # single source
python main.py --dry-run            # scrape only, no DB writes
python main.py --stagger 60         # 60s between sources
```

---

## Monitoring

Run these in **Supabase → SQL Editor**:

```sql
-- Last 7 days of scrape runs
SELECT source, status, listings_found, listings_new,
       listings_updated, listings_deleted, duration_seconds,
       to_char(run_at AT TIME ZONE 'Asia/Dubai', 'YYYY-MM-DD HH24:MI') as run_at_uae
FROM scrape_runs
ORDER BY run_at DESC LIMIT 50;

-- Active listings by source
SELECT source, COUNT(*) as active
FROM car_listings WHERE is_active = true
GROUP BY source ORDER BY active DESC;

-- Price drops in last 24 hours
SELECT cl.make, cl.model, cl.year, cl.source,
       (clc.changed_fields->'price_aed'->>'old')::numeric as old_price,
       (clc.changed_fields->'price_aed'->>'new')::numeric as new_price,
       clc.changed_at AT TIME ZONE 'Asia/Dubai' as changed_at_uae
FROM car_listing_changes clc
JOIN car_listings cl ON cl.id = clc.listing_id
WHERE clc.change_type = 'updated'
  AND clc.changed_fields ? 'price_aed'
  AND (clc.changed_fields->'price_aed'->>'new')::numeric
    < (clc.changed_fields->'price_aed'->>'old')::numeric
  AND clc.changed_at > NOW() - INTERVAL '24 hours'
ORDER BY clc.changed_at DESC;

-- New listings in last 24 hours
SELECT make, model, year, price_aed, emirate, source, first_seen_at
FROM car_listings
WHERE first_seen_at > NOW() - INTERVAL '24 hours'
ORDER BY first_seen_at DESC;
```

---

## RAG Layer (Phase 2)

When ready to build the WhatsApp AI assistant:

1. Uncomment `CREATE EXTENSION IF NOT EXISTS vector` in `schema.sql`
2. Uncomment the `embedding vector(1536)` column and re-run migration
3. Add a nightly embedding job: for new/changed listings, call embeddings API
   (OpenAI `text-embedding-3-small` or `voyage-2`) on concatenated fields
4. Build the RAG retrieval: pgvector similarity search → Claude API → WhatsApp via Twilio
5. Zero schema migration needed — the column is already designed in

---

## Scraper Maintenance

If a scraper stops returning data:

1. Check **GitHub Actions → run log** — look for 403/429/0-listing warnings
2. Visit the site manually — check if URL structure or HTML changed
3. Update the relevant CSS selectors in `scrapers/*.py`
4. Run `python main.py --source <name> --dry-run` locally to verify

Scrapers use a layered approach (Apify → structured JSON → HTML selectors) so a
single-layer change at a site won't necessarily break data collection entirely.
