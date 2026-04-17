-- ============================================================
-- Car Listings Schema v2 — RAG-ready
-- Run once in Supabase SQL Editor
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;
-- Uncomment when ready for RAG:
-- CREATE EXTENSION IF NOT EXISTS vector;

-- ============================================================
-- CORE TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS car_listings (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Source
    source              TEXT NOT NULL,
    external_id         TEXT NOT NULL,
    url                 TEXT NOT NULL,
    content_hash        TEXT NOT NULL,

    -- Identity
    make                TEXT,
    model               TEXT,
    variant             TEXT,
    year                INTEGER,
    body_type           TEXT,

    -- Condition
    condition           TEXT,
    mileage_km          INTEGER,
    fuel_type           TEXT,
    transmission        TEXT,
    engine_cc           INTEGER,
    cylinders           INTEGER,
    color               TEXT,
    doors               INTEGER,

    -- Pricing (price_aed_max populated when listing shows a range)
    price_aed           NUMERIC(12,2),
    price_aed_max       NUMERIC(12,2),
    price_negotiable    BOOLEAN,

    -- Location
    emirate             TEXT,
    area                TEXT,

    -- Seller
    seller_type         TEXT,
    seller_name         TEXT,
    seller_phone        TEXT,

    -- Media
    image_urls          TEXT[],
    description         TEXT,
    specs               JSONB DEFAULT '{}',

    -- Lifecycle
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_changed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    deleted_at          TIMESTAMPTZ,

    -- RAG (uncomment when ready):
    -- embedding         vector(1536),

    UNIQUE (source, external_id)
);

-- ============================================================
-- CHANGE HISTORY
-- ============================================================
CREATE TABLE IF NOT EXISTS car_listing_changes (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    listing_id      UUID NOT NULL REFERENCES car_listings(id) ON DELETE CASCADE,
    changed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    change_type     TEXT NOT NULL,      -- created | updated | deleted | relisted
    changed_fields  JSONB,
    old_hash        TEXT,
    new_hash        TEXT
);

-- ============================================================
-- SCRAPE RUN LOG
-- ============================================================
CREATE TABLE IF NOT EXISTS scrape_runs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source           TEXT NOT NULL,
    status           TEXT NOT NULL,     -- success | partial | failed
    listings_found   INTEGER DEFAULT 0,
    listings_new     INTEGER DEFAULT 0,
    listings_updated INTEGER DEFAULT 0,
    listings_deleted INTEGER DEFAULT 0,
    error_message    TEXT,
    duration_seconds NUMERIC(8,2)
);

-- ============================================================
-- INDEXES
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_listings_source         ON car_listings(source);
CREATE INDEX IF NOT EXISTS idx_listings_active         ON car_listings(is_active);
CREATE INDEX IF NOT EXISTS idx_listings_make_model     ON car_listings(make, model);
CREATE INDEX IF NOT EXISTS idx_listings_year           ON car_listings(year);
CREATE INDEX IF NOT EXISTS idx_listings_price          ON car_listings(price_aed);
CREATE INDEX IF NOT EXISTS idx_listings_emirate        ON car_listings(emirate);
CREATE INDEX IF NOT EXISTS idx_listings_last_seen      ON car_listings(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_listings_source_active  ON car_listings(source, is_active, external_id);
CREATE INDEX IF NOT EXISTS idx_changes_listing_id      ON car_listing_changes(listing_id);
CREATE INDEX IF NOT EXISTS idx_changes_changed_at      ON car_listing_changes(changed_at);
CREATE INDEX IF NOT EXISTS idx_changes_type            ON car_listing_changes(change_type);

-- ============================================================
-- RLS
-- ============================================================
ALTER TABLE car_listings         ENABLE ROW LEVEL SECURITY;
ALTER TABLE car_listing_changes  ENABLE ROW LEVEL SECURITY;
ALTER TABLE scrape_runs          ENABLE ROW LEVEL SECURITY;
-- Service role bypasses RLS automatically.
-- Add team read policies here when building dashboard/RAG.

-- ============================================================
-- USEFUL MONITORING QUERIES (save these in Supabase saved queries)
-- ============================================================

-- Last 7 days of scrape runs:
-- SELECT source, status, listings_found, listings_new, listings_updated,
--        listings_deleted, duration_seconds, run_at
-- FROM scrape_runs ORDER BY run_at DESC LIMIT 50;

-- Active listing count by source:
-- SELECT source, COUNT(*) as active FROM car_listings
-- WHERE is_active = true GROUP BY source ORDER BY active DESC;

-- Price drops in last 24h:
-- SELECT cl.make, cl.model, cl.year, cl.source,
--        (clc.changed_fields->'price_aed'->>'old')::numeric as old_price,
--        (clc.changed_fields->'price_aed'->>'new')::numeric as new_price,
--        clc.changed_at
-- FROM car_listing_changes clc
-- JOIN car_listings cl ON cl.id = clc.listing_id
-- WHERE clc.change_type = 'updated'
--   AND clc.changed_fields ? 'price_aed'
--   AND clc.changed_at > NOW() - INTERVAL '24 hours'
-- ORDER BY clc.changed_at DESC;
