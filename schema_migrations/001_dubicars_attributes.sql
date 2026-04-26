-- 001_dubicars_attributes.sql
-- Adds a generic `attributes` JSONB column to car_listings, used as the
-- source-specific catch-all for fields that don't map to a real column and
-- aren't part of the curated SPECS_WHITELIST.
--
-- DubiCars in particular carries a long tail of card-level metadata
-- (regional_specs_id, drive_type, ad_type, is_360, item_wheels, …) that we
-- want to keep but don't need first-class column status for.
--
-- Idempotent — safe to re-run.

ALTER TABLE car_listings
    ADD COLUMN IF NOT EXISTS attributes JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_listings_attributes_gin
    ON car_listings USING GIN (attributes);
