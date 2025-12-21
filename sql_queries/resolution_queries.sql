-- Queries used in DataResolver for efficient data resolution

-- 1. Identify instruments and their missing data ranges
-- This query finds the earliest and latest timestamp that needs resolution for each instrument.
-- It targets records where resolve_required is TRUE or price data is missing (0 or NULL).
-- It only looks at past data (datetime < NOW()).

SELECT
    instrument_id,
    MIN(datetime) as min_dt,
    MAX(datetime) as max_dt
FROM price_history_intraday  -- or price_history_daily
WHERE
    datetime < NOW()
    AND (
        resolve_required = TRUE
        OR open = 0 OR open IS NULL
        OR high = 0 OR high IS NULL
        OR low = 0 OR low IS NULL
        OR close = 0 OR close IS NULL
    )
GROUP BY instrument_id;

-- 2. Update existing records with resolved data
-- This query updates the price history table with data fetched from the provider.
-- It matches on instrument_id and datetime.

UPDATE price_history_intraday
SET
    open = :open,
    high = :high,
    low = :low,
    close = :close,
    volume = :volume,
    resolve_required = FALSE
WHERE
    instrument_id = :instrument_id
    AND datetime = :datetime;

-- 3. Aggregate Intraday to Daily (for current day)
-- This query aggregates 5-minute candles to form a daily candle for the current day.
-- It uses TimescaleDB/PostgreSQL aggregation functions.

SELECT
    instrument_id,
    first(open, datetime) as open,
    max(high) as high,
    min(low) as low,
    last(close, datetime) as close,
    sum(volume) as volume
FROM price_history_intraday
WHERE
    instrument_id = ANY(:instrument_ids)
    AND datetime >= :today_start_time
GROUP BY instrument_id;

-- 4. Mark gaps for resolution
-- This query identifies gaps in data collection (e.g., if the system was down)
-- and marks the corresponding records as needing resolution.

UPDATE price_history_intraday
SET resolve_required = TRUE
WHERE
    datetime > :last_save_time
    AND datetime < :current_time
    AND instrument_id IN (SELECT id FROM instruments WHERE should_record_data = TRUE);

