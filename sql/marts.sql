-- Grain: one row per accepted pickup date. Currency includes source taxes/fees, not profit.
CREATE OR REPLACE TABLE gold_daily AS
SELECT date_key, count(*) AS trips, sum(total_usd) AS total_usd,
       sum(fare_usd) AS fare_usd, sum(miles) AS miles,
       avg(duration_seconds)/60.0 AS mean_trip_minutes
FROM fact_trip GROUP BY date_key ORDER BY date_key;
-- Grain: one row per pickup borough; Unknown is retained rather than invented.
CREATE OR REPLACE TABLE gold_pickup_borough AS
SELECT z.borough, count(*) AS trips, sum(f.total_usd) AS total_usd,
       avg(f.miles) AS mean_miles
FROM fact_trip f JOIN dim_zone z ON f.pickup_zone_id=z.zone_id
GROUP BY z.borough ORDER BY trips DESC;
