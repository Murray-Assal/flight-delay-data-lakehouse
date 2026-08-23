-- Metabase card: Delayed flights
-- Visualization: Number
SELECT COALESCE(SUM(delayed_flights), 0) AS delayed_flights
FROM iceberg.gold.agg_delay_by_airline_hour
