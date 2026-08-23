-- Metabase card: Total flights
-- Visualization: Number
SELECT COALESCE(SUM(total_flights), 0) AS total_flights
FROM iceberg.gold.agg_delay_by_airline_hour
