-- Metabase card: Weighted average delay
-- Visualization: Number; set suffix to " min".
SELECT COALESCE(
    SUM(average_delay_minutes * total_flights) / NULLIF(SUM(total_flights), 0),
    0e0
) AS average_delay_minutes
FROM iceberg.gold.agg_delay_by_airline_hour
