-- Metabase card: Overall delay rate
-- Visualization: Number; set the number format to Percentage.
SELECT COALESCE(
    CAST(SUM(delayed_flights) AS DOUBLE) / NULLIF(SUM(total_flights), 0),
    0e0
) AS delay_rate
FROM iceberg.gold.agg_delay_by_airline_hour
