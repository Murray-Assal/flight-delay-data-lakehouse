-- Metabase chart: Hourly delay-rate trend
-- Visualization: Line chart. X-axis: hour_start_at. Y-axis: delay_rate.
-- Format delay_rate as Percentage.
SELECT
    hour_start_at,
    SUM(total_flights) AS total_flights,
    SUM(delayed_flights) AS delayed_flights,
    CAST(SUM(delayed_flights) AS DOUBLE) / NULLIF(SUM(total_flights), 0) AS delay_rate,
    SUM(average_delay_minutes * total_flights) / NULLIF(SUM(total_flights), 0) AS average_delay_minutes
FROM iceberg.gold.agg_delay_by_airline_hour
GROUP BY hour_start_at
ORDER BY hour_start_at
