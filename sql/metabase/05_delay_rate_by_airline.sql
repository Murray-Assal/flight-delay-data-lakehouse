-- Metabase chart: Delay rate by airline
-- Visualization: Bar chart. X-axis: airline_iata. Y-axis: delay_rate.
-- Format delay_rate as Percentage.
SELECT
    airline_iata,
    SUM(total_flights) AS total_flights,
    SUM(delayed_flights) AS delayed_flights,
    CAST(SUM(delayed_flights) AS DOUBLE) / NULLIF(SUM(total_flights), 0) AS delay_rate,
    SUM(average_delay_minutes * total_flights) / NULLIF(SUM(total_flights), 0) AS average_delay_minutes
FROM iceberg.gold.agg_delay_by_airline_hour
GROUP BY airline_iata
ORDER BY delay_rate DESC, total_flights DESC, airline_iata
