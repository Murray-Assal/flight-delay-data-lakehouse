-- Metabase card: Flight-delay detail
-- Visualization: Table. This uses Silver for readable flight and airport names;
-- the dashboard metrics remain sourced from Gold.
SELECT
    flight_iata,
    airline_iata,
    airline_name,
    departure_airport_iata,
    departure_airport_name,
    arrival_airport_iata,
    arrival_airport_name,
    scheduled_departure_at,
    delay_minutes,
    is_delayed,
    observed_at
FROM iceberg.silver.silver_flights
WHERE quality_status = 'valid'
  AND delay_minutes IS NOT NULL
ORDER BY scheduled_departure_at DESC, delay_minutes DESC
LIMIT 500
