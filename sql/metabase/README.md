# Metabase dashboard SQL

Each `.sql` file is one saved Metabase native query for the **Flight Delay Analytics** dashboard.

1. In Metabase, select **New → SQL query** and choose `Flight Delay Lakehouse`.
2. Copy the contents of one query file, run it, select the indicated visualization, and save it to the **Flight Delay Analytics** collection.
3. Add the saved questions to one dashboard in numeric order.

| File | Dashboard card | Visualization |
|---|---|---|
| `01_total_flights.sql` | Total flights | Number |
| `02_delayed_flights.sql` | Delayed flights | Number |
| `03_overall_delay_rate.sql` | Overall delay rate | Number / percentage |
| `04_weighted_average_delay.sql` | Average delay | Number |
| `05_delay_rate_by_airline.sql` | Delay rate by airline | Bar |
| `06_delay_rate_by_airport.sql` | Delay rate by departure airport | Bar |
| `07_hourly_delay_trend.sql` | Hourly delay-rate trend | Line |
| `08_flight_delay_detail.sql` | Flight-delay detail | Table |

The rate columns intentionally return a decimal between `0` and `1`; format them as a percentage in Metabase. The average-delay cards are weighted by flight count, so an hour with a small number of flights cannot distort the overall metric.