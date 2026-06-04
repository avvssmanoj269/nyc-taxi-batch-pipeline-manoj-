
# Data Dictionary

## Silver Table: yellow_trips_clean

| Column | Description |
|---|---|
| vendor_id | Taxi vendor identifier |
| pickup_datetime | Trip pickup timestamp |
| dropoff_datetime | Trip dropoff timestamp |
| pickup_date | Pickup date |
| dropoff_date | Dropoff date |
| pickup_day | Day of pickup month |
| passenger_count | Number of passengers |
| trip_distance | Trip distance in miles |
| trip_duration_minutes | Trip duration in minutes |
| rate_code_id | Rate code assigned to the trip |
| store_and_forward_flag | Whether the trip record was stored before forwarding |
| pickup_location_id | Pickup taxi zone ID |
| pickup_borough | Pickup borough |
| pickup_zone | Pickup zone name |
| pickup_service_zone | Pickup service zone |
| dropoff_location_id | Dropoff taxi zone ID |
| dropoff_borough | Dropoff borough |
| dropoff_zone | Dropoff zone name |
| dropoff_service_zone | Dropoff service zone |
| payment_type | Payment type code |
| fare_amount | Metered fare amount |
| extra | Extra charges |
| mta_tax | MTA tax amount |
| tip_amount | Tip amount |
| tolls_amount | Tolls amount |
| improvement_surcharge | Improvement surcharge |
| congestion_surcharge | Congestion surcharge |
| airport_fee | Airport fee |
| total_amount | Total trip amount |
| ingestion_timestamp | Timestamp when row was ingested into Bronze |
| source_file | Original source file path |
| batch_id | Unique ingestion batch ID |
| silver_processed_timestamp | Timestamp when row was processed into Silver |

## Gold Table: daily_trip_summary

| Column | Description |
|---|---|
| pickup_date | Trip pickup date |
| total_trips | Number of trips for the day |
| total_revenue | Total revenue for the day |
| avg_total_amount | Average total amount |
| avg_fare_amount | Average fare amount |
| avg_trip_distance | Average trip distance |
| avg_trip_duration_minutes | Average trip duration |
| total_trip_distance | Total distance travelled |
| gold_processed_timestamp | Timestamp when Gold table was generated |

## Gold Table: monthly_revenue_summary

| Column | Description |
|---|---|
| total_trips | Number of trips in the month |
| total_revenue | Total monthly revenue |
| avg_total_amount | Average total amount |
| avg_fare_amount | Average fare amount |
| avg_trip_distance | Average trip distance |
| avg_trip_duration_minutes | Average trip duration |
| total_trip_distance | Total monthly trip distance |
| gold_processed_timestamp | Timestamp when Gold table was generated |

## Gold Table: zone_trip_summary

| Column | Description |
|---|---|
| pickup_borough | Pickup borough |
| pickup_zone | Pickup zone name |
| pickup_service_zone | Pickup service zone |
| total_trips | Number of trips from the zone |
| total_revenue | Total revenue from the zone |
| avg_total_amount | Average total amount |
| avg_fare_amount | Average fare amount |
| avg_trip_distance | Average trip distance |
| avg_trip_duration_minutes | Average trip duration |
| gold_processed_timestamp | Timestamp when Gold table was generated |
