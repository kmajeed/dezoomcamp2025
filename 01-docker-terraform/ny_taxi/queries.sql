/*Question 3. Trip Segmentation Counts*/
SELECT
    CASE
        WHEN trip_distance <= 1 THEN 'Up to 1 mile'
        WHEN trip_distance > 1 AND trip_distance <= 3 THEN 'Between 1 and 3 miles'
        WHEN trip_distance > 3 AND trip_distance <= 7 THEN 'Between 3 and 7 miles'
        WHEN trip_distance > 7 AND trip_distance <= 10 THEN 'Between 7 and 10 miles'
        WHEN trip_distance > 10 THEN 'Over 10 miles'
        ELSE 'Unknown'  -- Handle potential NULL or unexpected values
    END AS trip_segment,
    COUNT(*) AS trip_count
FROM
    green_taxi_trips
WHERE
    dropoff_datetime >= '2019-10-01' AND dropoff_datetime < '2019-11-01'
GROUP BY
    trip_segment
ORDER BY
    trip_segment;

/*Question 4. Longest trip for each day*/
WITH DailyMaxTrips AS (
    SELECT
        DATE(pickup_datetime) AS pickup_date,
        MAX(trip_distance) AS max_trip_distance
    FROM
        green_taxi_trips
    GROUP BY
        pickup_date
),
RankedTrips AS (
    SELECT
        DATE(pickup_datetime) AS pickup_date,
        trip_distance,
        ROW_NUMBER() OVER (PARTITION BY DATE(pickup_datetime) ORDER BY trip_distance DESC) as rn
    FROM
        green_taxi_trips
    WHERE (DATE(pickup_datetime), trip_distance) IN (SELECT pickup_date, max_trip_distance from DailyMaxTrips)
)
SELECT pickup_date, trip_distance
from RankedTrips
where rn = 1
ORDER BY trip_distance DESC;

/* Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.
*/
SELECT
    tz.zone,
    SUM(gtt.total_amount) AS total_amount_sum
FROM
    green_taxi_trips gtt
JOIN
    taxi_zones tz ON gtt.pickup_location_id = tz.location_id
WHERE
    DATE(gtt.pickup_datetime) = '2019-10-18'
GROUP BY
    tz.zone
HAVING
    SUM(gtt.total_amount) > 13000
ORDER BY
    total_amount_sum DESC;

/*
Question 6
For the passengers picked up in October 2019 in the zone named "East Harlem North" 
which was the drop off zone that had the largest tip?
Note: it's tip , not trip
We need the name of the zone, not the ID.
*/
SELECT
    dropoff_zone.zone,
    MAX(gtt.tip_amount) AS largest_tip_amount
FROM
    green_taxi_trips gtt
JOIN
    taxi_zones pickup_zone ON gtt.pickup_location_id = pickup_zone.location_id
JOIN
    taxi_zones dropoff_zone ON gtt.dropoff_location_id = dropoff_zone.location_id
WHERE
    pickup_zone.zone = 'East Harlem North'
    AND gtt.pickup_datetime >= '2019-10-01'
    AND gtt.pickup_datetime < '2019-11-01'
GROUP BY
    dropoff_zone.zone
ORDER BY
    largest_tip_amount DESC
LIMIT 1;

