--
-- QUESTION 3
--
SELECT COUNT(*)
FROM PUBLIC.green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01'
    AND lpep_dropoff_datetime < '2019-11-01'
    AND trip_distance <= 1;
--
SELECT COUNT(*)
FROM PUBLIC.green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01'
    AND lpep_dropoff_datetime < '2019-11-01'
    AND trip_distance > 1
    AND trip_distance <= 3;
--
SELECT COUNT(*)
FROM PUBLIC.green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01'
    AND lpep_dropoff_datetime < '2019-11-01'
    AND trip_distance > 3
    AND trip_distance <= 7;
--
SELECT COUNT(*)
FROM PUBLIC.green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01'
    AND lpep_dropoff_datetime < '2019-11-01'
    AND trip_distance > 7
    AND trip_distance <= 10;
--
SELECT COUNT(*)
FROM PUBLIC.green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01'
    AND lpep_dropoff_datetime < '2019-11-01'
    AND trip_distance > 10;
--
-- QUESTION 4
--
SELECT *
FROM public.green_taxi_trips
ORDER BY trip_distance DESC
LIMIT 1;
--
--QUESTION 5
--
SELECT z."Zone" AS pickup_zone_name,
SUM(t."total_amount") AS total_amount
FROM public.green_taxi_trips t
    JOIN public.zones z ON t."PULocationID" = z."LocationID"
WHERE DATE(t."lpep_pickup_datetime") = '2019-10-18'
GROUP BY t."PULocationID",
    z."Zone"
HAVING SUM(t."total_amount") > 13000
ORDER BY total_amount DESC
LIMIT 3;
--
--QUESTION 6
--
SELECT
dz."Zone" AS dropoff_zone_name,
MAX(t."tip_amount") AS largest_tip
FROM public.green_taxi_trips t
    JOIN public.zones pz ON t."PULocationID" = pz."LocationID"
    JOIN public.zones dz ON t."DOLocationID" = dz."LocationID"
WHERE pz."Zone" = 'East Harlem North'
    AND t."lpep_pickup_datetime" >= '2019-10-01'
    AND t."lpep_pickup_datetime" < '2019-11-01'
GROUP BY dz."Zone"
ORDER BY largest_tip DESC
LIMIT 1;