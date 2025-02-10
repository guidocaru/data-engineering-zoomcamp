--
-- QUESTION 1
--
SELECT
COUNT(*)
FROM `zoomcamp-449416.zoomcamp.yellow_taxi` --
-- QUESTION 2
    --
SELECT
    DISTINCT PULocationID
FROM `zoomcamp-449416.zoomcamp.yellow_taxi` --`zoomcamp-449416.zoomcamp.yellow_taxi_external`
    --
-- QUESTION 3
    --
SELECT
    PULocationID,
    DOLocationID
FROM `zoomcamp-449416.zoomcamp.yellow_taxi` --
-- QUESTION 4
    --
SELECT
    COUNT(*)
FROM `zoomcamp-449416.zoomcamp.yellow_taxi`
WHERE fare_amount = 0 --
-- QUESTION 5
    --
CREATE TABLE
    `zoomcamp-449416.zoomcamp.yellow_taxi_optimized` PARTITION BY DATE(tpep_dropoff_datetime) CLUSTER BY VendorID AS
SELECT *
FROM `zoomcamp-449416.zoomcamp.yellow_taxi`;
--
-- QUESTION 6
--
SELECT
DISTINCT VendorID
FROM `zoomcamp-449416.zoomcamp.yellow_taxi` --`zoomcamp-449416.zoomcamp.yellow_taxi_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15' --
-- QUESTION 1
    - -