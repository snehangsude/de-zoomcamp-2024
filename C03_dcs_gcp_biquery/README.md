### Q1.

SELECT COUNT(*) FROM `dtc-de-course-0001.green_taxi_2022.green_taxi`

### Q2.

SELECT COUNT(PULocationID) FROM `dtc-de-course-0001.green_taxi_2022.green_taxi`

### Q3.

SELECT COUNT(fare_amount) FROM `dtc-de-course-0001.green_taxi_2022.green_taxi` WHERE fare_amount = 0

### Q4.

CREATE OR REPLACE TABLE `dtc-de-course-0001.green_taxi_2022.green_taxi_clustered`
CLUSTER BY PULocationID, lpep_pickup_datetime AS
SELECT * FROM `dtc-de-course-0001.green_taxi_2022.green_taxi`;

### Q5.

SELECT 
  COUNT(DISTINCT PULocationID) 
FROM 
  `dtc-de-course-0001.green_taxi_2022.green_taxi_clustered`
WHERE
  lpep_pickup_datetime BETWEEN 1654021800000000000 AND 1654108199000000000;