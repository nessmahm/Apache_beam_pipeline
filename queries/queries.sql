1. Calculate the total number of trips.

SELECT COUNT(*) AS total_rows FROM yellow_tripdata_2022-01;

*----*

2. Calculate the average trip distance.

SELECT AVG(trip_distance) AS avg_distance
FROM yellow_tripdata_2022-01;

*----*

3. Find the top 5 most common pickup locations.

SELECT PULocationID, COUNT(PULocationID) AS pickup_location_count
FROM yellow_tripdata_2022-01
GROUP BY PULocationID
ORDER BY pickup_location_count DESC
LIMIT 5;

*----*
4. Find the top 5 most common drop-off locations.

SELECT DOLocationID, COUNT(DOLocationID) AS dropoff_location_count
FROM yellow_tripdata_2022-01
GROUP BY DOLocationID
ORDER BY dropoff_location_count DESC
LIMIT 5;

*----*

5. Calculate the total amount of tips given.

SELECT SUM(Tip_amount) AS total_tips FROM yellow_tripdata_2022-01;