# Question 0
### This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution.
### What are the dropoff taxi zones at the latest dropoff times?


```

EXPLAIN CREATE MATERIALIZED VIEW latest_dropoff_time AS
  WITH t AS(
  SELECT  max(tpep_dropoff_datetime) AS latest_dropoff_time
  FROM trip_data  
  )
  SELECT
    zone,latest_dropoff_time
    FROM t, trip_data
        JOIN taxi_zone
            ON trip_data.DOLocationID = taxi_zone.location_id
  WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;
```

# Question 1
### Create a materialized view to compute the average, min and max trip time between each taxi zone.
### From this MV, find the pair of taxi zones with the highest average trip time. You may need to use the dynamic filter pattern for this.
### Bonus (no marks): Create an MV which can identify anomalies in the data. 
### For example, if the average trip time between two zones is 1 minute, but the max trip time is 10 minutes and 20 minutes respectively.



```
CREATE MATERIALIZED VIEW trip_time AS 
WITH T AS (
SELECT 
avg(EXTRACT(SECOND FROM tpep_dropoff_datetime - tpep_pickup_datetime)) AVG_TRIP_TIME, 
min(EXTRACT(SECOND FROM tpep_dropoff_datetime -tpep_pickup_datetime))  MIN_TRIP_TIME, 
max(EXTRACT(SECOND FROM tpep_dropoff_datetime -tpep_pickup_datetime))  MAX_TRIP_TIME, 
taxi_zone.zone pickupzone, taxi_zone_1.zone dropoffzone 
FROM trip_data
 JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
 JOIN taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id
 group by 4,5
)
SELECT * FROM T 
WHERE AVG_TRIP_TIME >= MIN_TRIP_TIME AND AVG_TRIP_TIME <= MAX_TRIP_TIME


SELECT * FROM trip_time WHERE pickupzone = 'Yorkville East'
AND dropoffzone = 'Steinway'
ORDER BY  AVG_TRIP_TIME DESC;

dev=> SELECT * FROM trip_time WHERE pickupzone = 'Yorkville East'
dev-> AND dropoffzone = 'Steinway'
dev-> ORDER BY  AVG_TRIP_TIME DESC;
 avg_trip_time | min_trip_time | max_trip_time |   pickupzone   | dropoffzone 
---------------+---------------+---------------+----------------+-------------
     33.000000 |     33.000000 |     33.000000 | Yorkville East | Steinway
(1 row)


 dev=> SELECT * FROM trip_time WHERE pickupzone = 'Murray Hill'
dev-> AND dropoffzone = 'Midwood'
dev-> ORDER BY  AVG_TRIP_TIME DESC;
 avg_trip_time | min_trip_time | max_trip_time | pickupzone | dropoffzone 
---------------+---------------+---------------+------------+-------------
(0 rows)

dev=> SELECT * FROM trip_time WHERE pickupzone = 'East Flatbush/Farragut'
dev-> AND dropoffzone = 'East Harlem North'
dev-> ORDER BY  AVG_TRIP_TIME DESC;
 avg_trip_time | min_trip_time | max_trip_time | pickupzone | dropoffzone 
---------------+---------------+---------------+------------+-------------
(0 rows)

SELECT * FROM trip_time WHERE pickupzone = 'Midtown Center'
AND dropoffzone = 'University Heights/Morris Heights'
ORDER BY  AVG_TRIP_TIME DESC;

dev=> SELECT * FROM trip_time WHERE pickupzone = 'Midtown Center'
dev-> AND dropoffzone = 'University Heights/Morris Heights'
dev-> ORDER BY  AVG_TRIP_TIME DESC;
 avg_trip_time | min_trip_time | max_trip_time | pickupzone | dropoffzone 
---------------+---------------+---------------+------------+-------------
(0 rows)

```

# Question 2

## Recreate the MV(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.

```
DROP  MATERIALIZED VIEW trip_time;

CREATE MATERIALIZED VIEW trip_time AS 
WITH T AS (
SELECT 
avg(EXTRACT(SECOND FROM tpep_dropoff_datetime - tpep_pickup_datetime)) AVG_TRIP_TIME, 
min(EXTRACT(SECOND FROM tpep_dropoff_datetime -tpep_pickup_datetime))  MIN_TRIP_TIME, 
max(EXTRACT(SECOND FROM tpep_dropoff_datetime -tpep_pickup_datetime))  MAX_TRIP_TIME, 
COUNT(*)  no_of_trips,
taxi_zone.zone pickupzone, taxi_zone_1.zone dropoffzone 
FROM trip_data
 JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
 JOIN taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id
 group by 5,6
)
SELECT * FROM T 
WHERE AVG_TRIP_TIME >= MIN_TRIP_TIME AND AVG_TRIP_TIME <= MAX_TRIP_TIME;

SELECT * FROM trip_time WHERE pickupzone = 'Yorkville East'
AND dropoffzone = 'Steinway'
ORDER BY  AVG_TRIP_TIME DESC;

 avg_trip_time | min_trip_time | max_trip_time | no_of_trips |   pickupzone   | dropoffzone 
---------------+---------------+---------------+-------------+----------------+-------------
     33.000000 |     33.000000 |     33.000000 |           1 | Yorkville East | Steinway
(1 row)

```

# Question 3
### From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups? For example if the latest pickup time is 2020-01-01 17:00:00, then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

### HINT: You can use dynamic filter pattern to create a filter condition based on the latest pickup time.

### NOTE: For this question 17 hours was picked to ensure we have enough data to work with.