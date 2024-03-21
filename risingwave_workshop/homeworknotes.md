[Risingwave workshop video](https://www.youtube.com/watch?v=L2BHFnZ6XjE) <br/>
[Risingwave workshop details](https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04/blob/main/workshop.md)  <br/>
[Homework questions](https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04/blob/main/homework.md#setting-up)


# Question 0
 This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution.
 What are the dropoff taxi zones at the latest dropoff times?


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
 Create a materialized view to compute the average, min and max trip time between each taxi zone.
 From this MV, find the pair of taxi zones with the highest average trip time. You may need to use the dynamic filter pattern for this.
 Bonus (no marks): Create an MV which can identify anomalies in the data. 
 For example, if the average trip time between two zones is 1 minute, but the max trip time is 10 minutes and 20 minutes respectively.



```
CREATE MATERIALIZED VIEW trip_time AS  
WITH T AS (
SELECT 
EXTRACT(hour FROM tpep_dropoff_datetime -tpep_pickup_datetime) * 60 * 60  + 
EXTRACT(minute FROM tpep_dropoff_datetime -tpep_pickup_datetime) * 60  +
EXTRACT(second FROM tpep_dropoff_datetime -tpep_pickup_datetime)  triptime, 
taxi_zone.zone pickupzone, taxi_zone_1.zone dropoffzone 
FROM trip_data
 JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
 JOIN taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id
)
SELECT avg(triptime) AVG_TRIP_TIME , min(triptime) MIN_TRIP_TIME, max(triptime) MAX_TRIP_TIME ,pickupzone,dropoffzone 
from T 
group by pickupzone,dropoffzone;

dev=> SELECT * FROM trip_time ORDER BY  AVG_TRIP_TIME DESC LIMIT 5;;
 avg_trip_time | min_trip_time | max_trip_time |           pickupzone           |        dropoffzone        
---------------+---------------+---------------+--------------------------------+---------------------------
  86373.000000 |  86373.000000 |  86373.000000 | Yorkville East                 | Steinway
  86324.000000 |  86324.000000 |  86324.000000 | Stuy Town/Peter Cooper Village | Murray Hill-Queens
  86320.000000 |  86320.000000 |  86320.000000 | Washington Heights North       | Highbridge Park
  86294.000000 |  86294.000000 |  86294.000000 | Two Bridges/Seward Park        | Bushwick South
  86036.000000 |  86036.000000 |  86036.000000 | Clinton East                   | Prospect-Lefferts Gardens


```

# Question 2

Recreate the MV(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.

```
DROP  MATERIALIZED VIEW trip_time;

CREATE MATERIALIZED VIEW trip_time AS  
WITH T AS (
SELECT 
EXTRACT(hour FROM tpep_dropoff_datetime -tpep_pickup_datetime) * 60 * 60  + 
EXTRACT(minute FROM tpep_dropoff_datetime -tpep_pickup_datetime) * 60  +
EXTRACT(second FROM tpep_dropoff_datetime -tpep_pickup_datetime)  triptime, 
taxi_zone.zone pickupzone, taxi_zone_1.zone dropoffzone 
FROM trip_data
 JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
 JOIN taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id
)
SELECT avg(triptime) AVG_TRIP_TIME , min(triptime) MIN_TRIP_TIME, max(triptime) MAX_TRIP_TIME , COUNT(*)  no_of_trips , pickupzone,dropoffzone 
from T 
group by pickupzone,dropoffzone;

dev=> SELECT * FROM trip_time ORDER BY  AVG_TRIP_TIME DESC LIMIT 5;
 avg_trip_time | min_trip_time | max_trip_time | no_of_trips |           pickupzone           |        dropoffzone        
---------------+---------------+---------------+-------------+--------------------------------+---------------------------
  86373.000000 |  86373.000000 |  86373.000000 |           1 | Yorkville East                 | Steinway
  86324.000000 |  86324.000000 |  86324.000000 |           1 | Stuy Town/Peter Cooper Village | Murray Hill-Queens
  86320.000000 |  86320.000000 |  86320.000000 |           1 | Washington Heights North       | Highbridge Park
  86294.000000 |  86294.000000 |  86294.000000 |           1 | Two Bridges/Seward Park        | Bushwick South
  86036.000000 |  86036.000000 |  86036.000000 |           1 | Clinton East                   | Prospect-Lefferts Gardens

```

# Question 3
 From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups? For example if the latest pickup time is 2020-01-01 17:00:00, then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

 HINT: You can use dynamic filter pattern to create a filter condition based on the latest pickup time.

 NOTE: For this question 17 hours was picked to ensure we have enough data to work with.

```
 CREATE MATERIALIZED VIEW busiest_zones AS 
WITH T AS (
SELECT zone , tpep_pickup_datetime
FROM trip_data 
JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
WHERE tpep_pickup_datetime > ('2022-01-03 10:53:33'::TIMESTAMP ) - interval '17 hour'
)
SELECT count(*) pickups,zone 
FROM T
group by zone
order by pickups DESC
LIMIT 10;

dev=> SELECT * FROM busiest_zones;
 pickups |             zone             
---------+------------------------------
      19 | LaGuardia Airport
      17 | JFK Airport
      17 | Lincoln Square East
      16 | Penn Station/Madison Sq West
      13 | Upper East Side North
      12 | Times Sq/Theatre District
      11 | East Chelsea
      10 | Upper East Side South
       8 | Clinton East
       8 | Lenox Hill West
(10 rows)

```