SQL Scripts used to run the week 1 homework for the postgres db tables

https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/01-docker-terraform/homework.md
Question 3. Count records
How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18.

Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.

select count(*) from green_taxi_trips 
where 
DATE(lpep_pickup_datetime) >= TO_DATE('2019-09-18', 'YYYY-MM-DD') AND
DATE(lpep_pickup_datetime) < TO_DATE('2019-09-19', 'YYYY-MM-DD')
order by lpep_pickup_datetime asc


Question 4. Largest trip for each day
Which was the pick up day with the largest trip distance Use the pick up time for your calculations.

Use either of the below queries - both gave same result


select lpep_pickup_datetime, lpep_dropoff_datetime, trip_distance from green_taxi_trips
where trip_distance = (select max(trip_distance) from green_taxi_trips)

or 

SELECT 
    DATE_TRUNC('day', lpep_pickup_datetime) AS pickup_day,
    SUM(trip_distance) AS total_trip_distance
FROM 
    green_taxi_trips
GROUP BY 
    pickup_day
ORDER BY 
    total_trip_distance DESC
LIMIT 1;


Question 5. Three biggest pick up Boroughs
Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

select tz."Borough", sum(gt.total_amount)
from green_taxi_trips gt, taxi_zone_lookup tz
where 
DATE(gt.lpep_pickup_datetime) >= TO_DATE('2019-09-18', 'YYYY-MM-DD') AND
DATE(gt.lpep_pickup_datetime) < TO_DATE('2019-09-19', 'YYYY-MM-DD') AND
tz."LocationID" = gt."PULocationID" AND 
tz."Borough" <> 'Unknown'
GROUP BY tz."Borough"
ORDER BY 2 DESC
LIMIT 3


Question 6. Largest tip
For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

Note: it's not a typo, it's tip , not trip

SELECT 
    tz_drop."Zone" AS drop_off_zone,
    MAX(gt.tip_amount) AS max_tip_amount
FROM 
    green_taxi_trips gt
JOIN 
    taxi_zone_lookup tz_pick ON gt."PULocationID" = tz_pick."LocationID"
JOIN 
    taxi_zone_lookup tz_drop ON gt."DOLocationID" = tz_drop."LocationID"
WHERE 
    tz_pick."Zone" = 'Astoria'
    AND EXTRACT(YEAR FROM gt.lpep_pickup_datetime) = 2019
    AND EXTRACT(MONTH FROM gt.lpep_pickup_datetime) = 9
GROUP BY 
    tz_drop."Zone"
ORDER BY 
    max_tip_amount DESC
LIMIT 1;