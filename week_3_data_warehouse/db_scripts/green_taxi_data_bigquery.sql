-- Replace script with project id where placeholder is set up  (if schema name is different replace ny_taxi as well)
-- ensure the Parquet files are loaded into  bucket nyc-tl-data-dts-2024-module3 under prefix green_taxi for external table creation 


-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `<REPLACE_PROJECT_ID_HERE>.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://nyc-tl-data-dts-2024-module3/green_taxi/green_tripdata_2022-*.parquet', 'gs://nyc-tl-data-dts-2024-module3/green_taxi/green_tripdata_2022-*.parquet']
);

--count of records from external table 840402
select count(*) from <REPLACE_PROJECT_ID_HERE>.ny_taxi.external_green_tripdata; 

-- Check green trip data
SELECT * FROM <REPLACE_PROJECT_ID_HERE>.ny_taxi.external_green_tripdata limit 10;

-- Create a non partitioned table from external table (BigQuery table)
CREATE OR REPLACE TABLE <REPLACE_PROJECT_ID_HERE>.ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM <REPLACE_PROJECT_ID_HERE>.ny_taxi.external_green_tripdata;

--count of records from non partitioned table 840402 estimated to process: 0B
--**  Question 1: What is count of records for the 2022 Green Taxi Data??
select count(*) from <REPLACE_PROJECT_ID_HERE>.ny_taxi.green_tripdata_non_partitoned; 

--** Question 2 Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
--estimated 258 records 0B estimated
select count(distinct PULocationID) from <REPLACE_PROJECT_ID_HERE>.ny_taxi.external_green_tripdata; 
--estimated 258 records 6.41MB estimated
select count(distinct PULocationID) from <REPLACE_PROJECT_ID_HERE>.ny_taxi.green_tripdata_non_partitoned; 

--** Question 3 How many records have a fare_amount of 0? 1622 records
select count(*) from <REPLACE_PROJECT_ID_HERE>.ny_taxi.green_tripdata_non_partitoned
where fare_amount = 0; 

--**Question 4 
-- Create a partitioned table from external table
CREATE OR REPLACE TABLE <REPLACE_PROJECT_ID_HERE>.ny_taxi.green_tripdata_partitoned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM <REPLACE_PROJECT_ID_HERE>.ny_taxi.external_green_tripdata;


-- Create a partitioned and clustered table from external table
CREATE OR REPLACE TABLE <REPLACE_PROJECT_ID_HERE>.ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM <REPLACE_PROJECT_ID_HERE>.ny_taxi.external_green_tripdata;


--** Question 5 Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. 
-- Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. 
-- What are these values?

--query non partioned table estimated 12.82MB
select count(distinct PULocationID) from <REPLACE_PROJECT_ID_HERE>.ny_taxi.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

--query partioned table estimated 1.12MB
select count(distinct PULocationID) from <REPLACE_PROJECT_ID_HERE>.ny_taxi.green_tripdata_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

--Question8
-- 0MB estimated
select count(*) from <REPLACE_PROJECT_ID_HERE>.ny_taxi.external_green_tripdata WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
--6.41MB estimated
select count(*) from <REPLACE_PROJECT_ID_HERE>.ny_taxi.green_tripdata_non_partitoned WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'; 
-- 576 KB estimated
select count(*) from <REPLACE_PROJECT_ID_HERE>.ny_taxi.green_tripdata_partitoned WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'; 
-- 576 KB estimated
select count(*) from <REPLACE_PROJECT_ID_HERE>.ny_taxi.green_tripdata_partitoned_clustered WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'; 



