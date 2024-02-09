-- Replace script with project id where placeholder is set up  (if schema name is different replace ny_taxi as well)
-- ensure the CSV files are loaded into  bucket nyc-tl-data-dts-2024-module3 for external table creation 
-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `<replace_with_project_id>.ny_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tl-data-dts-2024-module3/yellow_tripdata_2019-*.csv', 'gs://nyc-tl-data-dts-2024-module3/yellow_tripdata_2020-*.csv']
);

-- Check yello trip data
SELECT * FROM <replace_with_project_id>.ny_taxi.external_yellow_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE <replace_with_project_id>.ny_taxi.yellow_tripdata_non_partitoned AS
SELECT * FROM <replace_with_project_id>.ny_taxi.external_yellow_tripdata;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE <replace_with_project_id>.ny_taxi.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM <replace_with_project_id>.ny_taxi.external_yellow_tripdata;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM <replace_with_project_id>.ny_taxi.yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM <replace_with_project_id>.ny_taxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE <replace_with_project_id>.ny_taxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM <replace_with_project_id>.ny_taxi.external_yellow_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM <replace_with_project_id>.ny_taxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM <replace_with_project_id>.ny_taxi.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;