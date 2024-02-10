## Code, Homework, Notes and Instructions from DataEngineering zoomcamp week3

[DataEngineering zoomcamp week3](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse) 
<br/>

### 1. Bigquery using yellow trip data
Manually download the yellow taxi files and load it to GCS bucket nyc-tl-data-dts-2024-module3 with format of file name as yellow_tripdata_2020-*.csv

To learn Bigquery tables, partioning, and clustering use the scripts from [Yellow data scripts](./db_scripts/yellow_taxi_data_bigquery.sql) to run in bigquery. Ensure to replace project id before running. 
 

The Parquet files loaded into bucket is from [here](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow)

The scripts to create

### 2. Bigquery and ML on yellow trip data
To learn Bigquery and ML usage use the scripts [Yellow taxi bigquery & ml scripts](./db_scripts/yellow_taxi_data_bigquery_ml.sql)
to run in bigquery and generate a model. <br/>

Bigquery can be used used to train a linear regression model on yellow taxi data. We are psasing tip_amount as target label. The goal of ML training is to predict the tip_amount for a ride. it uses training, evaluation, and test data split from this dataset to perform training, evaluation of trained model, and testing the model. Once a model is created hyperparameter tuning is done again and retrained. 

Once the model is generated it can be deployed in docker container for performing prediction. Scripts to perform that is 
[here](./bigquery_ml_deployment/big_query_ml.md)



### 3.  Homework for week 3

Instructions for homework for week is [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/03-data-warehouse/homework.md).

#### Step 1 
Ensure bucket is created. Use mage to start the container.
<br/>
Run the mage script shared [here](./mage_scripts/green_taxi_2022_v2.py) to load the parquet files to the bucket. Pass project id as mage argument variable project_id

```
docker compose build
docker compose up
```

When using parquet files, I used pyarrow library to load the pandas dataframe to parquet table. By default parquet files having datetime fields are not automatically converted to timestamp by bigquery. Hence I set explict keyword argument to indicate the datetime columns to be set to int64 so bigquery can interpret and convert them to timestamp on BQ side. 

Reference: </br>
[Pyarrow parquet write_table documentation](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html#pyarrow.parquet.write_table)


[Bigquery parquet conversion](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#parquet_conversions)

#### Step 2 (Homework scripts)
Run the SQL scripts for green taxi from 
[here](./db_scripts/green_taxi_data_bigquery.sql) 


