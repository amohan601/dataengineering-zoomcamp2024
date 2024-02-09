## Code, Homework, Notes and Instructions from DataEngineering zoomcamp week3

Datawarehouse for week3 <a href="https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse">DataEngineering zoomcamp week3</a>

<br/>

### Bigquery and yellow trip data
To learn Bigquery tables, partioning, and clustering use the scripts from <a href="https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse">Yellow data scripts</a> to run in bigquery

The Parquet files loaded into bucket is from  <a href="https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow">here</a>

### Bigquery and ML
To learn Bigquery and ML usage use the scripts <a href="./db_scripts/">Yellow data scripts</a> to run in bigquery and generate a model <br/>

Once the model is generated it can be deployed in docker container for performing prediction

### Loading the data from CSV files in GCS to BigQuery


#### Step 1 
Ensure bucket is created. Use mage to start the container.
<br/>
Run the mage script shared <a href="./mage_scripts/">here</a> to load the parquet files to the bucket. 

```
docker compose build
docker compose up
```

When using parquet files, I used pyarrow library to load the pandas dataframe to parquet table. By default parquet files having datetime fields are not automatically converted to timestamp by bigquery. Hence I set explict keyword argument to indicate the datetime columns to be set to int64 so bigquery can interpret and convert them to timestamp on BQ side. 

Reference: </br>
<a href="https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html#pyarrow.parquet.write_table">Pyarrow parquet write_table documentation</a>

<a href="https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#parquet_conversions">Bigquery parquet conversion</a>

#### Step 2 (Homework scripts)
Run the SQL scripts for green taxi from <a href="./db_scripts/">here</a>  <br/>  

