## Code, Homework, Notes and Instructions from DataEngineering zoomcamp week5

[DataEngineering zoomcamp week5](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/05-batch) 
<br/>

Following along week 5 videos to set up spark. 

### Spark
With anaconda and Mac I did the spark set by first installing pyspark and then for environment variable set up findspark and wget for examples

Open anaconda
Activate the environment where you want to apply these changes
1. Run pyspark and install it as a package in this environment <br/>
2. Run findspark and install it as a package in this environment<br/>
3. Optionally if needed run wget to run wget queries on notebook.

Ensure that openJDK is already set up. This allowed me to not having to install spark seperately and manually setting up the environment
Also with this I had to use Jupyter Lab (instead of Jupter notebook) to open a jupyter notebook for running the programs
Once the spark is set up start the conda environment and open Jupyter Lab. 
Run the program [CheckPySparkVersion](./CheckPySparkVersion.ipynb) to check everything is running fine.

### Examples

1. Execute a simple example for testing how to download a CSV file and load it to spark is here for FHVHV data. [PySparkDownloadCSV](./PySparkDownloadCSV.ipynb) 

2. Execute a program to partition spark dataframe for FHVHV data [PySparkPartitionExample](./PySparkPartitionExample.ipynb) 

3. Execute a shell script to run [download_taxi_data.sh](./download_taxi_data.sh) and download yellow and green taxi data
```
./download_taxi_data.sh yellow 2020
./download_taxi_data.sh yellow 2021
./download_taxi_data.sh green 2020
./download_taxi_data.sh green 2021
```
4. Execute a program to load the downloaded files to spark dataframe and repartition and save back to parquet. [TaxiDataCSVToParquetSpark](./TaxiDataCSVToParquetSpark.ipynb) 

5. Execute a program to understand about how to run select, filter on spark data frame and how to generate SQL like queries and run it for yellow and green taxi data. Also perform a union all of two tables and run a SQL query that we ran in DBT assignment in previous week [TaxiDataParquetSQLQueryTable](./TaxiDataParquetSQLQueryTable.ipynb) 

### Homework

For homework we need FHV data for 2019. Load it using [download_taxi_data.sh](./download_taxi_data.sh)
```
./download_taxi_data.sh fhv 2019
```

Run the program FHVSparkprocessing](./FHVSparkprocessing.ipynb) to load it to spark dataframe and execute queries for homework assignments. While loading the dataframe only the month of october is loaded. 
