#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import time 
from kafka import KafkaProducer
from pyspark.sql import types
from time import sleep


TOPIC_NAME = 'green-trips-test1'
TEST_LOAD=True
LOG_PREFIX = 'MY_LOG '

print(LOG_PREFIX,' load green trips data to df..')
df = pd.read_csv('green_tripdata_2019-10.csv')

#look at the data
for i, row in enumerate(df.itertuples(index=False),1):
    row_dict = {col: getattr(row, col) for col in row._fields}
    print(i, row_dict, '')
    if i == 3:
        break
##        
 

#check kafka connection

print(LOG_PREFIX,'check kafka connection..')
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)
producer.bootstrap_connected()



print(LOG_PREFIX,'load data to kafka..')
##load data to kafka
def load_data():
    t0 = time.time()
    for i, row in enumerate(df.itertuples(index=False),1):
        row_dict = {col: getattr(row, col) for col in row._fields}
        #print(i, row_dict, '')
        if TEST_LOAD and i == 5:
            break
        message = {}    
        message['lpep_pickup_datetime']= row_dict['lpep_pickup_datetime']
        message['lpep_dropoff_datetime']= row_dict['lpep_dropoff_datetime']
        message['PULocationID']= row_dict['PULocationID']
        message['DOLocationID']= row_dict['DOLocationID']
        message['passenger_count']= row_dict['passenger_count']
        message['trip_distance']= row_dict['trip_distance']
        message['tip_amount']= row_dict['tip_amount']
        t1 = time.time()
        producer.send(TOPIC_NAME, value=message)
        t2 = time.time()
        #print(f'** loop time to send {(t2 - t1):.2f} seconds for index ', i )
        t1 = t2

    producer.flush()
    t3 = time.time()
    print(LOG_PREFIX,f'**end-end took {(t3-t0):.2f} seconds')    
    
load_data()

##create spark consumer
print(LOG_PREFIX,'create spark consumer...')
pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer1") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()


## read data
print(LOG_PREFIX,'read data in spark streaming...')
green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

print(LOG_PREFIX,'schema for raw data')
green_stream.printSchema()
#print(LOG_PREFIX,' green_stream ' ,green_stream)


##gracefull stop queries
def stop_stream_query(query, wait_time):
    """Stop a running streaming query"""
    while query.isActive:
        msg = query.status['message']
        data_avail = query.status['isDataAvailable']
        trigger_active = query.status['isTriggerActive']
        if not data_avail and not trigger_active and msg != "Initializing sources":
            print(LOG_PREFIX,'Stopping query...')
            query.stop()
        time.sleep(0.5)

    # Okay wait for the stop to happen
    print('Awaiting termination...')
    query.awaitTermination(wait_time)
    
    
# ## read to console
# def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
#     write_query = df.writeStream \
#         .outputMode(output_mode) \
#         .trigger(processingTime=processing_time) \
#         .format("console") \
#         .option("truncate", False) \
#         .start()
#     return write_query # pyspark.sql.streaming.StreamingQuery
    
# if TEST_LOAD:
#     print(LOG_PREFIX,' read raw data to console..')
#     query1_console = sink_console(green_stream, output_mode='append')
#     stop_stream_query(query1_console,30)
#     print(LOG_PREFIX,' query1_console(for raw data) is still active? ',query1_console.isActive)
#     query1_console.stop()

    
## look at first record
def my_peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)
    if first_row:
        print(LOG_PREFIX, batch_id, first_row[1])
    #end of func

print(LOG_PREFIX,'read raw data writeStream.foreachBatch')
query1_peek = green_stream.writeStream.foreachBatch(my_peek).start()
stop_stream_query(query1_peek,120)
print(LOG_PREFIX,'query1_peek(for raw data) is active? ',query1_peek.isActive)
        

print(LOG_PREFIX,'add schema to data...')
### add schema to data
schema = types.StructType() \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())

green_stream_withschema = green_stream \
  .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
  .select("data.*")


print(LOG_PREFIX,'schema after attaching schema to data')
green_stream_withschema.printSchema()


# print(LOG_PREFIX,'load data again')
# load_data()

# if TEST_LOAD:
#     print(LOG_PREFIX,'read data with schema to console')
#     query2_console = sink_console(green_stream_withschema, output_mode='append')
#     sleep(60)
#     print(LOG_PREFIX,'query2_console(for data with schema) is active? ',query2_console.isActive)
#     query2_console.stop()
    
# ## look at first record
# print(LOG_PREFIX,'read data with schema writeStream.foreachBatch')
# query2_peek= green_stream_withschema.writeStream.foreachBatch(my_peek).start()
# sleep(60)
# print(LOG_PREFIX,'query2_peek(for data with schema) is active? ',query2_peek.isActive)
# query2_peek.stop()