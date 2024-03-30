## prerequisites
set up apache spark and docker following [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/macos.md) <br/>
start the conda environment as
```
conda activate dtezoomcamp
```

### Question 1 Redpanda version
to start kafka using redpanda
```
docker compose up -d 
```
Use docker ps to get the id of the tasks running

To go inside docker shell use below. Replace with the ID of red panda (do not take redpanda console container task). We can view red panda version using below. 
```
docker exec -it d4b8a0761fd0 rpk help
docker exec -it d4b8a0761fd0 redpanda --version
```

Below is the version
v23.2.26 - 328d83a06e743eaa53773b1246c260bdfd3e88b1

### Question 2 Creating a topic

Use below to create topic
```
docker exec -it d4b8a0761fd0 rpk topic create --help
docker exec -it d4b8a0761fd0 rpk topic create test-topic -c cleanup.policy=compact -r 1 -p 2
```
Output is 
TOPIC       STATUS
test-topic  OK

Run below to verify
```
docker exec -it d4b8a0761fd0 rpk topic describe test-topic
```
SUMMARY <br/>
NAME        test-topic <br/>
PARTITIONS  2 <br/>
REPLICAS    1 <br/>

### Question 3 Connecting to the Kafka server

Install below package in environment
```
pip install kafka-python
```
Run the part 1 of program [here](./ConnecToRedPanda-Kafka-Test.ipynb) to check connectivity Output is True

### Question 4 Sending test message
Run the part 2 of the program [here](./ConnecToRedPanda-Kafka-Test.ipynb) The time taken is mostly in sending message and flushing do not take time. Cumulative for entire loop takes around 0.52 seconds or so. 

Sending the messages again and watch the consumer client using below

```
docker exec -it d4b8a0761fd0 rpk topic consume test-topic
```

### Question 5 Create green trips data topic and send messages
To create the topic
```
docker exec -it d4b8a0761fd0 rpk topic create green-trips -c cleanup.policy=compact -r 1 -p 2
```
Run the program [here](./GreenTripsKafka.ipynb)
This will load green trips data to topic, view the binary data, attach schema and view the data again. 

load data time taken for green-trips **end-end took 64.59 seconds

Before attaching schema <br/>

0 Row(key=None, value=bytearray(b'{"lpep_pickup_datetime": "2019-10-01 00:26:02", "lpep_dropoff_datetime": "2019-10-01 00:39:58", "PULocationID": 112, "DOLocationID": 196, "passenger_count": 1.0, "trip_distance": 5.88, "tip_amount": 0.0}'), topic='green-trips2', partition=0, offset=0, timestamp=datetime.datetime(2024, 3, 30, 17, 38, 8, 323000), timestampType=0)

After attaching schema <br/>
0 Row(lpep_pickup_datetime='2019-10-01 00:26:02', lpep_dropoff_datetime='2019-10-01 00:39:58', PULocationID=112, DOLocationID=196, passenger_count=1.0, trip_distance=5.88, tip_amount=0.0)