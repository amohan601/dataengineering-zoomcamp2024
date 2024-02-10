
Set up google cloud login for local.
```
    export PATH=$PATH:"/path_to_google_sdk_bin_folder"

    export GOOGLE_APPLICATION_CREDENTIALS="/path_to_api_creds_json_file"
    
    gcloud auth application-default login

```

Load the generated model from bigquery ML into google storage

```
    bq --project_id dtc-de-course-411602 extract -m ny_taxi.tip_model gs://nyc-tl-data-dts-2024-module3/taxi_ml_model/tip_model
```
Make a directory locally and copy the remote gcs model to our directory.
```
    mkdir taxi_model
    gsutil cp -r gs://nyc-tl-data-dts-2024-module3/taxi_ml_model/tip_model taxi_model
```

Create a serving directory to copy tip_model to that directory.
```
    mkdir -p serving_dir/tip_model/1
    cp -r taxi_model/tip_model/*  serving_dir/tip_model/1
```

Pull tensorflow docker image to serve the model. 

```
    docker pull tensorflow/serving
    docker run -p 8501:8501 --mount type=bind,source=pwd/serving_dir/tip_model,target= /models/tip_model -e MODEL_NAME=tip_model -t tensorflow/serving &
```

Test the prediction using the URL and payload below. 
```
    curl -d '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}' -X POST http://localhost:8501/v1/models/tip_model:predict
    http://localhost:8501/v1/models/tip_model

```