## Notes and Instructions from week1

###  Prerequisites
<ol>
  <li>Folder setup</li>
  Checkout this repository
  <li>Docker Desktop</li>
  Download <a href="https://www.docker.com/products/docker-desktop/">Docker desktop</a>
  <li>Python and libraries</li>
  Install python 3.9 or higher
  Verify installation by running python --version
  <li>Anaconda to manage python environment</li>
  This is optional, but helpful to manage multiple python environments when running multiple projects
  Download <a href="https://www.anaconda.com/download">Anaconda</a>
  Create new environment "learn" 
  Add the additional dependencies sqlalchemy and psycopg2 (for connecting to postgres), pgcli, pandas
  <li>Visual studio code for development</li>
  Optional but preferred
  Download <a href="https://code.visualstudio.com/download">Visual studio code</a>
  <li>Google cloud account</li>
  Setup <a href="https://console.cloud.google.com/">google cloud account</a> to a folder called "gcp" at root of repository folder 
  Note down the project id. This will be used later. 
  Download Google cloud SDK and save it under gcp folder
  <li>Download terraform</li>
  Download <a href="https://www.terraform.io/downloads">Terraform</a> to a folder called "terraform"  at root of repository folder
  <li>Download test files</li>
  Download the below test files to week_1_basics_n_setup folder <br/>
  
  wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz   <br/>
  wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz  <br/>
  wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv  <br/>
 
</ol>

### Things we will do here
<ol>
  <li>Create Docker container</li>
  1: existing container
  simple script to run a docker container with postgres and bash entry point
  <pre>
  docker run -it --entrypoint=bash python:3.9
  </pre>
  2: docker file with postgres as image and run a simple python script (uses pipeline.py script and Dockerfile)
  <pre>
  docker build  -t test:pandas .
  docker run  -it test:pandas
  </pre>

  3: Create postgres container using below script
    <pre>
    docker run -it \
        -e  POSTGRES_USER=root \
        -e  POSTGRES_PASSWORD=root \
        -e  POSTGRES_DB=ny_taxi \
        -v  <local volume path/ny_taxi_postgres_data>:/var/lib/postgresql/data \
        -p 5432:5432 \
        postgres:13
  </pre>


  <li>Run PGCLI to view data</li>
  Run below command to view data using PGCLI admin screen
    <pre>
     pgcli -h localhost -p 5432 -u root -d ny_taxi
    </pre>
 
  Run \dt to check if any tables created (There will be none)
 
  <li>Load the data to postgres</li>
  Load the data from yellow_tripdata_2021-01.csv.gz to postgres
  Load Jupyter notebook from anaconda and run upload-data.ipynb file 
  Recheck the table in PGCLI using \dt
  run below script to see the data
  <pre>
  Select count(*) from yellow_taxi_trips
  </pre>
   
  <li>Run PDADMIN to run SQL queries</li>
  we will create pgadmin container to connect to postgres and run SQL queries
  Stop Docker container for postgres and we will restart it in network to run and connect Postgres contianer to PGADMIN contianer 
  Run these three scripts in different terminals. 
   
   docker network create pg-network 

    docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v  <local volume path/ny_taxi_postgres_data>:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13

    docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name pgadmin-2 \
    dpage/pgadmin4

   
    PGADMIN can be accessed using to see the DB and the tables. 
    http://localhost:8080/browser/
    

  <li>Load sample data to DB using python script</li>
  Drop table we created we can use below script to run the python script that does same thing as jupyter notebook file. 

  <pre>
         URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    python ingest_data.py \
      --user=root \
      --password=root \
      --host=localhost \
      --port=5432 \
      --db=ny_taxi \
      --table_name=yellow_taxi_trips \
      --url=${URL}
 </pre>
  

   
  <li>Putting everything together</li>
     Navigate to week_1_basics_n_setup
     Run conda script to activate the environment conda activate learn 
     The docker-compose file contains code to create a postgres container and PGADMIN
     Verify the volume location where data is mapped locally
     Run docker compose file using below
  <pre> docker-compose up </pre> 
     This will create the postgres DB and start PGADMIN. PGADMIN can be accessed using below to see the DB
  <pre> http://localhost:8080/browser/
  </pre> 
      
     In another terminal start python http servier as 
  <pre> python -m http.server  
  </pre>  
     This will serve files under 
  <pre> http://<yourip:8000> 
  </pre>   
     Next we will run the ingest_data.py file to run and load the data files. 
     Rerun the script by changing the csv file name and table name to load the 3 CSV files we are working on .
    
     URL="http://<yourip:8000>/yellow_tripdata_2021-01.csv.gz"
    python ingest_data.py \
      --user=root \
      --password=root \
      --host=localhost \
      --port=5432 \
      --db=ny_taxi \
      --table_name=yellow_taxi_trips \
      --url=${URL}
 
     The below URL is accessible because we started python http server and serving the data through this link 
  
  
      URL="http://<yourip:8000>/green_tripdata_2019-09.csv"

      python ingest_data.py \
        --user=root \
        --password=root \
        --host=localhost \
        --port=5432 \
        --db=ny_taxi \
        --table_name=green_taxi_trips \
        --url=${URL}
      
      URL="http://<yourip:8000>/taxi+_zone_lookup.csv"

      python ingest_data.py \
        --user=root \
        --password=root \
        --host=localhost \
        --port=5432 \
        --db=ny_taxi \
        --table_name=taxi_zone_lookup \
        --url=${URL}
     
        you can also use the WGET url instead of local host URL
        
        Run the homework SQL queries using <a href="https://github.com/amohan601/dataengineering-zoomcamp2024/blob/main/week_1_basics_n_setup/sql-scripts.sql">SQL scripts here</a>
  <li>Google cloud account set up with credential key file</li>
  Set up account following the vidoes and create service account and add roles. Then generate key and save it under gcp folder
  <li>Terraform setup with variables to generate resources in google cloud</li>
  <pre>
  export PATH=$PATH:"/path_to_google_sdk_bin_folder"
  export PATH=$PATH:"/path_to_terraform"
  export GOOGLE_APPLICATION_CREDENTIALS="/path_to_api_creds_json_file"
  </pre>
  Run this to authenticate the user 
  <pre>
  gcloud auth application-default login
  </pre>
  
  navigate to terraform_basic folder edit the main.tf and replace <my-project-id> with project id
  run terraform init  terraform plan and finally terraform apply This should create the resourcds in GCP. Run terraform destroy once verified. 
  Perform the same operations in terraform_with_variables folder. 

  
</ol>

