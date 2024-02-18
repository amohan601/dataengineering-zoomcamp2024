## Code, Homework, Notes and Instructions from DataEngineering zoomcamp week4

[DataEngineering zoomcamp week4](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering) 
<br/>

Following along week 4 videos I set up dbt in cloud using Option A discussed in the link above. 

### DBT
I created DBT project as a seperate repo [DBT learning repo](https://github.com/amohan601/dataengineering-dbt)

The repo contains the scripts to create the model in video tutorial and also the model needed for homework for this week. 

#### Homework
The [DBT learning repo](https://github.com/amohan601/dataengineering-dbt)  creates a stage model for green and yellow data data. It also creates model for fhv data. The fact is then created by unionizing all the green, yellow, fhv data
Then it is joined with dim_zone data to generate the data
Each union object is identified by giving service type as 'Green', 'Yellow', 'FHV' and identified accordingly.

The DBT repo is a seperate repo created to allow merge and commit from DBT cloud. 

The looker studio report for DBT [PDF Visual report](/full_trips.pdf)

