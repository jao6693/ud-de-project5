![Sparkify](https://github.com/jao6693/ud-de-project5/blob/master/img/sparkify_logo.png?raw=true)

# Sparkify Analytics 

<b>Sparkify</b> is an international media services provider  
It's primary business is to build an <b>audio streaming</b> platform that provides music, videos and podcasts from record labels and media companies  

## Challenge

Sparkify wants to better serve its users and thus needs to analyze the data collected on songs and user activity on their music streaming app. The analytical goal is to understand what songs users are listening to  

## Architecture 

The data is currently stored as JSON files and resides on AWS in a S3 bucket (logs on user activity as well as metadata on the songs). This architecture doesn't provide an easy way to directly query the data  

## Analytics goals 

Sparkify wants to leverage its cloud investment and use highly scalable components to support the analytical queries. They chose to use Amazon Redshift as intermediate and final data storage solution.  

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to 

They also decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.  

The main idea is to create high grade data pipelines that:
* are dynamic and built from reusable tasks  
* can be monitored
* allow easy backfills  

They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets  

## Design considerations

The analytical model is designed the following way:  
* one fact table called `songplays` contains facts (like level, location, user_agent, ...)
* four dimension tables called `users`, `songs`, `artists`, `time` contain additional information on songs, artists, users, ...

These tables are connected according to the following <b>star schema</b>:  

![analytic model representation](https://github.com/jao6693/ud-de-project5/blob/master/img/analytic_model.png?raw=true)

With this model, analysts are able to easily query informations directly from tables or to use BI tools on top of it  

The Data Pipeline is designed the following way:  
* in the first stage (divided in 2 parallel steps) data is extracted from S3 buckets and staged into Redshift  
* in the second stage the data is loaded from the staging tables into the <b>fact table</b>
* in the third stage (divided in 4 parallel steps) the data is loaded from the staging tables into the <b>dimension tables</b>
* in the fourth and final stage (divided in 5 parallel steps) some quality checks are performed to ensure that every previous step has been successfully completed  

At the end, the Data Pipeline looks like this:  

![data pipeline representation](https://github.com/jao6693/ud-de-project5/blob/master/img/data_pipeline.png?raw=true)

## Files & Configuration

The following configuration must be set in order for the DAG to be executed successfully:  
* create a connection named `aws_credentials` of type `Amazon Web Services` to store your own access_key and secret  
* create a connection named `redshift` of type `PostgreSQL` to store your credentials to access Redshift  

The following files are available within the project
* `dags>final_dag.py`:  
This file is the main file of the project  
It contains the DAG to be executed, as well as the different tasks used within the DAG  

* `plugins>helpers>sql_queries.py`:  
This file contains all the SQL DDL & DML  
The SQL transformations & movements are there  

* `plugins>operators`:  
This folder contains all the custom operators used in the Data Pipeline  

* `plugins>operators>stage_redshift.py`:  
This file is the custom operator that copies data from S3 to Redshift  (staging area)  

* `plugins>operators>load_fact.py`:  
This file is the custom operator that combines data from events and songs to populate the <b>fact table</b>  

* `plugins>operators>load_dimension.py`:  
This file is the custom operator that retrieves data from events and/or songs to populate the <b>dimension table</b>  

* `plugins>operators>data_quality.py`:  
This file is the custom operator that connects to Redshift once all the data transformation & movements are completed, to ensure that the content of the different tables is as expected  

## Airflow

The DAG can be executed from the Airflow UI, once Airflow is installed  
