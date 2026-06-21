from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import json

## Retry config so the flaky NASA API doesn't fail the whole run
default_args = {
    "retries": 5,
    "retry_delay": pendulum.duration(seconds=30),
}

## Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    schedule='@daily',
    catchup=False,
    default_args=default_args
) as dag:

    ## step 1: Create the table if it doesnt exists
    @task
    def create_table():
        ## initialize the Postgreshook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        ## SQL query to create the table
        create_table_query="""
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        ## Execute the table creation query
        postgres_hook.run(create_table_query)


    ## Step 2: Extract the NASA API Data(APOD)-Astronomy Picture of the Day[Extract pipeline]
    ## https://api.nasa.gov/planetary/apod?api_key=Demo_KEY
    extract_apod=HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',  ## Connection ID Defined In Airflow For NASA API
        endpoint='planetary/apod', ## NASA API enpoint for APOD
        method='GET',
        data={"api_key":"{{ conn.nasa_api.extra_dejson.api_key}}"}, ## USe the API Key from the connection
        response_filter=lambda response:response.json(), ## Convert response to json
        extra_options={"timeout": 25}, ## fail fast on hung connections so retries kick in sooner
    )


    ## Step 3: Transform the data(Pick the information that i need to save)
    @task
    def transform_apod_data(apod_data):
        ## Extract the required fields from the API response
        apod_data={
            "title":apod_data.get("title", ' '),
            "explanation":apod_data.get("explanation", ' '),
            "url":apod_data.get("url", ' '),
            "date":apod_data.get("date", ' '),
            "media_type":apod_data.get("media_type", ' ')
        }
        return apod_data

    ## step 4:  Load the data into Postgres SQL
    @task
    def load_apod_data(apod_data):
        ## initialize the Postgreshook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        ## SQL query to insert data
        insert_query="""
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s)
        """
        ## Execute the insert query
        postgres_hook.run(insert_query, parameters=(
            apod_data["title"],
            apod_data["explanation"],
            apod_data["url"],
            apod_data["date"],
            apod_data["media_type"]
        ))


    ## step 5: Verify the data DBViewer


    ## step 6: Define the task dependencies
    ## Extract
    create_table()>> extract_apod ##Ensure the table is created before extracting data
    api_response=extract_apod.output ## Get the output of the API call
    
    ## Transform 
    transformed_data=transform_apod_data(api_response)
    
    ## Load
    load_apod_data(transformed_data) ## Load the transformed data into Postgres
