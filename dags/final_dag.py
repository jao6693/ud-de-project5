from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# https://airflow.apache.org/docs/stable/_modules/airflow/example_dags/tutorial.html
"""
default_args = {
    'owner': 'fabien',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG('final_dag',
          catchup=False,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
        )
"""
default_args = {
    'owner': 'fabien',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'email_on_retry': False,
}

dag = DAG('final_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
          )

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials='aws_credentials',
    redshift_conn_id='redshift',
    bucket='s3://udacity-dend/log_data/{}/{}/{}-{}-{}-events.json',
    table='staging_events',
    queries=SqlQueries,
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials='aws_credentials',
    redshift_conn_id='redshift',
    bucket='s3://udacity-dend/song_data/*/*/*/*.json',
    table='staging_songs',
    queries=SqlQueries,
    provide_context=True
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='user',
    queries=SqlQueries
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='song',
    queries=SqlQueries
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artist',
    queries=SqlQueries
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    queries=SqlQueries
)

"""
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)
"""
end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)


# dependencies & ordering
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> end_operator
stage_songs_to_redshift >> end_operator
