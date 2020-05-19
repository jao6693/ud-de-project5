from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# https://airflow.apache.org/docs/stable/_modules/airflow/example_dags/tutorial.html
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
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
          )

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

# tasks
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials='aws_credentials',
    redshift_conn_id='redshift',
    bucket='s3://udacity-dend/log_data/{}/{}/{}-{}-{}-events.json',
    table='staging_events',
    queries=SqlQueries,
    json_format='s3://udacity-dend/log_json_path.json',
    timestamped=True,
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials='aws_credentials',
    redshift_conn_id='redshift',
    bucket='s3://udacity-dend/song_data/A/A/A',
    table='staging_songs',
    queries=SqlQueries,
    json_format='auto',
    timestamped=False,
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    queries=SqlQueries
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    queries=SqlQueries,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    queries=SqlQueries,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    queries=SqlQueries,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    queries=SqlQueries,
    truncate=True
)

end_load_operator = DummyOperator(
    task_id='Stop_loading',
    dag=dag
)

run_songplay_quality_checks = DataQualityOperator(
    task_id='Run_songplay_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays'
)

run_artist_quality_checks = DataQualityOperator(
    task_id='Run_artist_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists'
)

run_song_quality_checks = DataQualityOperator(
    task_id='Run_song_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs'
)

run_user_quality_checks = DataQualityOperator(
    task_id='Run_user_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='users'
)

run_time_quality_checks = DataQualityOperator(
    task_id='Run_time_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='time'
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

# dependencies & ordering
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> end_load_operator
load_song_dimension_table >> end_load_operator
load_artist_dimension_table >> end_load_operator
load_time_dimension_table >> end_load_operator

end_load_operator >> run_songplay_quality_checks
end_load_operator >> run_artist_quality_checks
end_load_operator >> run_song_quality_checks
end_load_operator >> run_user_quality_checks
end_load_operator >> run_time_quality_checks

run_songplay_quality_checks >> end_operator
run_artist_quality_checks >> end_operator
run_song_quality_checks >> end_operator
run_user_quality_checks >> end_operator
run_time_quality_checks >> end_operator
