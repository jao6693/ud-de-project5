from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator, SubDagOperator
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

# DAG factory for subDAGs (dimension tables & data quality)


def load_dimension_table_subdag(parent_dag_name, child_dag_name, args, tables):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
    )
    with dag_subdag:
        for table in tables:
            task = LoadDimensionOperator(
                task_id=f'load_{table}_dimension_table',
                default_args=args,
                dag=dag_subdag,
                redshift_conn_id='redshift',
                table=table,
                queries=SqlQueries,
                truncate=True
            )

    return dag_subdag


def load_data_quality_subdag(parent_dag_name, child_dag_name, args, tables):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
    )
    with dag_subdag:
        for table in tables:
            task = DataQualityOperator(
                task_id=f'Run_{table}_data_quality_check',
                default_args=args,
                dag=dag_subdag,
                redshift_conn_id='redshift',
                table=table
            )

    return dag_subdag


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

load_dimension_tables = SubDagOperator(
    task_id='Load_dimension_tables',
    subdag=load_dimension_table_subdag('final_dag', 'Load_dimension_tables', default_args, [
                                       'users', 'songs', 'artists', 'time']),
    default_args=default_args,
    dag=dag,
)

run_quality_checks = SubDagOperator(
    task_id='Run_data_quality_checks',
    subdag=load_data_quality_subdag('final_dag', 'Run_data_quality_checks',
                                    default_args, ['songplays', 'users', 'songs', 'artists', 'time']),
    default_args=default_args,
    dag=dag,
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

# dependencies & ordering
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_dimension_tables

load_dimension_tables >> run_quality_checks

run_quality_checks >> end_operator
