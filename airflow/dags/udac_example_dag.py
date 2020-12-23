"""
Prerequisites: 

1)Make sure to create tables in AWS Redshift using queries from /airflow/create_tables.sql

This Dag runs daily and loads data from S3 to Redshift in following order:

1) Copy Data from S3 to Staging Redshift Tables
2) Load Fact Table
3) Load Dimension Tables
4) Perform DQ Checks

Customer Operators created and used for this DAG are:

1) StageToRedshiftOperator
2) LoadDimensionOperator
3) LoadFactOperator
4) DataQualityOperator

Insert Queries for Facts and Dimensions are located under /airflow/plugins/helpers/sql_queries.py

"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadDimensionOperator,
                               LoadFactOperator, DataQualityOperator)
from helpers import SqlQueries

#Default args for DAG
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime.today()-timedelta(hours=48),
}

#Dag Definition 
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          template_searchpath=['/home/workspace/airflow/dags'],
          schedule_interval='0 7 * * *',
          catchup=False
        )
 
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


#Load Songs table in staging
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="stage_songs",
    redshift_conn_id="redshift",
    aws_credential_id="airflow_redshift_user",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    table="staging_songs",
    copy_opt="auto",
    dag=dag
)

#Load Events table in staging
stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    redshift_conn_id="redshift",
    aws_credential_id="airflow_redshift_user",
    s3_bucket="udacity-dend",
    s3_key="log_data/"+'{{macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y/%m")}}'+"/"+'{{macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y-%m-%d")}}'+"*",
    table="staging_events",
    copy_opt="s3://udacity-dend/log_json_path.json",
    dag=dag
)

#Load songs dimension table
load_songs_dimension_table = LoadDimensionOperator(
    task_id="Load_songs_dim_table",
    target_table="songs",
    redshift_conn_id="redshift",
    dag=dag
)
 
#Load users dimension table
load_users_dimension_table = LoadDimensionOperator(
    task_id="Load_users_dim_table",
    target_table="users",
    redshift_conn_id="redshift",
    dag=dag
)

#Load artists dimension table
load_artists_dimension_table = LoadDimensionOperator(
    task_id="Load_artists_dim_table",
    target_table="artists",
    redshift_conn_id="redshift",
    dag=dag
)

#Load time dimension table
load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    target_table="time",
    redshift_conn_id="redshift",
    dag=dag
)

#Load songplays fact table
load_songplays_fact_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    fact_sql="fact_sql",
    target_table="songplays",
    redshift_conn_id="redshift",
    dag=dag
)

#Data quality check for songs table
run_songs_data_quality_checks = DataQualityOperator(
    task_id="songs_data_quality_checks",
    table="songs",
    redshift_conn_id="redshift",
    dag=dag
)

#Data quality check for users table
run_users_data_quality_checks = DataQualityOperator(
    task_id="users_data_quality_checks",
    table="users",
    redshift_conn_id="redshift",
    dag=dag
)

#Data quality check for artists table
run_artists_data_quality_checks = DataQualityOperator(
    task_id="artists_data_quality_checks",
    table="artists",
    redshift_conn_id="redshift",
    dag=dag
)

#Data quality check for time table
run_time_data_quality_checks = DataQualityOperator(
    task_id="time_data_quality_checks",
    table="time",
    redshift_conn_id="redshift",
    dag=dag
)
 
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
 
#Setup Dependency
start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift
stage_songs_to_redshift >> load_songplays_fact_table
stage_events_to_redshift >> load_songplays_fact_table
load_songplays_fact_table >> load_songs_dimension_table
load_songplays_fact_table >> load_users_dimension_table
load_songplays_fact_table >> load_artists_dimension_table
load_songplays_fact_table >> load_time_dimension_table
load_songs_dimension_table >> run_songs_data_quality_checks
load_artists_dimension_table >> run_artists_data_quality_checks
load_users_dimension_table >> run_users_data_quality_checks
load_time_dimension_table >> run_time_data_quality_checks
run_songs_data_quality_checks >> end_operator
run_artists_data_quality_checks >> end_operator
run_users_data_quality_checks >> end_operator
run_time_data_quality_checks >> end_operator
