from datetime import datetime, timedelta
import pendulum
import os
import sys
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
# from udacity.common import sql_statements
from helpers import SqlQueries

# S3 bucket and path configurations
S3_BUCKET = 'udacity-dend'
S3_LOG_KEY = 'log_data'
S3_SONG_KEY = 'song_data'
LOG_JSON_PATH = f's3://{S3_BUCKET}/log_json_path.json'
REGION = 'us-west-2'
AWS_CREDENTIALS_ID = 'aws_credentials'
REDSHIFT_CONN_ID = 'redshift'

# Default arguments for the DAG
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'cathcup': False,
    'email_on_retry': False,

}

# Define the DAG
@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():
    # Define the start task
    start_operator = DummyOperator(task_id='Begin_execution')

    # Stage events data from S3 to Redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Staging_events',
        redshift_conn_id = REDSHIFT_CONN_ID,
        aws_credentials_id = AWS_CREDENTIALS_ID,
        table = "staging_events",
        s3_bucket = S3_BUCKET,
        s3_key = S3_LOG_KEY,
        region = REGION,
        data_format = "JSON 'auto'",
    )

    # Stage songs data from S3 to Redshift
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id = REDSHIFT_CONN_ID,
        aws_credentials_id = AWS_CREDENTIALS_ID,
        table = "staging_songs",
        s3_bucket = S3_BUCKET,
        s3_key = S3_LOG_KEY,
        region = REGION,
        data_format = "JSON 'auto'",
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        postgres_conn_id = REDSHIFT_CONN_ID,
        sql = SqlQueries.songplay_table_insert,
        table = 'songplays',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        postgres_conn_id = REDSHIFT_CONN_ID,
        sql = SqlQueries.user_table_insert,
        table = '"users"',
    )

    # Load data into the songplays fact table
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        postgres_conn_id = REDSHIFT_CONN_ID,
        sql = SqlQueries.song_table_insert,
        table = 'song',
    )

    # Load data into the artists dimension table
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        postgres_conn_id = REDSHIFT_CONN_ID,
        sql = SqlQueries.artist_table_insert,
        table = 'artist',
    )

    # Load data into the songs dimension table
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        postgres_conn_id = REDSHIFT_CONN_ID,
        sql = SqlQueries.time_table_insert,
        table = 'time',
    )

    # Run data quality checks
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        postgres_conn_id = REDSHIFT_CONN_ID,
        tests = [
            {"check_sql": "SELECT COUNT(*) FROM users WHERE user_id IS NULL", "expected_result": 0},
            {"check_sql": "SELECT COUNT(*) FROM song WHERE title IS NULL", "expected_result": 0},
            {"check_sql": "SELECT COUNT(*) FROM artist WHERE name IS NULL", "expected_result": 0},
            {"check_sql": "SELECT COUNT(*) FROM time WHERE month IS NULL", "expected_result": 0},
            {"check_sql": "SELECT COUNT(*) FROM songplays WHERE user_id IS NULL", "expected_result": 0}
        ]
    )

    # Define the end task
    end_operator = DummyOperator(task_id = 'Stop_execution')

    # Set task dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

# Instantiate the DAG
final_project_dag = final_project()