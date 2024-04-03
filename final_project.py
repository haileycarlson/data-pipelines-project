from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
# from udacity.common import sql_statements
from helpers import SqlQueries, sql_statements

S3_BUCKET = 'udacity-dend'
S3_LOG_KEY = 'log_data/{execution_date.year}/{execution_date,month}'
S3_SONG_KEY = 'song_data'
LOG_JSON_PATH = f's3://{S3_BUCKET}/log_json_path.json'
REGION = 'us-west-2'
AWS_CREDENTIALS_ID = 'aws_crentials'
REDSHIFT_CONN_ID = 'redshift'
DAG_ID = 'dag'

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id = REDSHIFT_CONN_ID,
        aws_credentials_id = AWS_CREDENTIALS_ID,
        table = "staging_events",
        s3_bucket = S3_BUCKET,
        s3_key = S3_LOG_KEY,
        region = REGION,
        data_format = f"JSON '{LOG_JSON_PATH}'",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id = REDSHIFT_CONN_ID,
        aws_credentials_id = AWS_CREDENTIALS_ID,
        table = "staging_events",
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
        default_args = default_args,
        postgres_conn_id = REDSHIFT_CONN_ID,
        sql_queries = SqlQueries.user_table_insert,
        table = 'users',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        default_args = default_args,
        postgres_conn_id = REDSHIFT_CONN_ID,
        sql_queries = SqlQueries.song_table_insert,
        table = 'users',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        default_args = default_args,
        postgres_conn_id = REDSHIFT_CONN_ID,
        sql_queries = SqlQueries.artist_table_insert,
        table = 'artists',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        default_args = default_args,
        postgres_conn_id = REDSHIFT_CONN_ID,
        sql_queries = SqlQueries.time_table_insert,
        table = 'time',
    )
    tables = ['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time']
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        postgres_conn_id = REDSHIFT_CONN_ID,
        tests = [DataQualityOperator.no_results_test(table) for table in tables],
    )

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table
    load_user_dimension_table >> load_song_dimension_table
    load_song_dimension_table >> load_artist_dimension_table
    load_artist_dimension_table >> load_time_dimension_table
    load_time_dimension_table >> run_quality_checks


final_project_dag = final_project()