# data-pipelines-project

## Overview

This project involves building an ETL pipeline using Apache Airflow to load, transform, and run quality checks on data that is stored in Amazon Redshift. The pipeline is designed to automate the process of extracting data from JSON files in S3, staging it in Redshift, transforming it into star schema, and ensuring data quality through various checks.

## Project Structure

- DAG Configuration
- Operators
    -Stage Operator
    - Fact and Dimension Operators
    - Data Quality Operator

## Prerequisites

- Python 3
- Apache Airflow
- Amazon Redshift cluster
- AWS credentials with access to S3 and Redshift

## DAG Configuration

The DAG is configured with the following default parameters:

1. No dependencies on past runs: Ensures each DAG run is independent.
2. Retry policy: On failure, tasks are retried 3 times.
3. Retry delay: Retries happen every 5 minutes.
4. Catchup: Catchup is turned off to avoid running historical DAG runs.
5. Email on retry: Email notifications on retries are disabled.

## Building the Operators

### Stage Operator

The 'StageToRedshiftOperator' loads JSON files from S3 to Amazon Redshift. It uses the following parameters to create and run a SQL COPY statement:

- 'redshift_conn_id': Redshift connection ID.
- 'aws_credentials_id': AWS credentials ID.
- 'table': Target table in Redshift.
- 's3_bucket': S3 bucket name.
- 's3_key': S3 key prefix.
- 'region': AWS region.
- 'data_format': Format of the data (e.g., JSON path).

### Fact and Dimension Operators

These operators transform and load data into fact and dimension tables in Redshift.

### Data Quality Operator

The 'DataQualityOperator' runs checks on the data to ensure its integrity.

## Dependencies and Graph View

The task dependencies are configured to ensure the correct order of operations. The graph view follows the flow shown below:
`
Begin_execution
     |
Stage_events_to_redshift --> Load_songplays_fact_table --> Load_user_dim_table
     |                                         |                     |
Stage_songs_to_redshift                        |                     |
                                                -----------------------
                                                            |
                                                   Run_data_quality_checks
                                                            |
                                                   Stop_execution
`                                                

## Conclusion

This project demonstrates how to build a robust ETL pipeline using Apache Airflow, S3, and Redshift. The operators are designed to be reused and configurable, allowing for flexible data pipelines that can handle various data sources and transformations.


