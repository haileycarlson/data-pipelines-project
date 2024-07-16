from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id = "",
                 tables = [],
                 sql_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.tables = tables
        self.sql_checks = sql_checks

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')

        postgres = PostgresHook(self.postgres_conn_id)
        
        for table, sql_check in zip(self.tables, self.sql_checks):
            self.log.info(f"Performing data quality check on table {table}")
            records = postgres.get_records(sql_check)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed for table {table}. No results returned")
            elif records[0][0] < 1:
                raise ValueError(f"Data quality check passed for table {table}")
            else:
                self.log.info(f"Data quality check passed for table {table}")
            
        
        self.log.info("DataQualityOperator execution complete")