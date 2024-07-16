from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Operator to perform data quality checks on specified tables.
    """
    #UI color for the Airflow web interface
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id = "", # Redshift/Postgres connection ID
                 tables = [], # List of tables to check
                 sql_checks = [], # List of SQL checks to perform
                 *args, **kwargs):
        """
        Initialize the DataQualityOperator.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.tables = tables
        self.sql_checks = sql_checks

    def execute(self, context):
        """
        Execute the data quality checks.
        """
        self.log.info('DataQualityOperator not implemented yet')

        # Establish connection to the Postgres/Redshift database
        postgres = PostgresHook(self.postgres_conn_id)
        
        # Iterate over tables and corresponding SQL checks
        for table, sql_check in zip(self.tables, self.sql_checks):
            self.log.info(f"Performing data quality check on table {table}")
            records = postgres.get_records(sql_check)

            # Check if the SQL query returned any results
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed for table {table}. No results returned")
            elif records[0][0] < 1:
                raise ValueError(f"Data quality check passed for table {table}")
            else:
                self.log.info(f"Data quality check passed for table {table}")
            
        
        self.log.info("DataQualityOperator execution complete")