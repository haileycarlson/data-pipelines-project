from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Operator to load data into a dimension table in Redshift.
    """
    # UI color for the Airflow web interface
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                postgres_conn_id = "", # Redshift/Postgres connection ID
                sql = "", # SQL query to insert data into the dimension table
                table = "", # Dimension table to load data into
                 *args, **kwargs):
        """
        Initialize the LoadDimensionOperator
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.table = table

    def execute(self, context):
        """
        Execute the data loading process.
        """
        self.log.info('LoadDimensionOperator not implemented yet')

        # Establish connection to the Postgres/Redshift database
        postgres = PostgresHook(postgres_conn_id = self.postgres_conn_id)

        # Clear the target dimension table
        self.log.info("Clearing table")
        postgres.run("DELETE FROM {}".format(self.table))

        # Load data into the dimension table
        self.log.info(f"Loading data into the fact table {self.table}")
        postgres.run(self.sql)

        self.log.info("LoadFactOperator execution complete")