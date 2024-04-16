from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                postgres_conn_id = "",
                sql = "",
                table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.table = table


    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')

        postgres = PostgresHook(postgres_conn_id = self.postgres_conn_id)

        # self.log.info("Clearing table")
        # postgres.run("DELETE FROM {}".format(self.table))

        self.log.info(f"Loading data into the fact table {self.table}")
        postgres.run(self.sql)

        self.log.info("LoadFactOperator execution complete")
