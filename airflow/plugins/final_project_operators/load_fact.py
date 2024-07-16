from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Operator to stage data from S3 to Redshift.
    """
    # UI color for the Airflow web interface
    ui_color = '#358140'

    # Template fields for dynamic rendering
    template_fields = ("s3_key",)

    # SQL COPY command template
    copy_sql = """ 
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "", # Redshift connection ID
                 aws_credentials_id = "", # AWS credentials ID
                 table = "", # Target table in Redshift
                 s3_bucket = "", # S3 bucket name
                 s3_key = "", # S3 key prefix
                 region = "", # AWS region
                 data_format = "", # Data format (e.g., JSON, SCV)
                 *args, **kwargs):
        """
        Initialize the StageToRedshiftOperator.
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.data_format = data_format

    def execute(self, context):
        """
        Execute the data staging process.
        """
        self.log.info('Executing StageToRedshiftOperator')

        # Get AWS credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        # Establish connection to the Redshift database
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        # Clear the target table in Redshift
        self.log.info("Clearing data from Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        # Copy data from S3 to Redshift
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.data_format
        )
        redshift.run(formatted_sql)




