from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    
    """
    Copy query for staging tables
    """
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        json
        '{}'
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credential_id = "",
                 copy_sql = "",
                 s3_bucket = "",
                 s3_key = "",
                 table = "",
                 ignore_headers = 1,
                 copy_opt = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.copy_sql = copy_sql
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.ignore_headers = ignore_headers
        self.copy_opt = copy_opt
        
    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from staging table: {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift table: {}".format(self.table))
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.copy_opt
        )
        redshift.run(formatted_sql)




