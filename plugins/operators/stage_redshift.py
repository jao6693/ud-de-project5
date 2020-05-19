from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials,
                 redshift_conn_id,
                 bucket,
                 table,
                 queries,
                 json_format,
                 timestamped,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.credentials = aws_credentials
        self.conn_id = redshift_conn_id
        self.bucket = bucket
        self.table = table
        self.queries = queries
        self.json_format = json_format
        self.timestamped = timestamped

    def execute(self, context):
        """
        """
        self.log.info(
            f'Starting StageToRedshiftOperator for table {self.table}')
        # get current context
        execution_date = context.get('execution_date')
        self.log.info(f'Execution date {execution_date}')
        # get AWS credentials from airflow key store
        aws_hook = AwsHook(self.credentials)
        self.log.info('Get AWS credentials')
        credentials = aws_hook.get_credentials()

        # get Redshift connection string
        redshift_hook = PostgresHook(self.conn_id)

        # create the corresponding table on Redshift
        self.log.info(
            f'Run CREATE statement for {self.table} table from helper class')
        # the query is in the helper class
        query = f'{self.table}_table_create'
        sql_statement = getattr(self.queries, query)
        self.log.info(sql_statement)
        redshift_hook.run(sql_statement)

        s3_bucket = self.bucket
        if self.timestamped == True:
            # load timestamped files from S3 based on the execution time and run backfills
            s3_bucket = s3_bucket.format(execution_date.year,
                                         execution_date.month,
                                         execution_date.year,
                                         execution_date.month,
                                         f'{execution_date.day:02d}')

        self.log.info(s3_bucket)

        # copy S3 files into Redshift
        if self.json_format == 'auto':
            self.log.info(
                f'Run COPY statement for {self.table} from helper class')
            sql_statement = getattr(self.queries, 'COPY_SQL_AUTO') \
                .format(self.table, s3_bucket, credentials.access_key, credentials.secret_key)
        else:
            self.log.info(
                f'Run COPY statement for {self.table} from helper class')
            sql_statement = getattr(self.queries, 'COPY_SQL_FORMAT') \
                .format(self.table, s3_bucket, credentials.access_key, credentials.secret_key, self.json_format)

        self.log.info(sql_statement)
        redshift_hook.run(sql_statement)
