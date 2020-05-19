from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        self.log.info(f'Starting DataQualityOperator for table {self.table}')

        # get Redshift connection string
        redshift_hook = PostgresHook(self.conn_id)
        # count the number of rows
        records = redshift_hook.get_records(
            f'SELECT COUNT(*) FROM {self.table};')
        if len(records) < 1 or len(records[0]) < 1:
            self.log.info(
                f'Data quality check failed. {self.table} returned no result')
            raise ValueError(
                f'Data quality check failed. {self.table} returned no result')

        num_records = records[0][0]
        if num_records < 1:
            self.log.info(
                f'Data quality check failed. {self.table} contained 0 rows')
            raise ValueError(
                f'Data quality check failed. {self.table} contained 0 rows')

        # test passed !!
        self.log.info(
            f'Data quality on table {self.table} check passed with {records[0][0]} records')
