from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 queries,
                 tests,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.queries = queries
        self.tests = tests

    def execute(self, context):
        self.log.info(f'Starting DataQualityOperator for table {self.table}')

        # tests in error
        error_count = 0
        # get Redshift connection string
        redshift_hook = PostgresHook(self.conn_id)
        # get the check to run against Redshift
        for test in self.tests:
            sql_statement = getattr(self.queries, str(
                test.get('check'))).format(self.table)
            self.log.info(sql_statement)
            records = redshift_hook.get_records(sql_statement)[0]
            self.log.info(records[0])

            if test.get('operator') == '=':
                if records[0] != test.get('result'):
                    error_count += 1
            if test.get('operator') == '>':
                if records[0] <= test.get('result'):
                    error_count += 1
            if test.get('operator') == '<':
                if records[0] >= test.get('result'):
                    error_count += 1

        if error_count == 0:
            # tests passed !!
            self.log.info(
                f'Data quality on table {self.table} passed successfully')
        else:
            # tests failed
            self.log.error(f'Data quality on table {self.table} failed')
            raise ValueError(f'Data quality on table {self.table} failed')
