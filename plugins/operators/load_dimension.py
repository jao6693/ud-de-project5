from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 queries,
                 truncate,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.queries = queries
        self.truncate = truncate

    def execute(self, context):
        """
        """
        self.log.info(f'Starting LoadDimensionOperator for table {self.table}')

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

        # truncate table if required
        if self.truncate == True:
            self.log.warning(
                f'Run TRUNCATE statement for {self.table} from helper class')
            sql_statement = getattr(self.queries, 'TRUNCATE_SQL') \
                .format(self.table)
            self.log.info(sql_statement)
            redshift_hook.run(sql_statement)

        # load dimension table from staging
        self.log.info(
            f'Run INSERT statement for {self.table} from helper class')
        # the query is in the helper class
        query = f'{self.table}_table_insert'
        sql_statement = getattr(self.queries, query)
        self.log.info(sql_statement)
        redshift_hook.run(sql_statement)
