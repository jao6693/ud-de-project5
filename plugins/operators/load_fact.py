from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 queries,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.queries = queries

    def execute(self, context):
        """
        """
        self.log.info('Starting LoadFactOperator for table songplays')

        # get Redshift connection string
        redshift_hook = PostgresHook(self.conn_id)

        # create the corresponding table on Redshift
        self.log.info(
            f'Run CREATE statement for songplays table from helper class')
        # the query is in the helper class
        query = 'songplay_table_create'
        sql_statement = getattr(self.queries, query)
        self.log.info(sql_statement)
        redshift_hook.run(sql_statement)

        # load fact table from staging
        self.log.info(f'Run INSERT statement for songplay from helper class')
        # the query is in the helper class
        query = 'songplay_table_insert'
        sql_statement = getattr(self.queries, query)
        self.log.info(sql_statement)
        redshift_hook.run(sql_statement)
