from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 table = "",
                 redshift_conn_id = "",               
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('DataQualityOperator not implemented yet')
        rows = redshift.get_records("select count(*) FROM {}".format(self.table))
        
        self.log.info('DataQualityOperator not implemented yet')
        if len(rows) < 1 or len(rows[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
            
        num_rows = rows[0][0]
        if num_rows < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        logging.info(f"Data quality on table {self.table} check passed with {rows[0][0]} records")
  