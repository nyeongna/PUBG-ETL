from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
import logging

class loadToRedshiftOperator(BaseOperator):
    copy_sql = """
        COPY pubg.{}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        CSV
        IGNOREHEADER 1
        TIMEFORMAT 'YYYY-MM-DD HH:MI:SS';
    """

    def __init__(self, redshift_conn_id, aws_credentials, 
                 table, s3_bucket, sql_create, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.sql_create = sql_create
        
    def execute(self, context):
        logging.info(self.table)
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = self.s3_bucket + self.table

        logging.info(f'CREATING {self.table}')
        redshift.run(self.sql_create)
        formatted_sql = loadToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
        )
        logging.info(f'COPYING {self.table} FROM S3 to Redshift')
        redshift.run(formatted_sql)
        #super().execute(context)


class checkDataOperator(BaseOperator):
    def __init__(self,
                 redshift_conn_id="",
                 dq_check_list="",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_check_list = dq_check_list
        
    def execute(self, context):
        self.log.info(f'Data Quality Checking On Progress')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        table_list = ['songplays', 'users', 'songs', 'artists', 'time']
        table_id = {
            'songplays': 'playid',
            'users': 'userid',
            'songs': 'songid',
            'artists': 'artistid',
            'time': 'start_time'
        }
        for check in self.dq_check_list:
            dq_check = check['dq_check']
            exp_res = check['expected_result']
            success = check['success']
            fail = check['fail']
            for table in table_list:
                id = table_id[table]
                res = redshift.get_records(dq_check.format(table, id))
                exp = redshift.get_records(exp_res.format(table))
                if res == exp:
                    self.log.info(success.format(table))
                else:
                    raise ValueError(fail.format(table))
                    
#         for table in table_list:
#             records = redshift.get_records(f"SELECT * FROM {table} LIMIT 10")
#             if records is None or len(records[0]) < 1:
#                 raise ValueError(f"Data quality check failed. {table} returned no results")
#             if table=='songplays':
#                 r1 = redshift.get_records(f"SELECT DISTINCT playid FROM {table}")
#                 r2 = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
#                 if len(r1[0]) != len(r2[0]):
#                     raise ValueError(f"Data quality check failed. {table} PRIMARY KEY UNIQUENESS Failed")
#             if table=='time':
#                 r1 = redshift.get_records(f"SELECT DISTINCT start_time FROM {table}")
#                 r2 = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
#                 if len(r1[0]) != len(r2[0]):
#                     raise ValueError(f"Data quality check failed. {table} PRIMARY KEY UNIQUENESS Failed")
#             if table in ['users', 'songs', 'artists']:
#                 r1 = redshift.get_records(f"SELECT DISTINCT {table[:-1]}id FROM {table}")
#                 r2 = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
#                 if len(r1[0]) != len(r2[0]):
#                     raise ValueError(f"Data quality check failed. {table} PRIMARY KEY UNIQUENESS Failed")
        self.log.info(f'Data Quality Checking COMPLETED')
            