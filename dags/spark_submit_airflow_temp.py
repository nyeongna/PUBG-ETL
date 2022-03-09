from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
#from dags.defendencies.config.base import default_args
#from operators.check_data_quality import checkDataOperator
from operators.load_table_to_Redshift import loadToRedshiftOperator
import sql_queries
import logging

# Configurations
BUCKET_NAME = "pubg-etl-project"  # replace this with your bucket name
local_data_list = ["agg_small.csv", "kill_small.csv", "weapon.csv"]
local_data = [ "./dags/data/" + file for file in local_data_list ]
s3_data = [ "data/" + file for file in local_data_list ]

local_script = ["./dags/scripts/spark/spark-script-temp.py"]
s3_script = ["scripts/spark-script-temp.py"]
s3_clean = "clean_data/"

SPARK_STEPS = [ # Note the params values are supplied to the operator
    {
        "Name": "spark-submit for pubg-data: S3 -> EMR -> S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "pubg-data-load-temp",
    "ReleaseLabel": "emr-5.34.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}, {"Name": "Livy"}, {"Name": "JupyterEnterpriseGateway"}], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 10, 16),
    "depends_on_past": False,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_submit_airflow_temp",
    default_args=default_args,
    #schedule_interval="@daily",
    max_active_runs=1,
    catchup=False
)

# Load raw data files from local -> S3
def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook()
    logging.info("aws_default")
    logging.info("emr_default")
    for idx in range(len(filename)):
        s3.load_file(filename=filename[idx], bucket_name=bucket_name, replace=True, key=key[idx])
        logging.info(f"Load {filename[idx]} to {bucket_name}{key[idx]} COMPLETED")

# Dummy operator for indicating the start of the DAG        
start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

# Load raw data files from local -> S3
data_to_s3 = PythonOperator(
    dag=dag,
    task_id="data_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": local_data, "key": s3_data,},
)
# Load Spark scripts from local -> S3
script_to_s3 = PythonOperator(
    dag=dag,
    task_id="script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": local_script, "key": s3_script},
)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

# Add your steps to the EMR cluster
# Submitting Spark-script and do the ETL
# Then save the output results back to S3
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={ # these params are used to fill the paramterized values in SPARK_STEPS json
        "BUCKET_NAME": BUCKET_NAME,
        "s3_data": s3_data,
        "s3_script": s3_script[0],
        "s3_clean": s3_clean,
    },
    dag=dag,
)

# wait for the steps to complete
last_step = len(SPARK_STEPS) - 1
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# # Create_Fact+Dimension tables in Redshift
# def create_tables():
#     redshift = PostgresHook("redshift")
#     redshift.run(sql_queries.create_schema)
#     redshift.run(sql_queries.create_match_table)
#     redshift.run(sql_queries.create_time_table)
#     redshift.run(sql_queries.create_player_table)
#     redshift.run(sql_queries.create_weapon_table)
#     redshift.run(sql_queries.create_kill_log_table)

# # Create_Fact+Dimension tables in Redshift
# create_tables = PythonOperator(
#     task_id='create_tables',
#     dag=dag,
#     python_callable=create_tables
# )

# # Load match table from S3 -> Redshift (Dimension table)
# load_match_table = loadToRedshiftOperator(
#     task_id = 'load_match_table',
#     dag=dag,
#     redshift_conn_id = "redshift",
#     aws_credentials = "aws_default",
#     table='match',
#     s3_bucket='s3://pubg-etl-project/clean_data/',
#     sql_create=sql_queries.create_match_table
# )
# # Load time table from S3 -> Redshift (Dimension table)
# load_time_table = loadToRedshiftOperator(
#     task_id = 'load_match_table',
#     dag=dag,
#     redshift_conn_id = "redshift",
#     aws_credentials = "aws_default",
#     table='time',
#     s3_bucket='s3://pubg-etl-project/clean_data/',
#     sql_create=sql_queries.create_time_table
# )
# # Load player table from S3 -> Redshift (Dimension table)
# load_player_table = loadToRedshiftOperator(
#     task_id = 'load_match_table',
#     dag=dag,
#     redshift_conn_id = "redshift",
#     aws_credentials = "aws_default",
#     table='player',
#     s3_bucket='s3://pubg-etl-project/clean_data/',
#     sql_create=sql_queries.create_player_table
# )
# # Load weapon table from S3 -> Redshift (Dimension table)
# load_weapon_table = loadToRedshiftOperator(
#     task_id = 'load_match_table',
#     dag=dag,
#     redshift_conn_id = "redshift",
#     aws_credentials = "aws_default",
#     table='weapon',
#     s3_bucket='s3://pubg-etl-project/clean_data/',
#     sql_create=sql_queries.create_weapon_table
# )
# # Load kill_log table from S3 -> Redshift (Fact table)
# load_kill_log_table = loadToRedshiftOperator(
#     task_id = 'load_match_table',
#     dag=dag,
#     redshift_conn_id = "redshift",
#     aws_credentials = "aws_default",
#     table='kill_log',
#     s3_bucket='s3://pubg-etl-project/clean_data/',
#     sql_create=sql_queries.create_kill_log_table
# )

'''
Validate the final [Fact, Dimension] tables in Redshift
- check if len(record) > 0
- check if unneeded Null values
- check primary key uniqueness
'''
validate_data = DummyOperator(task_id="validate_data", dag=dag)

'''
Dummy operator for indicating the end of the DAG
'''
end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)



'''
TASK DEPEDNCIES
'''
start_data_pipeline >> [data_to_s3, script_to_s3] >> create_emr_cluster
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
# terminate_emr_cluster >> create_tables > [load_match_table, load_time_table, load_player_table, load_weapon_table, load_kill_log_table] >> validate_data
# validate_data >> end_data_pipeline
