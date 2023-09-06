from datetime import datetime

from airflow.contrib.operators.ssh_operator import SSHOperator
from plugins.sparketl.helper import ssh_set_env
from operators.parentDAG import BaseDAG


'''
A DAG template for daily unloading of a data sample from HDFS for a specific day, corresponding to the DAG launch date (ds). 
This variable does not need to be passed if the start date is not important. 
It is also necessary to set the variables: owner, dag_id, schedule_interval, spark_class_path, task_name, task_id.
'''

args = {
    "start_date": datetime(2022, 2, 18),
    "owner": ""
}

dag_id = "airflow_dag_template"
ds = '{{ ds }}'

with BaseDAG(custom_args=args, schedule_interval='* * * * *', dag_id=dag_id) as dag:

    env_params = {"ds": ds}
    spark_class_path = ""
    ssh_command = ssh_set_env(env_params, f"python3 {spark_class_path}")

    task_name = SSHOperator(
        task_id="task_id",
        ssh_conn_id="ssh_default",
        dag=dag,
        command=ssh_command
    )
