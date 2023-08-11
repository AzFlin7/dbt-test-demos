from datetime import datetime
from distutils.command.install_headers import install_headers
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import os

DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR')
DBT_VENV_PATH = os.getenv('DBT_VENV_PATH')

dag = DAG('dbt_transform_dag', description='dbt transformer dag',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 9, 2), catchup=False)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

install_deps = BashOperator(
            task_id='install_deps', 
            bash_command=f'{DBT_VENV_PATH} deps --project-dir {DBT_PROJECT_DIR}',
            dag=dag)

transformer_compile_tasks = []
transformer_run_tasks = []
for event_type in ["ForkEvent", "PushEvent", "WatchEvent"]:   
    compile_task =  BashOperator(
                            task_id=f'compile_transformer_event_{event_type}', 
                            bash_command=f'export EVENT_TYPE={event_type} &&{DBT_VENV_PATH} compile --select dbt_transformer --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}',
                            dag=dag)
    
    run_task = BashOperator(
                            task_id=f'run_transformer_event_{event_type}', 
                            bash_command=f'export EVENT_TYPE={event_type} &&{DBT_VENV_PATH} run --select dbt_transformer --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}',
                            dag=dag)
    
    compile_task >> run_task
    transformer_compile_tasks.append(compile_task)
    transformer_run_tasks.append(run_task)

dummy_task >> install_deps >> transformer_compile_tasks