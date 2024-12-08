from pendulum import datetime
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from include.task_groups.gcp_environment_check import gcp_environment_check

@dag(
    dag_id='kaggle_gcp_extraction',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['exemplo', 'gcp', 'task_group']
)
def pipeline_kaggle_extractor():
    # Instanciando a task group
    env_check = gcp_environment_check()
    
    # Adicionando um DummyOperator para convergir os caminhos
    merge_task = DummyOperator(
        task_id='merge_task',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    @task(task_id='final_message')
    def final_message():
        print("Pipeline concluído com sucesso!")
    
    t_final = final_message()

    # Conectar o merge_task ao final_message
    env_check >> merge_task >> t_final

dag = pipeline_kaggle_extractor()