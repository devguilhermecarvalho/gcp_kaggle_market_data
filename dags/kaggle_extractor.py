from pendulum import datetime
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from include.task_groups.gcp_environment_task_group import gcp_environment_check
from include.task_groups.kaggle_extractor_task_group import kaggle_extractor

@dag(
    dag_id='kaggle_gcp_extraction',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['extraction', 'gcp', 'kaggle']
)
def pipeline_kaggle_extractor():
    # Task Group: GCP Environment Check
    env_check = gcp_environment_check()

    # Task Group: Kaggle Extractor
    kaggle_extraction = kaggle_extractor()

    # DummyOperator para convergir os caminhos
    merge_task_01 = DummyOperator(
        task_id='merge_task_01',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    merge_task_02 = DummyOperator(
        task_id='merge_task_02',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # Final task to indicate successful pipeline completion
    @task(task_id='final_test_message')
    def final_test_message():
        print("Pipeline concluído com sucesso!")

    t_final = final_test_message()

    # Definir fluxo de dependências
    env_check >> merge_task_01 >> kaggle_extraction >> merge_task_02 >> t_final

dag = pipeline_kaggle_extractor()
