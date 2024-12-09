from pendulum import datetime
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from include.task_groups.gcp_environment_task_group import gcp_environment_check
from include.task_groups.kaggle_extractor_task_group import kaggle_extractor
from airflow.exceptions import AirflowException
import logging

def failure_callback(context):
    """Callback de falha para enviar notificações ou logar erros detalhados."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    exception = context.get("exception")
    logging.error(f"Falha na DAG {dag_id}, tarefa {task_id}: {exception}")

@dag(
    dag_id='kaggle_gcp_extraction',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['extraction', 'gcp', 'kaggle'],
    on_failure_callback=failure_callback
)
def pipeline_kaggle_extractor():
    env_check = gcp_environment_check()
    kaggle_extraction = kaggle_extractor()

    merge_task_01 = DummyOperator(
        task_id='merge_task_01',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    merge_task_02 = DummyOperator(
        task_id='merge_task_02',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    @task(task_id='final_test_message')
    def final_test_message():
        logging.info("Pipeline concluído com sucesso!")

    t_final = final_test_message()

    env_check >> merge_task_01 >> kaggle_extraction >> merge_task_02 >> t_final

dag = pipeline_kaggle_extractor()
