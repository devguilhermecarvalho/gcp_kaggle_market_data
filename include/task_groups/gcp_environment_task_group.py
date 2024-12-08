from airflow.operators.python import BranchPythonOperator
from airflow.decorators import task_group, task

from include.src.factory import ServiceFactory

@task_group
def gcp_environment_check():
    # Define funções para os BranchPythonOperators
    def cloud_storage_check_branch(**context):
        print("Iniciando verificação do ambiente do Cloud Storage")
        cloud_storage_check = ServiceFactory.create_instance('cloud_storage_check')
        task_group_id = context['ti'].task.task_group.group_id

        # Relatório de buckets
        report = cloud_storage_check.verify_buckets()
        print(f"Relatório de buckets: {report}")

        if report['missing_buckets']:
            print(f"Buckets ausentes detectados: {report['missing_buckets']}")
            return f"{task_group_id}.cloud_storage_builder"
        
        print("Todos os buckets estão presentes. Seguindo para próxima etapa.")
        return f"{task_group_id}.environment_checked"

    def bigquery_check_branch(**context):
        print("Iniciando verificação do ambiente do BigQuery")
        bigquery_check = ServiceFactory.create_instance('bigquery_check')
        task_group_id = context['ti'].task.task_group.group_id

        # Relatório de datasets
        report = bigquery_check.verify_datasets()
        print(f"Relatório de datasets: {report}")

        if report['missing_datasets']:
            print(f"Datasets ausentes detectados: {report['missing_datasets']}")
            return f"{task_group_id}.bigquery_builder"
        
        print("Todos os datasets estão presentes. Seguindo para próxima etapa.")
        return f"{task_group_id}.environment_checked"

    # Branch para Cloud Storage
    t1 = BranchPythonOperator(
        task_id='cloud_storage_check_branch',
        python_callable=cloud_storage_check_branch,
        provide_context=True
    )
    
    # Branch para BigQuery
    t2 = BranchPythonOperator(
        task_id='bigquery_check_branch',
        python_callable=bigquery_check_branch,
        provide_context=True
    )

    @task(task_id='cloud_storage_builder')
    def cloud_storage_builder():
        print("Construindo o ambiente do Cloud Storage")
        # Instância da funcionalidade da task
        cloud_storage_builder = ServiceFactory.create_instance('cloud_storage_builder')
        cloud_storage_builder.validate_or_create_buckets_and_tags()
        return "Cloud Storage Construído"

    @task(task_id='bigquery_builder')
    def bigquery_builder():
        print("Construindo o ambiente do BigQuery")
        bigquery_builder = ServiceFactory.create_instance('bigquery_builder')
        bigquery_builder.setup_datasets()
        return "BigQuery Construído"

    @task(task_id='environment_checked')
    def environment_checked():
        print("Ambiente verificado com sucesso!")
        return "Ambiente Verificado"

    # Instancia tarefas de construção e verificação
    t3 = cloud_storage_builder()
    t4 = bigquery_builder()
    t5 = environment_checked()

    # Configura os BranchPythonOperators para decidir o caminho
    t1 >> [t3, t5]
    t2 >> [t4, t5]
