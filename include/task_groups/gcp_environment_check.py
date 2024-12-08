from airflow.operators.python import BranchPythonOperator
from airflow.decorators import task_group, task

# Factory
from include.src.factory import ServiceFactory

@task_group
def gcp_environment_check():
    # Define funções para os BranchPythonOperators
    def cloud_storage_check_branch(**context):
        print("Verificando o ambiente do Cloud Storage")
        # Instância da funcionalidade da task
        cloud_storage_check = ServiceFactory.cloud_storage_check()
        # Obtém o task_group_id dinamicamente do contexto
        task_group_id = context['ti'].task.task_group.group_id
        
        result = False  # Simulação de verificação
        
        if result:
            return f"{task_group_id}.environment_checked"
        return f"{task_group_id}.cloud_storage_builder"

    def bigquery_check_branch(**context):
        print("Verificando o ambiente do BigQuery")
        # Instância da funcionalidade da task
        bigquery_check = ServiceFactory.bigquery_check()

        # Obtém o task_group_id dinamicamente do contexto
        task_group_id = context['ti'].task.task_group.group_id
        
        result = False  # Simulação de verificação
        
        if result:
            return f"{task_group_id}.environment_checked"
        return f"{task_group_id}.bigquery_builder"

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
        cloud_storage_builder = ServiceFactory.cloud_storage_builder()

        return "Cloud Storage Construído"

    @task(task_id='bigquery_builder')
    def bigquery_builder():
        print("Construindo o ambiente do BigQuery")
        bigquery_builder = ServiceFactory.bigquery_builder()
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
