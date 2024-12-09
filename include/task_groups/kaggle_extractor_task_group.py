from airflow.decorators import task_group, task
from airflow.operators.python import BranchPythonOperator
from include.src.factory import ServiceFactory
from include.src.config_loader import ConfigLoader
from include.src.kaggle_pipeline.kaggle_unziped import unzip_files

@task_group
def kaggle_extractor():
    def kaggle_validation_branch(**context):
        kaggle_validator = ServiceFactory.create_instance('kaggle_validator')
        kaggle_config = ConfigLoader.load_kaggle_config()
        
        dataset_ids = [d['id'] for d in kaggle_config.get('datasets', [])]
        validation_report = kaggle_validator.validate_datasets(dataset_ids)

        for dataset_id, result in validation_report.items():
            if not result["compacted_present"] or not result["unzipped_present"]:
                print(f"Arquivos ausentes ou inválidos detectados para o dataset '{dataset_id}'.")
                return "kaggle_extractor.extract_datasets"
        print("Todos os arquivos estão disponíveis no bucket com a estrutura de pastas por data.")
        return "kaggle_extractor.datasets_valid"

    t1 = BranchPythonOperator(
        task_id='kaggle_validation_branch',
        python_callable=kaggle_validation_branch,
        provide_context=True
    )

    @task(task_id='extract_datasets')
    def extract_datasets():
        kaggle_extractor = ServiceFactory.create_instance('kaggle_extractor')
        kaggle_config = ConfigLoader.load_kaggle_config()
        
        dataset_ids = [d['id'] for d in kaggle_config.get('datasets', [])]
        extracted_data = kaggle_extractor.extract_all(dataset_ids)
        return extracted_data

    @task(task_id='datasets_valid')
    def datasets_valid():
        print("Todos os datasets já estão disponíveis no bucket. Pulando extração.")

    @task(task_id='unzip_files')
    def unzip_files_task(data):
        return unzip_files(data)

    extracted_data = extract_datasets()
    unzipped_data = unzip_files_task(extracted_data)

    t1 >> [datasets_valid(), extracted_data]
    extracted_data >> unzipped_data
