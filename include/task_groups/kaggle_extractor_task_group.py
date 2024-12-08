from airflow.decorators import task_group, task
from airflow.operators.python import BranchPythonOperator

from include.src.factory import ServiceFactory
from include.src.config_loader import ConfigLoader 

@task_group
def kaggle_extractor():
    def kaggle_validation_branch(**context):
        kaggle_validator = ServiceFactory.create_instance('kaggle_validator')
        
        # Carrega as configurações específicas do Kaggle
        config_loader = ServiceFactory.create_instance('config_loader')
        kaggle_config = ConfigLoader.load_kaggle_config()
        
        dataset_ids = kaggle_config.get('datasets', [])
        if not dataset_ids:
            raise ValueError("Nenhum dataset encontrado na configuração do Kaggle.")

        dataset_ids = [d['id'] for d in dataset_ids]
        print(f"Datasets a validar: {dataset_ids}")

        validation_report = kaggle_validator.validate_datasets(dataset_ids)
        print(f"Relatório de validação: {validation_report}")

        if all(validation_report.values()):
            return "kaggle_extractor.datasets_valid"
        return "kaggle_extractor.extract_datasets"

    t1 = BranchPythonOperator(
        task_id='kaggle_validation_branch',
        python_callable=kaggle_validation_branch,
        provide_context=True
    )

    @task(task_id='extract_datasets')
    def extract_datasets():
        kaggle_extractor = ServiceFactory.create_instance('kaggle_extractor')
        
        # Carrega configurações do arquivo kaggle.yml
        kaggle_config = ConfigLoader.load_kaggle_config()
        dataset_ids = kaggle_config.get('datasets', [])
        
        if not dataset_ids:
            raise ValueError("Nenhum dataset encontrado na configuração do Kaggle.")

        dataset_ids = [d['id'] for d in dataset_ids]
        print(f"Extraindo datasets: {dataset_ids}")
        
        extracted_data = kaggle_extractor.extract_all(dataset_ids)
        print(f"Dados extraídos: {extracted_data}")
        return extracted_data

    @task(task_id='datasets_valid')
    def datasets_valid():
        print("Todos os datasets já estão disponíveis no bucket.")

    @task(task_id='upload_to_gcs')
    def upload_to_gcs(data):
        kaggle_uploader = ServiceFactory.create_instance('kaggle_uploader')
        kaggle_uploader.upload_all(data)

    @task(task_id='unzip_files')
    def unzip_files(data):
        """
        Descompacta os arquivos .zip e retorna os caminhos dos diretórios descompactados.
        """
        kaggle_unziped = ServiceFactory.create_instance('kaggle_unziped')
        unzipped_data = kaggle_unziped.unzip_all(data)
        print(f"Arquivos descompactados: {unzipped_data}")
        return unzipped_data

    extracted_data = extract_datasets()
    unzipped_data = unzip_files(extracted_data)
    upload_task = upload_to_gcs(unzipped_data)

    t1 >> [datasets_valid(), extracted_data]
    extracted_data >> unzipped_data >> upload_task