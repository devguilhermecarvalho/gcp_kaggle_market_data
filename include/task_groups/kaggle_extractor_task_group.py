from airflow.decorators import task_group, task
from airflow.operators.python import BranchPythonOperator
from include.src.factory import ServiceFactory
from include.src.config_loader import ConfigLoader


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
    def unzip_files(data):
        kaggle_unziped = ServiceFactory.create_instance('kaggle_unziped')

        # Verifica se todos os arquivos existem antes de prosseguir
        for file_path in data:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Arquivo '{file_path}' não encontrado para descompactação.")

        unzipped_data = kaggle_unziped.unzip_all(data)
        return unzipped_data

    @task(task_id='upload_to_gcs')
    def upload_to_gcs(data):
        kaggle_uploader = ServiceFactory.create_instance('kaggle_uploader')

        current_date = data.get("current_date", "unknown_date")
        dataset_name = data.get("dataset_name", "unknown_dataset")

        compacted_path = f"{kaggle_uploader.folder}/{dataset_name}/{current_date}/compacted/"
        unzipped_path = f"{kaggle_uploader.folder}/{dataset_name}/{current_date}/unzipped/"

        # Upload compactado
        print(f"Fazendo upload de arquivos compactados para {compacted_path}.")
        kaggle_uploader.upload_all(data["compacted"], destination_path=compacted_path)

        # Upload descompactado
        print(f"Fazendo upload de arquivos descompactados para {unzipped_path}.")
        kaggle_uploader.upload_all(data["unzipped"], destination_path=unzipped_path)

    extracted_data = extract_datasets()
    unzipped_data = unzip_files(extracted_data)
    upload_task = upload_to_gcs(unzipped_data)

    t1 >> [datasets_valid(), extracted_data]
    extracted_data >> unzipped_data >> upload_task
