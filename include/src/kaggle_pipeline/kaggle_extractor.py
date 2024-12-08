import os
from kaggle.api.kaggle_api_extended import KaggleApi
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import tempfile
from google.cloud import storage

class KaggleExtractor:
    def __init__(self, config=None):
        self.api = KaggleApi()
        self.api.authenticate()
        self.config = config or {}
        self.base_path = self.config.get('base_path', '/tmp/kaggle_data')
        self.bucket_name = self.config.get('bucket_name', 'kaggle_landing_zone')
        self.folder = self.config.get('folder', 'bronze')
        self.client = storage.Client()

    def extract_all(self, dataset_ids):
        """
        Baixa todos os datasets fornecidos e organiza em pastas por data.
        """
        results = []
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._extract_dataset, dataset_id) for dataset_id in dataset_ids]
            for future in futures:
                results.extend(future.result())
        return results

    def _extract_dataset(self, dataset_id):
        """
        Baixa e organiza o dataset em uma estrutura com base na data.
        """
        dataset_name = dataset_id.split('/')[-1]
        current_date = datetime.now().strftime("%d-%m-%Y")
        compacted_path = f"{self.folder}/{dataset_name}/{current_date}/compacted/"

        with tempfile.TemporaryDirectory() as temp_dir:
            zip_file_path = os.path.join(temp_dir, f"{dataset_name}.zip")

            print(f"Baixando dataset '{dataset_id}' para '{zip_file_path}'")
            self.api.dataset_download_files(dataset_id, path=temp_dir, unzip=False)

            if not os.path.exists(zip_file_path):
                raise FileNotFoundError(f"Erro no download: '{zip_file_path}' não encontrado.")

            # Upload do arquivo compactado
            self._upload_to_bucket(zip_file_path, compacted_path)

            print(f"Arquivo compactado '{zip_file_path}' processado.")
            return zip_file_path

    def _upload_to_bucket(self, file_path, destination_path):
        """
        Faz o upload de um único arquivo para o bucket.
        """
        bucket = self.client.bucket(self.bucket_name)
        blob_name = f"{destination_path}{os.path.basename(file_path)}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
        print(f"Arquivo '{file_path}' enviado para 'gs://{self.bucket_name}/{blob_name}'")

    def _upload_directory_to_bucket(self, directory_path, destination_path):
        """
        Faz o upload de todos os arquivos de um diretório para o bucket.
        """
        bucket = self.client.bucket(self.bucket_name)
        for root, _, files in os.walk(directory_path):
            for file_name in files:
                full_path = os.path.join(root, file_name)
                relative_path = os.path.relpath(full_path, directory_path)
                blob_name = f"{destination_path}{relative_path}"
                blob = bucket.blob(blob_name)
                blob.upload_from_filename(full_path)
                print(f"Arquivo '{full_path}' enviado para 'gs://{self.bucket_name}/{blob_name}'")

    def _unzip_file(self, zip_file_path, output_dir):
        """
        Descompacta um arquivo .zip.
        """
        import zipfile
        print(f"Descompactando '{zip_file_path}' para '{output_dir}'")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        print(f"Arquivo '{zip_file_path}' descompactado com sucesso.")
