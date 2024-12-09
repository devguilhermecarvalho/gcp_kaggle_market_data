import os
import zipfile
import tempfile
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import ConnectionError, Timeout
import logging

def unzip_files(data):
    kaggle_unziped = KaggleUnziped()
    return kaggle_unziped.unzip_all(data)

class KaggleUnziped:
    def __init__(self, config=None):
        self.config = config or {}
        self.client = storage.Client()
        self.bucket_name = 'kaggle_landing_zone'

    def unzip_all(self, data):
        """
        Descompacta e faz upload de múltiplos datasets em paralelo.
        """
        unzipped_paths = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self._unzip_and_upload, file_info) for file_info in data]
            for future in futures:
                try:
                    unzipped_paths.append(future.result())
                except Exception as e:
                    logging.error(f"Erro ao descompactar e fazer upload: {e}")
        return unzipped_paths

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2),
        retry=retry_if_exception_type((ConnectionError, Timeout))
    )
    def _unzip_and_upload(self, file_info):
        remote_path = file_info['path']
        dataset_name = file_info['dataset_name']

        with tempfile.TemporaryDirectory() as temp_dir:
            local_zip_path = os.path.join(temp_dir, os.path.basename(remote_path))
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(remote_path)
            try:
                blob.download_to_filename(local_zip_path)
            except Exception as e:
                logging.error(f"Erro ao baixar o arquivo do bucket: {e}")
                raise

            unzip_dir = os.path.join(temp_dir, "unzipped")
            os.makedirs(unzip_dir, exist_ok=True)

            try:
                with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(unzip_dir)
            except zipfile.BadZipFile as e:
                logging.error(f"Erro ao descompactar o arquivo: {e}")
                raise

            destination_folder = f"bronze/{dataset_name}/unzipped/"
            self._upload_directory_to_bucket(unzip_dir, destination_folder)
            return destination_folder

    def _upload_directory_to_bucket(self, directory_path, destination_path):
        """
        Faz o upload dos arquivos extraídos para o bucket em paralelo.
        """
        bucket = self.client.bucket(self.bucket_name)
        
        def upload_file(file_path):
            relative_path = os.path.relpath(file_path, directory_path)
            blob_name = f"{destination_path}{relative_path}"
            blob = bucket.blob(blob_name)
            try:
                blob.upload_from_filename(file_path)
                logging.info(f"Arquivo '{file_path}' enviado para 'gs://{self.bucket_name}/{blob_name}'")
            except Exception as e:
                logging.error(f"Erro ao enviar arquivo '{file_path}': {e}")
                raise

        # Lista todos os arquivos no diretório
        files_to_upload = [
            os.path.join(root, file_name)
            for root, _, files in os.walk(directory_path)
            for file_name in files
        ]

        # Upload em paralelo
        with ThreadPoolExecutor(max_workers=10) as executor:  # Ajuste `max_workers` conforme necessário
            executor.map(upload_file, files_to_upload)
