import os
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor

class KaggleUploader:
    def __init__(self, config=None):
        """
        Inicializa o uploader com o cliente do Cloud Storage e configurações.
        """
        self.client = storage.Client()
        self.config = config or {}
        self.bucket_name = self.config.get('bucket_name', 'kaggle_landing_zone')
        self.folder = self.config.get('folder', 'bronze')  # Adicionado para inicializar 'folder'

    def upload_all(self, data):
        """
        Faz o upload de todos os arquivos/diretórios para o bucket especificado.
        """
        with ThreadPoolExecutor() as executor:
            executor.map(self._upload_file, data)

    def _upload_file(self, file_path):
        """
        Faz o upload de um único arquivo ou percorre um diretório para upload.
        """
        if os.path.isdir(file_path):
            print(f"Percorrendo diretório '{file_path}' para upload.")
            for root, _, files in os.walk(file_path):
                for file_name in files:
                    full_path = os.path.join(root, file_name)
                    self._upload_individual_file(full_path, file_path)
        elif os.path.isfile(file_path):
            print(f"Enviando arquivo '{file_path}' para o bucket.")
            self._upload_individual_file(file_path)
        else:
            print(f"'{file_path}' não é um arquivo ou diretório válido. Pulando.")

    def _upload_individual_file(self, full_path, base_path=None):
        """
        Faz o upload de um único arquivo para o bucket.
        """
        bucket = self.client.bucket(self.bucket_name)
        
        # Se base_path for fornecido, construa o caminho relativo no bucket
        blob_name = os.path.relpath(full_path, base_path) if base_path else os.path.basename(full_path)
        blob_name = f"{self.folder}/{blob_name}"
        
        print(f"Enviando '{full_path}' para 'gs://{self.bucket_name}/{blob_name}'")
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(full_path)
        print(f"Arquivo '{full_path}' enviado com sucesso para 'gs://{self.bucket_name}/{blob_name}'.")
