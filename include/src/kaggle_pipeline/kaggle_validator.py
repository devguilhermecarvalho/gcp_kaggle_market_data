from google.cloud import storage
from kaggle.api.kaggle_api_extended import KaggleApi
from concurrent.futures import ThreadPoolExecutor

class KaggleValidator:
    def __init__(self, config=None):
        """
        Inicializa o validador com o cliente do Cloud Storage e configurações do Kaggle.
        """
        self.config = config or {}
        self.bucket_name = self.config.get('bucket_name', 'kaggle_landing_zone')
        self.folder = self.config.get('folder', 'bronze')
        
        # Inicializa o cliente do Cloud Storage
        self.client = storage.Client()

        # Inicializa o cliente do Kaggle
        self.api = KaggleApi()
        self.api.authenticate()

    def validate_datasets(self, dataset_ids):
        """
        Valida se os arquivos zipados dos datasets Kaggle já existem no bucket.
        """
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(self._dataset_exists_in_bucket, dataset_ids))
        return dict(zip(dataset_ids, results))

    def _dataset_exists_in_bucket(self, dataset_name):
        """
        Verifica se os arquivos zipados de um dataset existem no bucket no prefixo correto.
        """
        bucket = self.client.bucket(self.bucket_name)
        prefix = f"{self.folder}/{dataset_name}/"
        print(f"Verificando no bucket '{self.bucket_name}' com prefixo '{prefix}'")

        blobs = list(bucket.list_blobs(prefix=prefix))
        exists = any(blob.name.endswith('.zip') for blob in blobs)
        
        if exists:
            print(f"Dataset '{dataset_name}' encontrado no bucket.")
        else:
            print(f"Dataset '{dataset_name}' NÃO encontrado no bucket.")
        
        return exists