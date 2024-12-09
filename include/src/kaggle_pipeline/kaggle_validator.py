from google.cloud import storage
from datetime import datetime

class KaggleValidator:
    def __init__(self, config=None):
        self.config = config or {}
        self.bucket_name = self.config.get('bucket_name', 'kaggle_landing_zone')
        self.folder = self.config.get('folder', 'bronze')

        self.client = storage.Client()

    def validate_datasets(self, dataset_ids):
        results = {}

        for dataset_id in dataset_ids:
            dataset_name = dataset_id.split('/')[-1]
            compacted_path = f"{self.folder}/{dataset_name}/compacted/"
            unzipped_path = f"{self.folder}/{dataset_name}/unzipped/"

            compacted_present = self._path_exists(compacted_path)
            unzipped_present = self._path_exists(unzipped_path)

            results[dataset_id] = {
                "compacted_present": compacted_present,
                "unzipped_present": unzipped_present,
            }

            if not compacted_present or not unzipped_present:
                print(f"Dataset '{dataset_name}' não validado. Arquivos ausentes.")
            else:
                print(f"Dataset '{dataset_name}' validado no bucket.")
        return results

    def _path_exists(self, prefix):
        """
        Verifica se há arquivos no bucket com o prefixo fornecido.
        """
        bucket = self.client.bucket(self.bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))
        return bool(blobs)
