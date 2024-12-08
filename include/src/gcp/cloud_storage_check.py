from google.cloud import storage
from include.src.config_loader import ConfigLoader

class CloudStorageCheck:
    def __init__(self):
        self.config_loader = ConfigLoader()
        self.client = storage.Client()
        self.defautl_parameters = self.config_loader.get_default_parameters()
        self.cloud_storage_config = self.config_loader.config.get("cloud_storage", {})

    def verify_buckets(self):
        buckets_config = self.cloud_storage_config.get("buckets", [])
        expected_buckets = {bucket['name'] for bucket in buckets_config}
        existing_buckets = self._get_existing_buckets()

        report = {
            "existing_buckets": expected_buckets & existing_buckets,
            "missing_buckets": expected_buckets - existing_buckets
        }

        return report
    
    def _get_existing_buckets(self):
        """
        Obtém os nomes dos buckets existentes no Cloud Storage.

        Retorna:
            set: Um conjunto com os nomes dos buckets existentes.
        """
        buckets = self.client.list_buckets()
        return {bucket.name for bucket in buckets}