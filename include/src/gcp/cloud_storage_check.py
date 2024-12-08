# include/src/gcp/cloud_storage_check.py
from google.cloud import storage
from include.src.config_loader import ConfigLoader

class CloudStorageCheck:
    def __init__(self):
        self.config_loader = ConfigLoader()
        self.client = storage.Client()
        self.default_parameters = self.config_loader.get_default_parameters()
        self.cloud_storage_config = self.config_loader.get_gcp_configs()

    def verify_buckets(self):
        buckets_config = self.cloud_storage_config.get("cloud_storage", {}).get("buckets", [])
        expected_buckets = {bucket['name'] for bucket in buckets_config if 'name' in bucket and bucket['name']}
        existing_buckets = self._get_existing_buckets()

        print(f"Buckets esperados: {expected_buckets}")
        print(f"Buckets existentes: {existing_buckets}")

        report = {
            "existing_buckets": expected_buckets & existing_buckets,
            "missing_buckets": expected_buckets - existing_buckets
        }

        print(f"Relatório final: {report}")
        return report

    def _get_existing_buckets(self):
        buckets = self.client.list_buckets()
        return {bucket.name for bucket in buckets}
