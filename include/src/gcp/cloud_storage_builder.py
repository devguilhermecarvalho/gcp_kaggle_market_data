from google.cloud import storage
from include.src.config_loader import ConfigLoader

class CloudStorageBuilder:
    def __init__(self):
        
        self.config_loader = ConfigLoader()
        self.client = storage.Client()
        self.default_parameters = self.config_loader.get_default_parameters()
        self.buckets_config = self.config_loader.get_bucket_configs()

    def validate_or_create_buckets_and_tags(self):
        """Validate, create, and apply bucket configurations and tags."""
        for bucket_config in self.buckets_config:
            bucket_name = bucket_config["name"]
            bucket_options = bucket_config.get("options", {})
            folders = bucket_config.get("folders", [])
            bucket_tags = self._merge_tags(bucket_options.get("tags", {}), self.default_parameters.get("tags", {}))

            bucket = self._get_or_create_bucket(bucket_name, bucket_options)
            self._validate_and_apply_tags(bucket, bucket_tags)
            self._create_folders(bucket, folders)

    def _get_or_create_bucket(self, bucket_name, bucket_options):
        try:
            bucket = self.client.get_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' encontrado.")
        except Exception as e:
            print(f"Bucket '{bucket_name}' não encontrado. Tentando criar... Erro: {e}")
            bucket = self.client.bucket(bucket_name)

            bucket.location = bucket_options.get("region", self.default_parameters.get("region"))
            bucket.storage_class = bucket_options.get("storage_class", self.default_parameters.get("storage_class"))
            bucket.versioning_enabled = bucket_options.get("versioning", self.default_parameters.get("versioning"))

            try:
                bucket = self.client.create_bucket(bucket)
                print(f"Bucket '{bucket_name}' criado com sucesso.")
            except Exception as e:
                print(f"Erro ao criar o bucket '{bucket_name}': {e}")
                raise RuntimeError(f"Erro ao criar bucket '{bucket_name}'") from e

        return bucket


    def _merge_tags(self, bucket_tags, default_tags):
        """Merge bucket tags with default tags."""
        merged_tags = default_tags.copy()
        merged_tags.update(bucket_tags)
        return merged_tags

    def _validate_and_apply_tags(self, bucket, tags):
        """Validate and apply tags to the bucket."""
        existing_tags = bucket.labels

        if existing_tags != tags:
            bucket.labels = tags
            bucket.patch()
            print(f"Tags updated on bucket '{bucket.name}': {tags}")
        else:
            print(f"Tags on bucket '{bucket.name}' are already up-to-date.")

    def _create_folders(self, bucket, folders):
        """Create simulated folders in the bucket."""
        for folder_name in folders:
            blob = bucket.blob(f"{folder_name}/")
            if not blob.exists():
                blob.upload_from_string("")
                print(f"Folder '{folder_name}' created in '{bucket.name}'.")
            else:
                print(f"Folder '{folder_name}' already exists in '{bucket.name}'.")