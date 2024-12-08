from google.cloud import bigquery
from include.src.config_loader import ConfigLoader

class BigQueryBuilder:
    def __init__(self):
        self.config_loader = ConfigLoader()
        self.client = bigquery.Client()
        self.default_parameters = self.config_loader.get_default_parameters()
        self.bigquery_config = self.config_loader.config.get("bigquery", {})
    
    def setup_datasets(self):
        """Set up datasets in BigQuery based on YAML configurations."""
        datasets_config = self.bigquery_config.get("datasets", [])
        try:
            for dataset_config in datasets_config:
                dataset_name = dataset_config["name"]
                dataset_options = dataset_config.get("options", {})
                dataset_tags = self._merge_tags(
                    dataset_options.get('tags', {}),
                    self.default_parameters.get('tags', {})
                )
            self._create_or_update_dataset(dataset_name, dataset_options, dataset_tags)
        except Exception as e:
            print(f"Error setting up datasets: {e}")
    
    def _create_or_update_dataset(self, dataset_name, options, dataset_tags):
        """Create or update the dataset with the provided configurations."""
        dataset_id = f"{self.client.project}.{dataset_name}"
        dataset = bigquery.Dataset(dataset_id)

        dataset.location = options.get("region", self.default_parameters.get("region"))
        dataset.description = options.get("description", self.default_parameters.get("description"))
        dataset.labels = dataset_tags

        try:
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            print(f"Dataset {dataset_name} created or updated successfully.")
        except Exception as e:
            print(f"Error creating or updating dataset {dataset_name}: {e}")
        
    def _merge_tags(self, dataset_tags, default_tags):
        """Merge dataset tags with default tags."""
        merged_tags = default_tags.copy()
        merged_tags.update(dataset_tags)
        return merged_tags