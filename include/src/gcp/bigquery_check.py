from google.cloud import bigquery
from include.src.config_loader import ConfigLoader

class BigQueryCheck:
    def __init__(self):
        self.config_loader = ConfigLoader()
        self.client = bigquery.Client()
        self.default_parameters = self.config_loader.get_default_parameters()
        self.bigquery_config = self.config_loader.config.get("bigquery", {})

    def verify_datasets(self):
        datasets_config = self.bigquery_config.get("datasets", [])
        expected_datasets = {dataset["name"] for dataset in datasets_config if "name" in dataset}
        existing_datasets = self._get_existing_datasets()

        print(f"Datasets esperados: {expected_datasets}")
        print(f"Datasets existentes: {existing_datasets}")

        report = {
            "existing_datasets": expected_datasets & existing_datasets,
            "missing_datasets": expected_datasets - existing_datasets
        }

        print(f"Relatório final: {report}")
        return report

    def _get_existing_datasets(self):
        """
        Obtém os datasets existentes no BigQuery.

        Retorna:
            set: Um conjunto com os nomes dos datasets existentes.
        """
        datasets = self.client.list_datasets()

        return {dataset.dataset_id for dataset in datasets}