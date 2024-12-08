from google.cloud import bigquery
from include.src.config_loader import ConfigLoader

class BigQueryCheck:
    def __init__(self):
        self.config_loader = ConfigLoader()
        self.client = bigquery.Client()
        self.default_parameters = self.config_loader.get_default_parameters()
        self.bigquery_config = self.config_loader.config.get("bigquery", {})

    def verify_datasets(self):
        """
        Verifica se os datasets esperados já existem no BigQuery.
        
        Retorna:
            dict: Um relatório com as informações dos datasets existentes e ausentes.
        """
        datasets_config = self.bigquery_config.get("datasets", [])
        expected_datasets = {dataset["name"] for dataset in datasets_config}
        existing_datasets = self._get_existing_datasets()

        report ={
            "existing_datasets": expected_datasets & existing_datasets,
            "missing_datasets": expected_datasets - existing_datasets
        }

        print(f'Existing datasets: {report['existing_datasets']}')
        print(f'Missing datasets: {report['missing_datasets']}')
        return report

        def _get_existing_datasets(self):
            """
            Obtém os datasets existentes no BigQuery.

            Retorna:
                set: Um conjunto com os nomes dos datasets existentes.
            """
            datasets = self.client.list_datasets()

            return {dataset.dataset_id for dataset in datasets}