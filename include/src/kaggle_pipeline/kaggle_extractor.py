import os
from kaggle.api.kaggle_api_extended import KaggleApi
from concurrent.futures import ThreadPoolExecutor

class KaggleExtractor:
    def __init__(self, config=None):
        self.api = KaggleApi()
        self.api.authenticate()
        self.config = config or {}
        self.base_path = self.config.get('base_path', '/tmp/kaggle_data')

    def extract_all(self, dataset_ids):
        """
        Baixa todos os datasets fornecidos e retorna os caminhos dos arquivos .zip.
        """
        results = []
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._extract_dataset, dataset_id) for dataset_id in dataset_ids]
            for future in futures:
                results.extend(future.result())
        return results

    def _extract_dataset(self, dataset_id):
        """
        Baixa o dataset e retorna uma lista de caminhos dos arquivos .zip baixados.
        """
        dataset_name = dataset_id.split('/')[-1]
        dataset_path = os.path.join(self.base_path, dataset_name)
        os.makedirs(dataset_path, exist_ok=True)

        print(f"Baixando dataset '{dataset_id}' para '{dataset_path}'")
        self.api.dataset_download_files(dataset_id, path=dataset_path, unzip=False)

        # Retorna os arquivos .zip no diretório
        zip_files = [os.path.join(dataset_path, f) for f in os.listdir(dataset_path) if f.endswith('.zip')]
        print(f"Arquivos .zip baixados: {zip_files}")
        return zip_files
