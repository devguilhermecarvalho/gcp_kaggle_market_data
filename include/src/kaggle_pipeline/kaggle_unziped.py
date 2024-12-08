import os
import zipfile
from concurrent.futures import ThreadPoolExecutor

class KaggleUnziped:
    def __init__(self, config=None):
        self.config = config or {}

    def unzip_all(self, data):
        """
        Descompacta todos os arquivos .zip fornecidos e retorna os caminhos dos diretórios descompactados.
        """
        unzipped_paths = []
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._unzip_file, file_path) for file_path in data]
            for future in futures:
                unzipped_paths.append(future.result())
        return unzipped_paths

    def _unzip_file(self, file_path):
        """
        Descompacta um único arquivo .zip para um diretório específico.
        """
        base_dir = os.path.dirname(file_path)
        unzip_dir = os.path.join(base_dir, "unzipped")
        os.makedirs(unzip_dir, exist_ok=True)

        print(f"Descompactando '{file_path}' para '{unzip_dir}'")
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(unzip_dir)
        print(f"Arquivo descompactado: {file_path}")
        return unzip_dir