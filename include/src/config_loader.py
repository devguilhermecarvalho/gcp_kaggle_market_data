import yaml
from pydantic import BaseModel, ValidationError

class ConfigSchema(BaseModel):
    default_parameters: dict
    gcp_configs: dict
    bigquery: dict

class ConfigLoader:
    def __init__(self, config_path='include/configs/google_cloud.yml'):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self):
        try:
            with open(self.config_path, 'r') as file:
                config_data = yaml.safe_load(file)
                ConfigSchema(**config_data)  # Validate configuration with Pydantic
                return config_data
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found at {self.config_path}")
        except ValidationError as e:
            raise ValueError(f"Config validation error: {e}")

    def get_default_parameters(self):
        return self.config.get('default_parameters', {})

    def get_gcp_configs(self):
        return self.config.get('gcp_configs', {})

    def get_bucket_configs(self):
        gcp_configs = self.get_gcp_configs()
        buckets = gcp_configs.get('cloud_storage', {}).get('buckets', [])
        if not buckets:
            raise ValueError("Nenhum bucket encontrado no arquivo de configuração.")
        return buckets

    @staticmethod
    def load_kaggle_config():
        kaggle_config_path = 'include/configs/kaggle.yml'
        try:
            with open(kaggle_config_path, 'r') as file:
                return yaml.safe_load(file).get('kaggle', {})
        except FileNotFoundError:
            raise FileNotFoundError(f"Kaggle config file not found at {kaggle_config_path}")