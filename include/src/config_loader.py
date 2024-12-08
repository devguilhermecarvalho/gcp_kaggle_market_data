import yaml

class ConfigLoader:
    def __init__(self, config_path='include/configs/google_cloud.yml'):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self):
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found at {self.config_path}")

    def get_default_parameters(self):
        return self.config.get('default_parameters', {})
    
    def get_gcp_configs(self):
        return self.config.get('gcp_configs', {})
    
    def get_bucket_configs(self):
        gcp_configs = self.get_gcp_configs()
        return gcp_configs.get('cloud_storage', {}).get('buckets', [])
    
    def get_kaggle_configs(self):
        return self.config.get('kaggle', {})