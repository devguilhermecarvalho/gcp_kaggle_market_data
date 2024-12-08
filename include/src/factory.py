from typing import Any, Callable

from include.src.gcp.cloud_storage_check import CloudStorageCheck
from include.src.gcp.bigquery_check import BigQueryCheck
from include.src.gcp.cloud_storage_builder import CloudStorageBuilder
from include.src.gcp.bigquery_builder import BigQueryBuilder

from include.src.kaggle_pipeline.kaggle_validator import KaggleValidator
from include.src.kaggle_pipeline.kaggle_extractor import KaggleExtractor
from include.src.kaggle_pipeline.kaggle_uploader import KaggleUploader
from include.src.kaggle_pipeline.kaggle_unziped import KaggleUnziped

from include.src.config_loader import ConfigLoader

class ServiceFactory:
    """Factory class to create instances of services like validators and extractors."""
    class_map: dict[str, Callable[[], Any]] = {
        # GCP Environment
        'cloud_storage_check': CloudStorageCheck,
        'bigquery_check': BigQueryCheck,
        'cloud_storage_builder': CloudStorageBuilder,
        'bigquery_builder': BigQueryBuilder,

        # Kaggle Extractor Environment
        'kaggle_validator': KaggleValidator,
        'kaggle_extractor': KaggleExtractor,
        'kaggle_uploader': KaggleUploader,
        'kaggle_unziped': KaggleUnziped,

        # Config Loader
        'config_loader': ConfigLoader
    }

    @staticmethod
    def create_instance(service_name: str):
        try:
            return ServiceFactory.class_map[service_name]()
        except KeyError:
            raise ValueError(f"Service {service_name} not found in class map.")
