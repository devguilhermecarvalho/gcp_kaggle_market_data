from typing import Any, Callable

from include.src.gcp.cloud_storage_check import CloudStorageCheck
from include.src.gcp.bigquery_check import BigQueryCheck

from include.src.gcp.cloud_storage_builder import CloudStorageBuilder
from include.src.gcp.bigquery_builder import BigQueryBuilder

class ServiceFactory:
    """Factory class to create instances of services like validators and extractors."""
    class_map: dict[str, Callable[[], Any]] = {
        'cloud_storage_check': CloudStorageCheck,
        'bigquery_check': BigQueryCheck,
        'cloud_storage_builder': CloudStorageBuilder,
        'bigquery_builder': BigQueryBuilder
    }

    @staticmethod
    def create_instance(service_name: str):
        try:
            return ServiceFactory.class_map[service_name]()
        except KeyError:
            raise ValueError(f"Service {service_name} not found in class map.")
