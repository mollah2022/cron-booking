import tomllib
from pathlib import Path


class Settings:
    """
    This class reads the config.toml file and stores all
    values as attributes, so other files can easily use them
    like Settings().spark_app_name
    """

    def __init__(self, config_path: str = None):
        if config_path is None:
            # config.toml is in the same folder as this file (settings.py),
            # so we find it using a relative path
            config_path = Path(__file__).parent / "config.toml"

        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        # Spark related settings
        self.spark_app_name = config["spark"]["app_name"]
        self.spark_master = config["spark"]["master"]

        # Warehouse related settings
        self.warehouse_path = config["warehouse"]["path"]
        self.catalog_name = config["warehouse"]["catalog_name"]

        # Iceberg table related settings
        self.iceberg_database = config["iceberg"]["database"]
        self.iceberg_table_name = config["iceberg"]["table_name"]

        # Raw data path
        self.raw_json_path = config["data"]["raw_json_path"]