import os
import tomllib
from pathlib import Path


class Settings:
    """
    This class reads the config.toml file and stores all
    values as attributes, so other files can easily use them
    like Settings().spark_app_name
    """

    def __init__(self, config_path: str = None):
        project_root = Path(__file__).resolve().parent.parent

        if config_path is None:
            config_path = os.getenv("BOOKING_CONFIG_PATH")

        if config_path is None:
            candidate_paths = [
                project_root / "config" / "config.toml",
                project_root / "config.toml",
                Path(__file__).parent / "config.toml",
            ]
        else:
            candidate_paths = [Path(config_path)]

        resolved_config_path = None
        for candidate in candidate_paths:
            if not candidate.is_absolute():
                candidate = project_root / candidate
            if candidate.exists():
                resolved_config_path = candidate.resolve()
                break

        if resolved_config_path is None:
            raise FileNotFoundError(
                "Could not find config.toml. Provide BOOKING_CONFIG_PATH or pass config_path."
            )

        config_path = resolved_config_path
        project_root = config_path.parent.parent

        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        # Spark related settings
        self.spark_app_name = config["spark"]["app_name"]
        self.spark_master = config["spark"]["master"]

        # Warehouse related settings
        warehouse_path = Path(config["warehouse"]["path"])
        self.warehouse_path = str(
            warehouse_path if warehouse_path.is_absolute() else project_root / warehouse_path
        )
        self.catalog_name = config["warehouse"]["catalog_name"]

        # Iceberg table related settings
        self.iceberg_database = config["iceberg"]["database"]
        self.iceberg_table_name = config["iceberg"]["table_name"]

        # Raw data path
        raw_json_path = Path(config["data"]["raw_json_path"])
        self.raw_json_path = str(
            raw_json_path if raw_json_path.is_absolute() else project_root / raw_json_path
        )