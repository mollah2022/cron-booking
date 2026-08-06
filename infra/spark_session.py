from pyspark.sql import SparkSession

from config.settings import Settings


class SparkSessionFactory:
    """
    This class is responsible for creating a SparkSession
    that is configured to work with a local Iceberg catalog.
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def create_spark_session(self) -> SparkSession:
        settings = self.settings

        iceberg_package = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"

        spark = (
            SparkSession.builder
            .appName(settings.spark_app_name)
            .master(settings.spark_master)
            .config("spark.jars.packages", iceberg_package)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{settings.catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(
                f"spark.sql.catalog.{settings.catalog_name}.type",
                "hadoop",
            )
            .config(
                f"spark.sql.catalog.{settings.catalog_name}.warehouse",
                settings.warehouse_path,
            )
            .getOrCreate()
        )

        return spark
