from pyspark.sql import SparkSession

from config.settings import Settings


class SparkSessionFactory:
    """
    This class is responsible for creating a SparkSession
    that is configured to work with a local Iceberg catalog.

    Why a separate class for this?
    - Creating a SparkSession with Iceberg config is a bit long.
    - Keeping it in one place means if we ever change catalog
      settings, we only change this one file.
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def create_spark_session(self) -> SparkSession:
        settings = self.settings

        # Iceberg needs a runtime package so Spark understands
        # how to read/write the Iceberg table format.
        # Version below matches Spark 3.5.x with Scala 2.12.
        iceberg_package = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"

        spark = (
            SparkSession.builder
            .appName(settings.spark_app_name)
            .master(settings.spark_master)

            # Tell Spark to download and use the Iceberg runtime jar
            .config("spark.jars.packages", iceberg_package)

            # Enable Iceberg SQL extensions (needed for commands like
            # CREATE TABLE ... USING iceberg, MERGE INTO, etc.)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )

            # Register a catalog named "local" that Spark will use
            # whenever we write a table like local.booking_db.bookings
            .config(
                f"spark.sql.catalog.{settings.catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )

            # "hadoop" type catalog means Iceberg will store everything
            # as plain files on our local filesystem (no S3, no Hive metastore)
            .config(
                f"spark.sql.catalog.{settings.catalog_name}.type",
                "hadoop",
            )

            # This is the actual local folder where Iceberg will save
            # both data files and metadata files
            .config(
                f"spark.sql.catalog.{settings.catalog_name}.warehouse",
                settings.warehouse_path,
            )

            .getOrCreate()
        )

        return spark