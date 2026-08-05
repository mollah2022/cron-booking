from pyspark.sql import SparkSession, DataFrame, functions as F
from config.settings import Settings


class IcebergRepository:
    """
    Repository responsible for saving transformed booking data into
    a local Iceberg table using the configured catalog and warehouse.
    """

    def __init__(self, spark: SparkSession, settings: Settings):
        self.spark = spark
        self.settings = settings

        self.catalog_name = settings.catalog_name
        self.database = settings.iceberg_database
        self.table_name = settings.iceberg_table_name
        self.full_table_name = f"{self.catalog_name}.{self.database}.{self.table_name}"

    def table_exists(self) -> bool:
        """Return True if the target Iceberg table already exists."""
        tables_df = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{self.database}")
        existing_table = tables_df.filter(F.col("tableName") == self.table_name)
        return existing_table.limit(1).count() > 0

    def _ensure_namespace_exists(self) -> None:
        """Create the Iceberg namespace/database if it does not already exist."""
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog_name}.{self.database}")

    def write(self, df: DataFrame) -> None:
        """Save the DataFrame to the Iceberg table.

        If the table does not exist, it is created using Iceberg.
        If the table already exists, new rows are appended.
        """
        self._ensure_namespace_exists()

        if self.table_exists():
            self._append(df)
        else:
            self._create_table(df)

    def _create_table(self, df: DataFrame) -> None:
        temp_view_name = "__iceberg_repository_create_temp"
        df.createOrReplaceTempView(temp_view_name)

        self.spark.sql(
            f"""
            CREATE TABLE {self.full_table_name}
            USING iceberg
            AS SELECT * FROM {temp_view_name}
            """
        )

        self.spark.catalog.dropTempView(temp_view_name)

    def _append(self, df: DataFrame) -> None:
        df.write.format("iceberg").mode("append").save(self.full_table_name)
