from pyspark.sql import SparkSession, DataFrame, functions as F

from config.settings import Settings
from utils.logger import get_logger
from utils.exceptions import IcebergWriteError

logger = get_logger(__name__)


class IcebergRepository:
    """
    Repository responsible for all read/write operations on the
    local Iceberg booking table.

    This class only knows HOW to talk to Iceberg (create and
    merge/upsert). It does NOT know anything about business logic
    (that lives in transformer.py).
    """

    def __init__(self, spark: SparkSession, settings: Settings):
        self.spark = spark
        self.settings = settings

        self.catalog_name = settings.catalog_name
        self.database = settings.iceberg_database
        self.table_name = settings.iceberg_table_name
        self.full_table_name = f"{self.catalog_name}.{self.database}.{self.table_name}"

    # ------------------------------------------------------------------
    # EXISTENCE / SETUP
    # ------------------------------------------------------------------

    def table_exists(self) -> bool:
        """Return True if the target Iceberg table already exists."""
        tables_df = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{self.database}")
        existing_table = tables_df.filter(F.col("tableName") == self.table_name)
        return existing_table.limit(1).count() > 0

    def _ensure_namespace_exists(self) -> None:
        """Create the Iceberg namespace/database if it does not already exist."""
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog_name}.{self.database}")

    # ------------------------------------------------------------------
    # WRITE OPERATIONS
    # ------------------------------------------------------------------

    def _repartition_by_partition_column(self, df: DataFrame) -> DataFrame:
        """
        Repartitions the Spark DataFrame (in-memory, before writing)
        using the SAME column the Iceberg table is partitioned by
        (settings.iceberg_partition_column).
        """
        partition_column = self.settings.iceberg_partition_column
        return df.repartition(F.col(partition_column))

    def create_table(self, df: DataFrame) -> None:
        """
        Creates a new Iceberg table using the schema/data of df (CTAS),
        partitioned by settings.iceberg_partition_column.
        """
        self._ensure_namespace_exists()

        df = self._repartition_by_partition_column(df)

        temp_view_name = "__iceberg_repository_create_temp"
        df.createOrReplaceTempView(temp_view_name)

        partition_column = self.settings.iceberg_partition_column

        try:
            self.spark.sql(
                f"""
                CREATE TABLE {self.full_table_name}
                USING iceberg
                PARTITIONED BY ({partition_column})
                AS SELECT * FROM {temp_view_name}
                """
            )
            logger.info(
                f"Created new Iceberg table: {self.full_table_name} "
                f"(partitioned by {partition_column})"
            )
        except Exception as e:
            raise IcebergWriteError(f"Failed to create table {self.full_table_name}: {e}") from e
        finally:
            self.spark.catalog.dropTempView(temp_view_name)

    def merge(self, df: DataFrame, merge_key: str = "transaction_id") -> None:
        """
        Upserts data: updates rows that already exist (matched by
        merge_key), inserts rows that don't exist yet.
        """
        df = self._repartition_by_partition_column(df)

        temp_view_name = "__iceberg_repository_merge_temp"
        df.createOrReplaceTempView(temp_view_name)

        try:
            self.spark.sql(
                f"""
                MERGE INTO {self.full_table_name} AS target
                USING {temp_view_name} AS source
                ON target.{merge_key} = source.{merge_key}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
            logger.info(f"Merged data into {self.full_table_name} using key '{merge_key}'")
        except Exception as e:
            raise IcebergWriteError(f"Failed to merge into {self.full_table_name}: {e}") from e
        finally:
            self.spark.catalog.dropTempView(temp_view_name)

    def write(self, df: DataFrame) -> None:
        """
        Main entry point used by the pipeline: creates the table if
        it doesn't exist yet, otherwise MERGES (upserts) the data.
        """
        self._ensure_namespace_exists()

        if self.table_exists():
            self.merge(df)
        else:
            self.create_table(df)
