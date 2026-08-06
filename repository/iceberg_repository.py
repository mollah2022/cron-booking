from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame, functions as F

from config.settings import Settings
from utils.logger import get_logger
from utils.exceptions import IcebergWriteError

logger = get_logger(__name__)


class BaseRepository(ABC):
    """
    Interface (abstract base class) for anything that can store
    and retrieve our booking data.

    Why this exists (Dependency Inversion Principle):
    - job/booking_job.py only needs to know "something with a
      write() and read() method" - it doesn't need to know we are
      specifically using Iceberg.
    - If tomorrow we switch to Delta Lake or plain Parquet tables,
      we create a new class implementing this same interface
      (e.g. DeltaLakeRepository), and booking_job.py barely changes.
    """

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        raise NotImplementedError

    @abstractmethod
    def read(self) -> DataFrame:
        raise NotImplementedError


class IcebergRepository(BaseRepository):
    """
    Repository responsible for all read/write operations on the
    local Iceberg booking table.

    This class only knows HOW to talk to Iceberg (create, append,
    overwrite, merge, delete, optimize, etc). It does NOT know
    anything about business logic (that lives in transformer.py).
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

    def create_table(self, df: DataFrame) -> None:
        """Creates a new Iceberg table using the schema/data of df (CTAS)."""
        self._ensure_namespace_exists()

        temp_view_name = "__iceberg_repository_create_temp"
        df.createOrReplaceTempView(temp_view_name)

        try:
            self.spark.sql(
                f"""
                CREATE TABLE {self.full_table_name}
                USING iceberg
                AS SELECT * FROM {temp_view_name}
                """
            )
            logger.info(f"Created new Iceberg table: {self.full_table_name}")
        except Exception as e:
            raise IcebergWriteError(f"Failed to create table {self.full_table_name}: {e}") from e
        finally:
            self.spark.catalog.dropTempView(temp_view_name)

    def append(self, df: DataFrame) -> None:
        """Appends new rows to the existing table without touching old data."""
        try:
            df.writeTo(self.full_table_name).append()
            logger.info(f"Appended data to {self.full_table_name}")
        except Exception as e:
            raise IcebergWriteError(f"Failed to append to {self.full_table_name}: {e}") from e

    def overwrite(self, df: DataFrame) -> None:
        """Replaces ALL existing data in the table with df's data."""
        try:
            df.writeTo(self.full_table_name).overwritePartitions()
            logger.info(f"Overwrote all data in {self.full_table_name}")
        except Exception as e:
            raise IcebergWriteError(f"Failed to overwrite {self.full_table_name}: {e}") from e

    def merge(self, df: DataFrame, merge_key: str = "transaction_id") -> None:
        """
        Upserts data: updates rows that already exist (matched by
        merge_key), inserts rows that don't exist yet.

        Example: if a booking's status changes from "pending" to
        "cancelled", re-running the job will UPDATE that row instead
        of creating a duplicate.
        """
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
        """Convenience method: create table if missing, else append."""
        self._ensure_namespace_exists()

        if self.table_exists():
            self.append(df)
        else:
            self.create_table(df)

    # ------------------------------------------------------------------
    # DELETE
    # ------------------------------------------------------------------

    def delete(self, condition: str) -> None:
        """
        Deletes rows matching a SQL condition string.
        Example: repo.delete("status = 'cancelled'")
        """
        try:
            self.spark.sql(f"DELETE FROM {self.full_table_name} WHERE {condition}")
            logger.info(f"Deleted rows from {self.full_table_name} WHERE {condition}")
        except Exception as e:
            raise IcebergWriteError(f"Failed to delete from {self.full_table_name}: {e}") from e

    # ------------------------------------------------------------------
    # MAINTENANCE
    # ------------------------------------------------------------------

    def optimize(self) -> None:
        """
        Compacts many small data files into fewer, larger files.
        Important for performance once a table has been written to
        many times (e.g. daily appends over months create lots of
        small files, which slows down reads).
        """
        self.spark.sql(
            f"CALL {self.catalog_name}.system.rewrite_data_files('{self.database}.{self.table_name}')"
        )
        logger.info(f"Optimized (compacted) data files for {self.full_table_name}")

    # ------------------------------------------------------------------
    # READ / INSPECTION
    # ------------------------------------------------------------------

    def read(self) -> DataFrame:
        """Reads the full table and returns it as a DataFrame."""
        return self.spark.table(self.full_table_name)

    def snapshot_history(self) -> DataFrame:
        """
        Returns the Iceberg snapshot history - every write creates a
        new snapshot, so this shows a timeline of changes (used for
        auditing or time-travel queries).
        """
        return self.spark.sql(f"SELECT * FROM {self.full_table_name}.history")

    def explain(self, query: str = None) -> None:
        """
        Prints the Spark execution plan for a query against this
        table. Useful for debugging performance issues.
        If no query is given, explains a simple SELECT * on the table.
        """
        query = query or f"SELECT * FROM {self.full_table_name}"
        self.spark.sql(query).explain(True)

