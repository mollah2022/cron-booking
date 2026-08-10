from pyspark.sql import functions as F
from utils.logger import get_logger

logger = get_logger(__name__)


class IncrementalSyncer:
    def __init__(self, components, engine, tracker, postgres_table: str):
        self.components = components
        self.engine = engine
        self.tracker = tracker
        self.postgres_table = postgres_table

    def sync(self, first_snapshot_id: int, last_snapshot_id: int) -> None:
        logger.info(f"First snapshot_id: {first_snapshot_id}")
        logger.info(f"Last snapshot_id: {last_snapshot_id}")

        spark = self.components.spark
        repository = self.components.repository

        # Iceberg's built-in changelog metadata table, queried via SQL
        query = f"""
            SELECT *
            FROM {repository.full_table_name}.changes
        """
        changelog_df = (
            spark.read.format("iceberg")
            .option("start-snapshot-id", first_snapshot_id)
            .option("end-snapshot-id", last_snapshot_id)
            .load(f"{repository.full_table_name}.changes")
        )
        changelog_df = spark.createDataFrame(changelog_df.rdd, changelog_df.schema)
        changelog_df.createOrReplaceTempView("__changelog_temp")

        incremental_df = spark.sql("""
            SELECT * FROM __changelog_temp WHERE _change_type = 'INSERT'
        """).drop("_change_type", "_change_ordinal", "_commit_snapshot_id")

        logger.info(f"Incremental rows found: {incremental_df.count()}")

        incremental_pandas = incremental_df.toPandas()
        incremental_pandas.to_sql(self.postgres_table, self.engine, if_exists="append", index=False)
        logger.info(f"Inserted {len(incremental_pandas)} incremental rows into Postgres.")

        self.tracker.record(last_snapshot_id, "last", len(incremental_pandas))