from pyspark.sql import functions as F

from utils.logger import get_logger

logger = get_logger(__name__)


class IncrementalSyncer:
    """
    Responsible for ONE thing: after all batches are loaded, find
    the data that changed between the first and last snapshot, and
    sync that delta into Postgres.

    Does NOT know how batches were loaded - that's BatchLoader's job.
    """

    def __init__(self, components, engine, tracker, postgres_table: str):
        self.components = components
        self.engine = engine
        self.tracker = tracker
        self.postgres_table = postgres_table

    def sync(self, first_snapshot_id: int, job_start_time) -> None:
        last_snapshot_id = self._find_last_snapshot_id(job_start_time)

        logger.info(f"First snapshot_id: {first_snapshot_id}")
        logger.info(f"Last snapshot_id: {last_snapshot_id}")

        incremental_df = self._read_incremental_changes(first_snapshot_id, last_snapshot_id)
        self._sync_to_postgres(incremental_df, last_snapshot_id)

    def _find_last_snapshot_id(self, job_start_time):
        """
        Uses Iceberg's built-in history table to find the most recent
        snapshot, instead of manually tracking it in a variable during
        the loading loop. The job_start_time filter is important -
        without it, we could accidentally pick up an old snapshot from
        a PREVIOUS run if this table already existed before today.
        """
        repository = self.components.repository

        history_since_job_start = repository.snapshot_history().filter(
            F.col("made_current_at") >= F.lit(job_start_time)
        )
        last_snapshot_row = history_since_job_start.orderBy(F.col("made_current_at").desc()).first()
        return last_snapshot_row["snapshot_id"]

    def _read_incremental_changes(self, first_snapshot_id: int, last_snapshot_id: int):
        """
        Reads only the rows added between first_snapshot_id and
        last_snapshot_id using Iceberg's changelog table (we use
        merge() per batch, so plain append-based incremental reads
        won't work here - the changelog tracks all change types).
        """
        spark = self.components.spark
        repository = self.components.repository

        changelog_df = (
            spark.read.format("iceberg")
            .option("start-snapshot-id", first_snapshot_id)
            .option("end-snapshot-id", last_snapshot_id)
            .load(f"{repository.full_table_name}.changes")
        )

        # Materialize before filtering to avoid invalid predicate
        # pushdown of the _change_type metadata column.
        changelog_df = spark.createDataFrame(changelog_df.rdd, changelog_df.schema)

        incremental_df = changelog_df.filter(F.col("_change_type") == "INSERT").drop(
            "_change_type", "_change_ordinal", "_commit_snapshot_id"
        )

        incremental_count = incremental_df.count()
        logger.info(f"Incremental rows found between first and last snapshot: {incremental_count}")

        return incremental_df

    def _sync_to_postgres(self, incremental_df, last_snapshot_id: int) -> None:
        incremental_pandas = incremental_df.toPandas()
        incremental_pandas.to_sql(self.postgres_table, self.engine, if_exists="append", index=False)
        logger.info(f"Inserted {len(incremental_pandas)} incremental rows into Postgres table '{self.postgres_table}'.")

        self.tracker.record(last_snapshot_id, "last", len(incremental_pandas))