from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from pyspark.sql import functions as F
from sqlalchemy import create_engine, text

from job.common import PipelineComponentsBuilder
from utils.logger import get_logger

logger = get_logger(__name__)


class SnapshotTracker:
    """
    Handles the Postgres snapshot_tracking table: creating it and
    recording a row every time a batch is synced from Iceberg to
    Postgres, so there's an audit trail of which snapshot went where.
    """

    TABLE_NAME = "snapshot_tracking"

    def __init__(self, engine):
        self.engine = engine

    def ensure_table(self) -> None:
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                snapshot_id BIGINT NOT NULL,
                batch_label VARCHAR(50) NOT NULL,
                record_count BIGINT NOT NULL,
                status VARCHAR(20) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """
        with self.engine.begin() as conn:
            conn.execute(text(create_sql))
        logger.info(f"Ensured tracking table '{self.TABLE_NAME}' exists.")

    def record(self, snapshot_id: int, batch_label: str, record_count: int) -> None:
        insert_sql = f"""
            INSERT INTO {self.TABLE_NAME} (snapshot_id, batch_label, record_count, status)
            VALUES (:snapshot_id, :batch_label, :record_count, 'done')
        """
        with self.engine.begin() as conn:
            conn.execute(
                text(insert_sql),
                {"snapshot_id": snapshot_id, "batch_label": batch_label, "record_count": record_count},
            )
        logger.info(f"Recorded snapshot_id {snapshot_id} ({batch_label}, {record_count} rows) in '{self.TABLE_NAME}'.")


class BatchMigrationRunner:
    """
    Batch migration flow: splits the raw data into NUM_BATCHES batches,
    loads each batch into Iceberg, and syncs the results into Postgres
    with snapshot tracking.

    Triggered from job/booking_job.py via `python job/booking_job.py --batch`.
    Reuses PipelineComponentsBuilder from job/common.py, so nothing here
    duplicates the normal run's setup code.
    """

    NUM_BATCHES = 5
    POSTGRES_URL = "postgresql://booking_user:booking_pass@localhost:5435/booking_db"
    POSTGRES_TABLE = "corn-data-table"

    def __init__(self, components=None):
        self.components = components or PipelineComponentsBuilder().build()
        self.engine = create_engine(self.POSTGRES_URL)
        self.tracker = SnapshotTracker(self.engine)

    def run(self) -> None:
        settings = self.components.settings
        spark = self.components.spark
        extractor = self.components.extractor
        transformer = self.components.transformer
        repository = self.components.repository
        usd_rate = self.components.usd_rate

        self.tracker.ensure_table()

        job_start_time = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]

        raw_df = extractor.extract(settings.raw_json_path)
        total_rows = raw_df.count()
        logger.info(f"Total raw rows found: {total_rows}")

        # --- ACTUAL PYSPARK PARTITIONING (for processing, not Iceberg) ---
        raw_df_partitioned = raw_df.repartition(self.NUM_BATCHES).withColumn(
            "batch_partition_id", F.spark_partition_id()
        )

        # We only ever need the FIRST and LAST snapshot_id - not every
        first_snapshot_id = None

        for batch_index in range(self.NUM_BATCHES):
            logger.info(f"--- Processing batch {batch_index + 1}/{self.NUM_BATCHES} (Spark partition {batch_index}) ---")

            batch_df = raw_df_partitioned.filter(
                F.col("batch_partition_id") == batch_index
            ).drop("batch_partition_id")

            transformed_batch = transformer.transform(batch_df, usd_rate)
            transformed_batch = spark.createDataFrame(transformed_batch.rdd, transformed_batch.schema)

            repository.write(transformed_batch)

            latest_snapshot = repository.snapshot_history().orderBy(F.col("made_current_at").desc()).first()
            snapshot_id = latest_snapshot["snapshot_id"]
            logger.info(f"Batch {batch_index + 1} loaded into Iceberg. snapshot_id: {snapshot_id}")

            # We still need first_snapshot_id captured live, because on
            # batch 1 the table might not have existed before this run
            # (fresh CREATE TABLE), so Iceberg's history table only
            # starts existing from this exact moment anyway.
            if batch_index == 0:
                first_snapshot_id = snapshot_id

                first_batch_pandas = transformed_batch.toPandas()
                first_batch_pandas.to_sql(self.POSTGRES_TABLE, self.engine, if_exists="replace", index=False)
                logger.info(f"First batch ({len(first_batch_pandas)} rows) inserted into Postgres table '{self.POSTGRES_TABLE}'.")
                self.tracker.record(snapshot_id, "first", len(first_batch_pandas))

        # --- USE ICEBERG'S BUILT-IN HISTORY TABLE FOR THE "LAST" SNAPSHOT ---

        history_since_job_start = repository.snapshot_history().filter(
            F.col("made_current_at") >= F.lit(job_start_time)
        )
        last_snapshot_row = history_since_job_start.orderBy(F.col("made_current_at").desc()).first()
        last_snapshot_id = last_snapshot_row["snapshot_id"]

        logger.info(f"First snapshot_id: {first_snapshot_id}")
        logger.info(f"Last snapshot_id: {last_snapshot_id}")

        changelog_df = (
            spark.read.format("iceberg")
            .option("start-snapshot-id", first_snapshot_id)
            .option("end-snapshot-id", last_snapshot_id)
            .load(f"{repository.full_table_name}.changes")
        )
        changelog_df = spark.createDataFrame(changelog_df.rdd, changelog_df.schema)

        incremental_df = changelog_df.filter(F.col("_change_type") == "INSERT").drop(
            "_change_type", "_change_ordinal", "_commit_snapshot_id"
        )

        incremental_count = incremental_df.count()
        logger.info(f"Incremental rows found between first and last snapshot: {incremental_count}")

        incremental_pandas = incremental_df.toPandas()
        incremental_pandas.to_sql(self.POSTGRES_TABLE, self.engine, if_exists="append", index=False)
        logger.info(f"Inserted {len(incremental_pandas)} incremental rows into Postgres table '{self.POSTGRES_TABLE}'.")

        self.tracker.record(last_snapshot_id, "last", len(incremental_pandas))

        logger.info("All batches processed and synced to Postgres.")


if __name__ == "__main__":
    # Still runnable directly for backward-compat (e.g. an old cron entry),
    # but the recommended way to trigger this is now:
    #   python job/booking_job.py --batch
    BatchMigrationRunner().run()