from pyspark.sql import functions as F

from utils.logger import get_logger

logger = get_logger(__name__)


class BatchLoader:
    """
    Responsible for ONE thing: splitting the raw data into batches
    and loading each batch into Iceberg. Also inserts the very first
    batch directly into Postgres (the "starting point" data).

    Does NOT know anything about incremental syncing of later
    batches - that's IncrementalSyncer's job.
    """

    def __init__(self, components, engine, tracker, num_batches: int, postgres_table: str):
        self.components = components
        self.engine = engine
        self.tracker = tracker
        self.num_batches = num_batches
        self.postgres_table = postgres_table

    def run(self):
        """
        Loads all batches into Iceberg.
        Returns (first_snapshot_id, job_start_time) - both needed
        later by IncrementalSyncer to find the delta.
        """
        spark = self.components.spark
        extractor = self.components.extractor
        transformer = self.components.transformer
        repository = self.components.repository
        usd_rate = self.components.usd_rate
        settings = self.components.settings

        job_start_time = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]

        raw_df = extractor.extract(settings.raw_json_path)
        total_rows = raw_df.count()
        logger.info(f"Total raw rows found: {total_rows}")

        # --- ACTUAL PYSPARK PARTITIONING (for processing, not Iceberg) ---
        # No .cache() here - the raw dataset is too large to fit in
        # default Spark memory, and caching it caused "Not enough
        # space to cache" warnings.
        raw_df_partitioned = raw_df.repartition(self.num_batches).withColumn(
            "batch_partition_id", F.spark_partition_id()
        )

        first_snapshot_id = None

        for batch_index in range(self.num_batches):
            logger.info(f"--- Processing batch {batch_index + 1}/{self.num_batches} (Spark partition {batch_index}) ---")

            batch_df = raw_df_partitioned.filter(
                F.col("batch_partition_id") == batch_index
            ).drop("batch_partition_id")

            transformed_batch = transformer.transform(batch_df, usd_rate)

            # Materialize into a fresh DataFrame - avoids non-deterministic
            # lineage issues when Iceberg's MERGE INTO runs.
            transformed_batch = spark.createDataFrame(transformed_batch.rdd, transformed_batch.schema)

            repository.write(transformed_batch)

            latest_snapshot = repository.snapshot_history().orderBy(F.col("made_current_at").desc()).first()
            snapshot_id = latest_snapshot["snapshot_id"]
            logger.info(f"Batch {batch_index + 1} loaded into Iceberg. snapshot_id: {snapshot_id}")

            if batch_index == 0:
                first_snapshot_id = snapshot_id
                self._insert_first_batch_to_postgres(transformed_batch, snapshot_id)

        return first_snapshot_id, job_start_time

    def _insert_first_batch_to_postgres(self, transformed_batch, snapshot_id: int) -> None:
        first_batch_pandas = transformed_batch.toPandas()
        first_batch_pandas.to_sql(self.postgres_table, self.engine, if_exists="replace", index=False)
        logger.info(f"First batch ({len(first_batch_pandas)} rows) inserted into Postgres table '{self.postgres_table}'.")

        self.tracker.record(snapshot_id, "first", len(first_batch_pandas))