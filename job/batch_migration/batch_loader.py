from pyspark.sql import functions as F
from utils.logger import get_logger

logger = get_logger(__name__)


class BatchLoader:
    def __init__(self, components, engine, tracker, num_batches: int, postgres_table: str):
        self.components = components
        self.engine = engine
        self.tracker = tracker
        self.num_batches = num_batches
        self.postgres_table = postgres_table

    def run(self):
        spark = self.components.spark
        extractor = self.components.extractor
        transformer = self.components.transformer
        repository = self.components.repository
        usd_rate = self.components.usd_rate
        settings = self.components.settings

        raw_df = extractor.extract(settings.raw_json_path)
        logger.info(f"Total raw rows found: {raw_df.count()}")

        raw_df_partitioned = raw_df.repartition(self.num_batches).withColumn(
            "batch_partition_id", F.spark_partition_id()
        )

        first_snapshot_id = None

        for batch_index in range(self.num_batches):
            logger.info(f"--- Batch {batch_index + 1}/{self.num_batches} ---")

            batch_df = raw_df_partitioned.filter(
                F.col("batch_partition_id") == batch_index
            ).drop("batch_partition_id")

            transformed_batch = transformer.transform(batch_df, usd_rate)
            transformed_batch = spark.createDataFrame(transformed_batch.rdd, transformed_batch.schema)

            repository.write(transformed_batch)

            if batch_index == 0:
                first_snapshot_id = self._get_snapshot_id_via_sql(spark, repository, order="ASC")
                self._insert_first_batch_to_postgres(transformed_batch, first_snapshot_id)

        last_snapshot_id = self._get_snapshot_id_via_sql(spark, repository, order="DESC")

        return first_snapshot_id, last_snapshot_id

    def _get_snapshot_id_via_sql(self, spark, repository, order: str):
        query = f"""
            SELECT snapshot_id
            FROM {repository.full_table_name}.snapshots
            ORDER BY committed_at {order}
            LIMIT 1
        """
        logger.info(f"[SOURCE QUERY - fetching snapshot_id ({order})]:\n{query}")
        row = spark.sql(query).collect()[0]
        logger.info(f"Snapshot_id obtained from table '{repository.full_table_name}.snapshots': {row['snapshot_id']}")
        return row["snapshot_id"]

    def _insert_first_batch_to_postgres(self, transformed_batch, snapshot_id: int) -> None:
        first_batch_pandas = transformed_batch.toPandas()
        first_batch_pandas.to_sql(self.postgres_table, self.engine, if_exists="replace", index=False)
        logger.info(f"First batch ({len(first_batch_pandas)} rows) inserted into Postgres.")
        self.tracker.record(snapshot_id, "first", len(first_batch_pandas))