from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from sqlalchemy import create_engine, text

from config.settings import Settings
from infra.spark_session import SparkSessionFactory
from service.extractor import BookingExtractor
from service.transformer import BookingTransformer
from service.exchange_rate_service import ExchangeRateService
from repository.iceberg_repository import IcebergRepository
from utils.mapping_loader import MappingLoader
from utils.logger import get_logger

logger = get_logger(__name__)

BATCH_SIZE = 100_000
NUM_BATCHES = 5
POSTGRES_URL = "postgresql://booking_user:booking_pass@localhost:5435/booking_db"
POSTGRES_TABLE = "corn-data-table"
SNAPSHOT_TRACKING_TABLE = "snapshot_tracking"


def ensure_snapshot_tracking_table(engine) -> None:
    """
    Creates the snapshot_tracking table if it doesn't exist yet.
    This table keeps a log of every time we insert data into
    Postgres, along with which Iceberg snapshot that data came from.
    """
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {SNAPSHOT_TRACKING_TABLE} (
            id SERIAL PRIMARY KEY,
            snapshot_id BIGINT NOT NULL,
            batch_label VARCHAR(50) NOT NULL,
            record_count BIGINT NOT NULL,
            status VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))
    logger.info(f"Ensured tracking table '{SNAPSHOT_TRACKING_TABLE}' exists.")


def record_snapshot(engine, snapshot_id: int, batch_label: str, record_count: int) -> None:
    """
    Inserts one row into snapshot_tracking, logging which snapshot_id
    was just inserted into Postgres, how many records, and marks it
    as 'done' since the insert into the main table already succeeded.
    """
    insert_sql = f"""
        INSERT INTO {SNAPSHOT_TRACKING_TABLE} (snapshot_id, batch_label, record_count, status)
        VALUES (:snapshot_id, :batch_label, :record_count, 'done')
    """
    with engine.begin() as conn:
        conn.execute(
            text(insert_sql),
            {"snapshot_id": snapshot_id, "batch_label": batch_label, "record_count": record_count},
        )
    logger.info(f"Recorded snapshot_id {snapshot_id} ({batch_label}, {record_count} rows) in '{SNAPSHOT_TRACKING_TABLE}'.")


def main() -> None:
    settings = Settings()

    usd_rate = ExchangeRateService().get_rate("EUR", "USD")
    logger.info(f"Fetched exchange rate (EUR -> USD): {usd_rate}")

    spark = SparkSessionFactory(settings).create_spark_session()

    extractor = BookingExtractor(spark)
    transformer = BookingTransformer(spark, MappingLoader())
    repository = IcebergRepository(spark, settings)
    engine = create_engine(POSTGRES_URL)

    ensure_snapshot_tracking_table(engine)

    raw_df = extractor.extract(settings.raw_json_path)
    total_rows = raw_df.count()
    logger.info(f"Total raw rows found: {total_rows}")

    # --- ACTUAL PYSPARK PARTITIONING (for processing, not Iceberg) ---
    raw_df_partitioned = raw_df.repartition(NUM_BATCHES).withColumn(
        "batch_partition_id", F.spark_partition_id()
    )
    raw_df_partitioned.cache()

    snapshot_ids = []

    for batch_index in range(NUM_BATCHES):
        logger.info(f"--- Processing batch {batch_index + 1}/{NUM_BATCHES} (Spark partition {batch_index}) ---")

        batch_df = raw_df_partitioned.filter(
            F.col("batch_partition_id") == batch_index
        ).drop("batch_partition_id")

        transformed_batch = transformer.transform(batch_df, usd_rate)
        transformed_batch = spark.createDataFrame(transformed_batch.rdd, transformed_batch.schema)

        repository.write(transformed_batch)

        latest_snapshot = repository.snapshot_history().orderBy(F.col("made_current_at").desc()).first()
        snapshot_id = latest_snapshot["snapshot_id"]
        snapshot_ids.append(snapshot_id)
        logger.info(f"Batch {batch_index + 1} loaded into Iceberg. snapshot_id: {snapshot_id}")

        if batch_index == 0:
            first_batch_pandas = transformed_batch.toPandas()
            first_batch_pandas.to_sql(POSTGRES_TABLE, engine, if_exists="replace", index=False)
            logger.info(f"First batch ({len(first_batch_pandas)} rows) inserted into Postgres table '{POSTGRES_TABLE}'.")
            record_snapshot(engine, snapshot_id, "first", len(first_batch_pandas))

    first_snapshot_id = snapshot_ids[0]
    last_snapshot_id = snapshot_ids[-1]
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
    incremental_pandas.to_sql(POSTGRES_TABLE, engine, if_exists="append", index=False)
    logger.info(f"Inserted {len(incremental_pandas)} incremental rows into Postgres table '{POSTGRES_TABLE}'.")

    record_snapshot(engine, last_snapshot_id, "last", len(incremental_pandas))

    logger.info("All batches processed and synced to Postgres.")


if __name__ == "__main__":
    main()