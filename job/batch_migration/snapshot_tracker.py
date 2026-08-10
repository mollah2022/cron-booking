from utils.logger import get_logger

logger = get_logger(__name__)


class IcebergSnapshotTracker:
    def __init__(self, spark, catalog_name: str, database: str):
        self.spark = spark
        self.full_table_name = f"{catalog_name}.{database}.snapshot_tracking"

    def ensure_table(self) -> None:
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.full_table_name} (
                snapshot_id BIGINT,
                batch_label STRING,
                record_count BIGINT,
                status STRING,
                created_at TIMESTAMP
            )
            USING iceberg
        """
        logger.info(f"[SOURCE QUERY - ensure_table]:\n{query}")
        self.spark.sql(query)

    def record(self, snapshot_id: int, batch_label: str, record_count: int) -> None:
        query = f"""
            INSERT INTO {self.full_table_name}
            VALUES ({snapshot_id}, '{batch_label}', {record_count}, 'done', current_timestamp())
        """
        logger.info(f"[SOURCE QUERY - record]:\n{query}")
        self.spark.sql(query)