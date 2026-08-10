from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.logger import get_logger

logger = get_logger(__name__)


class SnapshotTracker:
    """
    Handles the Postgres snapshot_tracking table: creating it and
    recording a row every time a batch is synced from Iceberg to
    Postgres, so there's an audit trail of which snapshot went where.
    """

    TABLE_NAME = "snapshot_tracking"

    def __init__(self, engine: Engine):
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