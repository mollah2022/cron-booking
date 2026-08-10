from sqlalchemy import create_engine

from job.common import PipelineComponentsBuilder
from job.batch_migration.snapshot_tracker import SnapshotTracker
from job.batch_migration.batch_loader import BatchLoader
from job.batch_migration.incremental_sync import IncrementalSyncer
from utils.logger import get_logger

logger = get_logger(__name__)


class BatchMigrationRunner:
    """
    Small orchestrator for the batch migration flow. Delegates the
    actual work to BatchLoader (load batches into Iceberg + first
    batch into Postgres) and IncrementalSyncer (sync the delta into
    Postgres afterwards).

    Triggered from job/cli.py via `python job/cli.py --batch`.
    """

    NUM_BATCHES = 5
    POSTGRES_URL = "postgresql://booking_user:booking_pass@localhost:5435/booking_db"
    POSTGRES_TABLE = "corn-data-table"

    def __init__(self, components=None):
        self.components = components or PipelineComponentsBuilder().build()
        self.engine = create_engine(self.POSTGRES_URL)
        self.tracker = SnapshotTracker(self.engine)

        self.loader = BatchLoader(
            self.components, self.engine, self.tracker,
            num_batches=self.NUM_BATCHES, postgres_table=self.POSTGRES_TABLE,
        )
        self.syncer = IncrementalSyncer(
            self.components, self.engine, self.tracker,
            postgres_table=self.POSTGRES_TABLE,
        )

    def run(self) -> None:
        self.tracker.ensure_table()

        first_snapshot_id, job_start_time = self.loader.run()
        self.syncer.sync(first_snapshot_id, job_start_time)

        logger.info("All batches processed and synced to Postgres.")