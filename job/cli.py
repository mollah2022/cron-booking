from pathlib import Path
import argparse
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from job.normal_run import BookingJobRunner
from job.batch_migration.runner import BatchMigrationRunner
from utils.logger import get_logger
from utils.exceptions import BookingPipelineError

logger = get_logger(__name__)


class BookingJobCLI:
    def parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(
            description="Booking ETL pipeline - single entry point for both "
                         "normal runs and one-off batch migrations."
        )
        parser.add_argument(
            "--batch",
            action="store_true",
            help="Run the batch migration flow instead of the normal "
                 "single-load run.",
        )
        return parser.parse_args()

    def main(self) -> None:
        args = self.parse_args()

        if args.batch:
            logger.info("Running in BATCH MIGRATION mode.")
            BatchMigrationRunner().run()
        else:
            logger.info("Running in NORMAL (single-load) mode.")
            BookingJobRunner().run()


if __name__ == "__main__":
    try:
        BookingJobCLI().main()
    except BookingPipelineError as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        sys.exit(1)