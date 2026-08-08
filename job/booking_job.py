from pathlib import Path
import argparse
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from job.common import PipelineComponentsBuilder
from job.batch_load_job import BatchMigrationRunner
from utils.logger import get_logger
from utils.exceptions import (
    BookingPipelineError,
    DataExtractionError,
    IcebergWriteError,
)

logger = get_logger(__name__)


class BookingJobRunner:
    """
    Normal, day-to-day pipeline run: extract -> validate -> transform
    -> validate -> load. Runs every time booking_job.py is called
    with no flags.
    """

    def __init__(self, components=None):
        self.components = components or PipelineComponentsBuilder().build()

    def run(self) -> None:
        settings = self.components.settings
        extractor = self.components.extractor
        repository = self.components.repository

        # --- EXTRACT ---
        try:
            raw_df = extractor.extract(settings.raw_json_path)
            logger.info(f"Extracted {raw_df.count()} raw record(s) from {settings.raw_json_path}")
        except Exception as e:
            raise DataExtractionError(f"Failed to extract raw data: {e}") from e

        # --- VALIDATE RAW DATA (structure/schema check) ---
        self.components.schema_validator.validate(raw_df)

        # --- TRANSFORM (inside the Spark DAG) ---
        transformed_df = self.components.transformer.transform(raw_df, self.components.usd_rate)
        logger.info("Transformation complete.")

        # --- VALIDATE TRANSFORMED DATA (value/business rule check) ---
        self.components.data_quality_validator.validate(transformed_df)

        # --- LOAD ---
        try:
            repository.write(transformed_df)
            logger.info(f"Successfully wrote data to Iceberg table: {repository.full_table_name}")
        except Exception as e:
            raise IcebergWriteError(f"Failed to write to Iceberg table: {e}") from e


class BookingJobCLI:
    """
    Single terminal entry point for the whole pipeline.
    No flag       -> BookingJobRunner  (normal load)
    --batch flag  -> BatchMigrationRunner (batch migration + Postgres sync)
    """

    def parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(
            description="Booking ETL pipeline - single entry point for both "
                         "normal runs and one-off batch migrations."
        )
        parser.add_argument(
            "--batch",
            action="store_true",
            help="Run the batch migration flow instead of the normal "
                 "single-load run. Use this only when a migration is "
                 "needed - everyday runs should use no flag at all.",
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