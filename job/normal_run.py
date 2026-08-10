from job.common import PipelineComponentsBuilder
from utils.logger import get_logger
from utils.exceptions import DataExtractionError, IcebergWriteError

logger = get_logger(__name__)


class BookingJobRunner:
    """
    Normal, day-to-day pipeline run: extract -> validate -> transform
    -> validate -> load. Runs every time `python job/cli.py` is
    called with no flags. Only writes to Iceberg - does not touch
    Postgres at all (that's what the batch migration run is for).
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

        # --- LOAD (Iceberg only) ---
        try:
            repository.write(transformed_df)
            logger.info(f"Successfully wrote data to Iceberg table: {repository.full_table_name}")
        except Exception as e:
            raise IcebergWriteError(f"Failed to write to Iceberg table: {e}") from e