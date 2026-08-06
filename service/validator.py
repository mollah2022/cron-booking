from pyspark.sql import DataFrame

from utils.exceptions import SchemaValidationError
from utils.logger import get_logger

logger = get_logger(__name__)


class BookingSchemaValidator:
    """
    Validates that the raw extracted DataFrame has the columns
    we expect before we start transforming it.

    Why validate right after extraction?
    - If the raw JSON structure ever changes (a field gets removed
      or renamed by the data source), we want to catch that
      IMMEDIATELY, with a clear error - not silently get NULLs
      deep inside the transformer.
    """

    REQUIRED_COLUMNS = [
        "id",
        "accommodations",
        "accommodation_details",
        "booker",
        "commission",
        "currencies",
        "start",
        "end",
        "label",
        "price",
        "status",
    ]

    def validate(self, df: DataFrame) -> None:
        self._check_not_empty(df)
        self._check_required_columns(df)
        logger.info("Schema validation passed.")

    def _check_not_empty(self, df: DataFrame) -> None:
        if df.rdd.isEmpty():
            raise SchemaValidationError(
                "Extracted DataFrame is empty. No records to process."
            )

    def _check_required_columns(self, df: DataFrame) -> None:
        actual_columns = set(df.columns)
        missing_columns = [
            col for col in self.REQUIRED_COLUMNS if col not in actual_columns
        ]

        if missing_columns:
            raise SchemaValidationError(
                f"Missing required columns in raw data: {missing_columns}. "
                f"Expected columns: {self.REQUIRED_COLUMNS}"
            )