from pyspark.sql import DataFrame

from utils.exceptions import SchemaValidationError
from utils.logger import get_logger

logger = get_logger(__name__)


class BookingSchemaValidator:
    """
    Validates that the raw extracted DataFrame has the columns we
    expect, AND that each column's data type matches what we expect,
    before we start transforming it.

    Why validate both column existence AND data type?
    - A missing column is obvious and easy to catch.
    - A WRONG data type is more dangerous - e.g. if "start" suddenly
      comes as a timestamp instead of a string, or "id" comes as a
      long instead of a string, downstream code (to_date, string
      concatenation, etc) can silently produce wrong results instead
      of a clear error. Checking type upfront catches this early.
    """

    # Top-level columns that must exist, with their expected Spark type.
    REQUIRED_COLUMNS = {
        "id": "string",
        "accommodations": "struct",
        "accommodation_details": "struct",
        "booker": "struct",
        "commission": "struct",
        "currencies": "struct",
        "start": "string",
        "end": "string",
        "label": "string",
        "price": "struct",
        "status": "string",
    }

    def validate(self, df: DataFrame) -> None:
        self._check_not_empty(df)
        self._check_required_columns(df)
        self._check_column_types(df)
        logger.info("Schema validation passed (columns + data types).")

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
                f"Expected columns: {list(self.REQUIRED_COLUMNS.keys())}"
            )

    def _check_column_types(self, df: DataFrame) -> None:
        """
        Checks that each top-level column's actual Spark data type
        matches what we expect (e.g. "string", "struct").
        """
        mismatches = []

        for column, expected_type in self.REQUIRED_COLUMNS.items():
            actual_field = df.schema[column]
            actual_type = actual_field.dataType.typeName()

            if expected_type == "struct":
                if actual_type != "struct":
                    mismatches.append(
                        f"'{column}' expected struct, got '{actual_type}'"
                    )
            else:
                if actual_type != expected_type:
                    mismatches.append(
                        f"'{column}' expected '{expected_type}', got '{actual_type}'"
                    )

        if mismatches:
            raise SchemaValidationError(
                f"Column data type mismatch in raw data: {mismatches}"
            )