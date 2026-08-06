from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.exceptions import SchemaValidationError, DataQualityError
from utils.logger import get_logger

logger = get_logger(__name__)


class BookingSchemaValidator:
    """
    Validates that the raw extracted DataFrame has the columns we
    expect, AND that each column's data type matches what we expect,
    before we start transforming it.
    """

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

    REVENUE_SOURCE_FIELD = "commission.estimate_commission_amount.booker_currency"
    REVENUE_SOURCE_TYPE = "double"

    def validate(self, df: DataFrame) -> None:
        self._check_not_empty(df)
        self._check_required_columns(df)
        self._check_column_types(df)
        self._check_revenue_field(df)
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

    def _check_revenue_field(self, df: DataFrame) -> None:
        try:
            resolved_field = df.select(F.col(self.REVENUE_SOURCE_FIELD)).schema[0]
        except Exception:
            raise SchemaValidationError(
                f"Revenue source field '{self.REVENUE_SOURCE_FIELD}' does not exist in raw data."
            )

        actual_type = resolved_field.dataType.typeName()
        if actual_type != self.REVENUE_SOURCE_TYPE:
            raise SchemaValidationError(
                f"Revenue source field '{self.REVENUE_SOURCE_FIELD}' expected "
                f"'{self.REVENUE_SOURCE_TYPE}', got '{actual_type}'"
            )


class BookingDataQualityValidator:
    """
    Checks the ACTUAL VALUES in the transformed (final) DataFrame -
    not the structure/schema, but whether the data itself makes
    business sense.

    This runs AFTER transformer.transform(), because transaction_id,
    currency, and revenue only exist in the final output - they
    don't exist as-is in the raw data.

    Checks performed:
    1. No duplicate transaction_id (each booking should be unique)
    2. revenue and revenue_usd are not negative
    3. currency is a valid 3-letter code (e.g. "EUR", "USD")
    4. revenue is a real number (not null, not NaN)
    """

    VALID_CURRENCY_PATTERN = r"^[A-Z]{3}$"

    def validate(self, df: DataFrame) -> None:
        self._check_duplicate_ids(df)
        self._check_negative_revenue(df)
        self._check_invalid_currency(df)
        self._check_null_or_nan_revenue(df)
        logger.info("Data quality validation passed.")

    def _check_duplicate_ids(self, df: DataFrame) -> None:
        duplicate_count = (
            df.groupBy("transaction_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_count > 0:
            raise DataQualityError(
                f"Found {duplicate_count} duplicate transaction_id value(s) in transformed data."
            )

    def _check_negative_revenue(self, df: DataFrame) -> None:
        negative_count = df.filter(
            (F.col("revenue") < 0) | (F.col("revenue_usd") < 0)
        ).count()

        if negative_count > 0:
            raise DataQualityError(
                f"Found {negative_count} row(s) with negative revenue or revenue_usd."
            )

    def _check_invalid_currency(self, df: DataFrame) -> None:
        invalid_count = df.filter(
            ~F.col("currency").rlike(self.VALID_CURRENCY_PATTERN)
        ).count()

        if invalid_count > 0:
            raise DataQualityError(
                f"Found {invalid_count} row(s) with an invalid currency code "
                f"(expected a 3-letter uppercase code like 'EUR' or 'USD')."
            )

    def _check_null_or_nan_revenue(self, df: DataFrame) -> None:
        bad_count = df.filter(
            F.col("revenue").isNull() | F.isnan(F.col("revenue"))
        ).count()

        if bad_count > 0:
            raise DataQualityError(
                f"Found {bad_count} row(s) with a missing or invalid (NaN) revenue value."
            )