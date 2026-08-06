class BookingPipelineError(Exception):
    """
    Base exception for this project. All custom exceptions below
    inherit from this, so if needed, we can catch ANY pipeline
    error with a single except BookingPipelineError block.
    """
    pass


class DataExtractionError(BookingPipelineError):
    """Raised when reading the raw JSON data fails."""
    pass


class SchemaValidationError(BookingPipelineError):
    """Raised when the extracted data does not match the expected schema."""
    pass


class ExchangeRateFetchError(BookingPipelineError):
    """Raised when fetching the currency exchange rate fails."""
    pass


class IcebergWriteError(BookingPipelineError):
    """Raised when writing data to the Iceberg table fails."""
    pass

class DataQualityError(BookingPipelineError):
    """Raised when transformed data fails business/data quality checks
    (e.g. duplicate IDs, negative revenue, invalid currency)."""
    pass