from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from config.settings import Settings
from config.spark_session import SparkSessionFactory
from service.extractor import BaseExtractor, BookingExtractor
from service.transformer import BookingTransformer
from service.exchange_rate_service import BaseExchangeRateService, ExchangeRateService
from service.validator import BookingSchemaValidator, BookingDataQualityValidator
from repository.iceberg_repository import BaseRepository, IcebergRepository
from utils.mapping_loader import MappingLoader
from utils.logger import get_logger
from utils.exceptions import (
    BookingPipelineError,
    DataExtractionError,
    ExchangeRateFetchError,
    IcebergWriteError,
)

logger = get_logger(__name__)


def main() -> None:
    settings = Settings()

    # --- OUTSIDE THE SPARK DAG ---
    try:
        rate_service: BaseExchangeRateService = ExchangeRateService()
        usd_rate = rate_service.get_rate("EUR", "USD")
        logger.info(f"Fetched exchange rate (EUR -> USD): {usd_rate}")
    except Exception as e:
        raise ExchangeRateFetchError(f"Failed to fetch exchange rate: {e}") from e

    # --- SPARK SETUP ---
    spark = SparkSessionFactory(settings).create_spark_session()
    logger.info("Spark session created.")

    extractor: BaseExtractor = BookingExtractor(spark)
    schema_validator = BookingSchemaValidator()
    data_quality_validator = BookingDataQualityValidator()
    mapping_loader = MappingLoader()
    transformer = BookingTransformer(spark, mapping_loader)
    repository: BaseRepository = IcebergRepository(spark, settings)

    # --- EXTRACT ---
    try:
        raw_df = extractor.extract(settings.raw_json_path)
        logger.info(f"Extracted {raw_df.count()} raw record(s) from {settings.raw_json_path}")
    except Exception as e:
        raise DataExtractionError(f"Failed to extract raw data: {e}") from e

    # --- VALIDATE RAW DATA (structure/schema check) ---
    schema_validator.validate(raw_df)

    # --- TRANSFORM (inside the Spark DAG) ---
    transformed_df = transformer.transform(raw_df, usd_rate)
    logger.info("Transformation complete.")

    # --- VALIDATE TRANSFORMED DATA (value/business rule check) ---
    data_quality_validator.validate(transformed_df)

    # --- LOAD ---
    try:
        repository.write(transformed_df)
        logger.info(f"Successfully wrote data to Iceberg table: {repository.full_table_name}")
    except Exception as e:
        raise IcebergWriteError(f"Failed to write to Iceberg table: {e}") from e


if __name__ == "__main__":
    try:
        main()
    except BookingPipelineError as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        sys.exit(1)