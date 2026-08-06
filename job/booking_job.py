from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from config.settings import Settings
from config.spark_session import SparkSessionFactory
from service.extractor import BookingExtractor
from service.transformer import BookingTransformer
from service.exchange_rate_service import ExchangeRateService
from repository.iceberg_repository import IcebergRepository
from utils.mapping_loader import MappingLoader


def main() -> None:
    settings = Settings()

    # --- OUTSIDE THE SPARK DAG ---
    # This is a plain Python API call, made once, before Spark starts
    # any transformation. The result is a single float number.
    rate_service = ExchangeRateService()
    usd_rate = rate_service.get_rate("EUR", "USD")
    print(f"Fetched exchange rate (EUR -> USD): {usd_rate}")


    #--- SPARK SETUP---
    spark = SparkSessionFactory(settings).create_spark_session()

    extractor = BookingExtractor(spark)
    mapping_loader = MappingLoader()
    transformer = BookingTransformer(spark, mapping_loader)
    repository = IcebergRepository(spark, settings)

    # --- INSIDE THE SPARK DAG ---
    # usd_rate is passed in as a constant value here. Spark does not
    # call the API again - it just multiplies every row by this
    # single number, which is very fast.

    raw_df = extractor.extract(settings.raw_json_path)
    transformed_df = transformer.transform(raw_df, usd_rate)

    repository.write(transformed_df)

    print(f"Successfully wrote data to Iceberg table: {repository.full_table_name}")


if __name__ == "__main__":
    main()
