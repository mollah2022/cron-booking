from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from config.settings import Settings
from config.spark_session import SparkSessionFactory
from service.extractor import BookingExtractor
from service.transformer import BookingTransformer
from repository.iceberg_repository import IcebergRepository
from utils.mapping_loader import MappingLoader


def main() -> None:
    settings = Settings()
    spark = SparkSessionFactory(settings).create_spark_session()

    extractor = BookingExtractor(spark)
    mapping_loader = MappingLoader()
    transformer = BookingTransformer(spark, mapping_loader)
    repository = IcebergRepository(spark, settings)

    raw_df = extractor.extract(settings.raw_json_path)
    transformed_df = transformer.transform(raw_df)

    repository.write(transformed_df)

    print(f"Successfully wrote data to Iceberg table: {repository.full_table_name}")


if __name__ == "__main__":
    main()
