from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from config.settings import Settings
from config.spark_session import SparkSessionFactory


def main() -> None:
    settings = Settings()
    spark = SparkSessionFactory(settings).create_spark_session()

    full_table_name = (
        f"{settings.catalog_name}.{settings.iceberg_database}.{settings.iceberg_table_name}"
    )

    print(f"Verifying Iceberg table: {full_table_name}")

    print("=== TABLE SCHEMA ===")
    spark.sql(f"DESCRIBE {full_table_name}").show(truncate=False)

    print("=== TABLE SAMPLE (selected columns) ===")
    spark.sql(
        f"SELECT transaction_id, conversion_key, revenue, currency, status, check_in_date FROM {full_table_name} LIMIT 20"
    ).show(truncate=False)

    print("=== TABLE DATA (first 20 rows) ===")
    spark.sql(f"SELECT * FROM {full_table_name} LIMIT 20").show(truncate=False)


if __name__ == "__main__":
    main()
