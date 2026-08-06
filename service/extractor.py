from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame


class BaseExtractor(ABC):
    """
    Interface (abstract base class) for anything that can extract
    raw data into a Spark DataFrame.

    Why this exists (Dependency Inversion Principle):
    - job/booking_job.py should depend on "some extractor that has
      an extract() method" - not specifically on "BookingExtractor
      that reads JSON files".
    - If tomorrow we need to read from an API or a database instead
      of a JSON file, we just create a new class that implements
      this same interface (e.g. ApiExtractor). booking_job.py's
      code barely needs to change.
    """

    @abstractmethod
    def extract(self, source_path: str) -> DataFrame:
        raise NotImplementedError


class BookingExtractor(BaseExtractor):
    """
    Reads raw booking data from a JSON/JSONL file and converts it
    into a Spark DataFrame. This is one implementation of BaseExtractor.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract(self, source_path: str) -> DataFrame:
        """
        Reads a JSON/JSONL file and returns a Spark DataFrame.
        Each line in the file is treated as one JSON record.
        """
        df = self.spark.read.json(source_path)
        return df