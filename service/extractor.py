from pyspark.sql import SparkSession, DataFrame




class BookingExtractor:
    """
    Reads raw booking data from a JSON/JSONL file and converts it
    into a Spark DataFrame.
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