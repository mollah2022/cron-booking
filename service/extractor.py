from pyspark.sql import SparkSession, DataFrame


class BookingExtractor:
    """
    This class is responsible for reading raw booking data
    from a JSON file and converting it into a Spark DataFrame.

    Why a separate class for this?
    - This class only knows how to "read" data.
    - If tomorrow the data source changes (e.g. from JSON file
      to an API or database), only this file needs to change.
      No other file (transformer, loader) needs to know about it.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract(self, json_path: str) -> DataFrame:
        """
        Reads a JSON file and returns a Spark DataFrame.

        multiLine=True is used because our booking JSON is
        a single, nicely formatted (multi-line) JSON object,
        not one JSON record per line.
        """
        df = self.spark.read.option("multiLine", "false").json(json_path)
        return df