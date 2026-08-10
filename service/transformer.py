from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from utils.mapping_loader import MappingLoader


class BookingTransformer:
    """
    This class takes the raw booking DataFrame (from extractor)
    and cleans/transforms it into the final structure we want
    to save into the Iceberg table.
    """

    def __init__(self, spark: SparkSession, mapping_loader: MappingLoader):
        self.spark = spark
        self.mapping_loader = mapping_loader

    def transform(self, df: DataFrame, usd_rate: float) -> DataFrame:
        """
        Runs all transformation steps in order and returns the
        final, clean DataFrame ready to be written to Iceberg.

        usd_rate: exchange rate fetched once before calling this
        method (outside the Spark DAG).
        """
        df = self.apply_status_mapping(df)
        df = self.apply_region_mapping(df)
        df = self.parse_label_fields(df)
        df = self.select_final_columns(df)
        df = self.add_revenue_usd(df, usd_rate)
        return df


    def _build_status_lookup_df(self) -> DataFrame:
        status_mapping = self.mapping_loader.load_status_mapping()
        rows = [(k, v) for k, v in status_mapping.items()]
        return self.spark.createDataFrame(rows, ["raw_status", "mapped_status"])

    def _build_region_lookup_df(self) -> DataFrame:
        region_mapping = self.mapping_loader.load_region_mapping()
        rows = [(k, v) for k, v in region_mapping.items()]
        return self.spark.createDataFrame(rows, ["country_code", "region"])

    def apply_status_mapping(self, df: DataFrame) -> DataFrame:
        status_lookup = self._build_status_lookup_df()

        main = df.alias("main")
        lookup = status_lookup.alias("lookup")

        joined = main.join(

            
            F.broadcast(lookup),
            F.col("main.status") == F.col("lookup.raw_status"),
            "left",
        )

        joined = joined.withColumn(
            "mapped_status",
            F.coalesce(F.col("lookup.mapped_status"), F.col("main.status")),
        )

        joined = joined.drop("raw_status")

        return joined

    def apply_region_mapping(self, df: DataFrame) -> DataFrame:
        region_lookup = self._build_region_lookup_df()

        df = df.withColumn(
            "country_code_upper", F.upper(F.col("booker.address.country"))
        )

        main = df.alias("main")
        lookup = region_lookup.alias("lookup")

        joined = main.join(
            F.broadcast(lookup),
            F.col("main.country_code_upper") == F.col("lookup.country_code"),
            "left",
        )

        return joined

    def parse_label_fields(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(
            "site_key", F.upper(F.regexp_extract(F.col("label"), r"k-([a-zA-Z]+)", 1))
        )

        df = df.withColumn(
            "device_code", F.regexp_extract(F.col("label"), r"d-([a-zA-Z]+)", 1)
        )

        df = df.withColumn(
            "device",
            F.when(F.col("device_code") == "m", "mobile")
            .when(F.col("device_code") == "d", "desktop")
            .when(F.col("device_code") == "t", "tablet")
            .otherwise(F.col("device_code")),
        )

        df = df.withColumn(
            "referral_property_id",
            F.regexp_extract(F.col("label"), r"p-(BC-\d+)", 1),
        )

        return df

    def select_final_columns(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("accommodations.reservation").alias("transaction_id"),
            F.col("label").alias("conversion_key"),
            F.concat(F.lit("BC-"), F.col("accommodation_details.accommodation").cast("string"))
            .alias("property_id"),
            F.col("mapped_status").alias("status"),
            F.col("booker.travel_purpose").alias("travel_purpose"),
            F.col("country_code_upper").alias("country_code"),
            F.col("region"),
            F.col("currencies.booker").alias("currency"),
            F.to_date(F.col("start")).alias("check_in_date"),
            F.to_date(F.col("end")).alias("check_out_date"),
            F.col("site_key"),
            F.col("device"),
            F.col("referral_property_id"),
            F.col("commission.estimate_commission_amount.booker_currency").alias("revenue"),
        )

    def add_revenue_usd(self, df: DataFrame, usd_rate: float) -> DataFrame:
        """
        Adds a revenue_usd column by multiplying revenue by usd_rate.

        usd_rate is a plain Python float, fetched ONCE outside the
        Spark DAG (see ExchangeRateService). Here we just pass it in
        as a constant using F.lit() - Spark applies it to every row
        without needing any network call during the transformation.
        """
        return df.withColumn("revenue_usd", F.round(F.col("revenue") * F.lit(usd_rate), 2))
