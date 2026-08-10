from pyspark.sql import SparkSession

from config.settings import Settings
from infra.spark_session import SparkSessionFactory
from service.extractor import BookingExtractor
from service.transformer import BookingTransformer
from service.exchange_rate_service import ExchangeRateService
from service.validator import BookingSchemaValidator, BookingDataQualityValidator
from repository.iceberg_repository import IcebergRepository
from utils.mapping_loader import MappingLoader
from utils.logger import get_logger
from utils.exceptions import ExchangeRateFetchError

logger = get_logger(__name__)


class PipelineComponents:
    """
    Holds every object a pipeline run needs: settings, spark session,
    extractor, validators, transformer, repository, and the fetched
    USD exchange rate. Produced by PipelineComponentsBuilder so this
    setup exists in exactly ONE place and is shared by both the
    normal run and the batch migration run.
    """

    def __init__(
        self,
        settings: Settings,
        spark: SparkSession,
        extractor: BookingExtractor,
        schema_validator: BookingSchemaValidator,
        data_quality_validator: BookingDataQualityValidator,
        transformer: BookingTransformer,
        repository: IcebergRepository,
        usd_rate: float,
    ):
        self.settings = settings
        self.spark = spark
        self.extractor = extractor
        self.schema_validator = schema_validator
        self.data_quality_validator = data_quality_validator
        self.transformer = transformer
        self.repository = repository
        self.usd_rate = usd_rate


class PipelineComponentsBuilder:
    """
    Builds a PipelineComponents instance. Every job (normal run, batch
    migration, or any future job) should use this class instead of
    repeating the Spark session / extractor / transformer / repository
    setup itself.
    """

    def __init__(self, settings: Settings = None, rate_service=None):
        self.settings = settings or Settings()
        self.rate_service = rate_service or ExchangeRateService()

    def _fetch_usd_rate(self) -> float:
        try:
            usd_rate = self.rate_service.get_rate("EUR", "USD")
            logger.info(f"Fetched exchange rate (EUR -> USD): {usd_rate}")
            return usd_rate
        except Exception as e:
            raise ExchangeRateFetchError(f"Failed to fetch exchange rate: {e}") from e

    def build(self) -> PipelineComponents:
        usd_rate = self._fetch_usd_rate()

        spark = SparkSessionFactory(self.settings).create_spark_session()
        logger.info("Spark session created.")

        mapping_loader = MappingLoader()

        return PipelineComponents(
            settings=self.settings,
            spark=spark,
            extractor=BookingExtractor(spark),
            schema_validator=BookingSchemaValidator(),
            data_quality_validator=BookingDataQualityValidator(),
            transformer=BookingTransformer(spark, mapping_loader),
            repository=IcebergRepository(spark, self.settings),
            usd_rate=usd_rate,
        )