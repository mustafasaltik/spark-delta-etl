import logging
import pandas as pd

from pyspark.sql import DataFrame, SparkSession


class Extraction:
    """
    Extraction Layer Operations

    This class provides methods for loading and converting data into Spark DataFrames.
    It's expected to support data ingestion from various sources, with error handling and logging
    for monitoring the data extraction process.

    Attributes:
        spark (SparkSession): The active Spark session used for creating Spark DataFrames.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def from_csv(self, file_path: str) -> DataFrame:
        """
        Loads a CSV file into a Spark DataFrame.

        This method reads a CSV file using pandas, then converts the resulting pandas
        DataFrame into a Spark DataFrame. It logs the successful completion or failure
        of the extraction process.

        param file_path: The path to the CSV file to be loaded.
        return: A Spark DataFrame containing the loaded data.
        raises Exception: If the data ingestion process fails, an exception is logged and raised.
        """
        try:
            # Load datasets using pandas
            pd_dataframe = pd.read_csv(file_path)
            # Convert pandas DataFrames to Spark DataFrames
            spark_dataframe = self.spark.createDataFrame(pd_dataframe)
            logging.info(f"Extraction completed successfully for '{file_path}'")

            return spark_dataframe
        except Exception:
            logging.error("Data ingestion failed", exc_info=True)
            raise
