import logging
import pandas as pd


class Extraction:
    """
    Extraction Layer Operations
    """

    def __init__(self, spark):
        self.spark = spark

    def from_csv(self, file_path):
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
