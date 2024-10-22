import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import col, isnan, to_timestamp, trim, when, upper


class Transformation:
    """
    Transformation Layer Operations

    This class provides methods for transforming data within Spark DataFrames.
    It includes functionalities for handling null values, cleaning data, and
    converting data types, with logging for tracking transformations.

    Attributes:
        spark (SparkSession): The active Spark session used for data processing.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def to_null(self, column_name: str) -> Column:
        """
        Replaces specific values in a column with None (null).

        This method converts certain values in the specified column to null if they
        represent missing or invalid data, such as empty strings, NaN values, or
        common placeholders like "NULL" or "N/A".

        param column_name: The name of the column to transform.
        return: A column expression where the specified values are replaced with None.
        """
        return when(
            col(column_name).isNotNull() &
            ~isnan(col(column_name)) &
            (trim(col(column_name)) != "") &
            (trim(col(column_name)) != "NaN") &
            (upper(trim(col(column_name))) != "NULL") &
            (upper(trim(col(column_name))) != "N/A"),
            col(column_name)
        ).otherwise(None)

    def drop_null_values(self, df: DataFrame) -> DataFrame:
        """
        Cleans the DataFrame by replacing specific values with null and dropping rows with null values.

        This method applies the `to_null` transformation to all columns in the DataFrame,
        then drops any rows containing null values. The number of rows removed is logged.

        param df: The DataFrame to clean.
        return: A cleaned DataFrame with null values dropped.
        """
        before = df.count()
        cleaned_df = df.select(list(map(lambda c: self.to_null(c).alias(c), df.columns))).na.drop()
        after = cleaned_df.count()
        logging.info(f"{before - after} row(s) removed. Row count after cleaning: {after}")
        return cleaned_df

    def convert_to_timestamp(self, df: DataFrame, column_name: str, date_format: str) -> DataFrame:
        """
        Converts a specified column to a timestamp using the given format.

        This method applies a timestamp conversion to the specified column based on the provided
        date format string.

        param df: The DataFrame containing the column to convert.
        param column_name: The name of the column to be converted to a timestamp.
        param date_format: The format string to use for the timestamp conversion (e.g., "yyyy-MM-dd").
        return: A DataFrame with the specified column converted to a timestamp.
        """
        return df.withColumn(column_name, to_timestamp(df[column_name], date_format))