import logging
from pyspark.sql.functions import col, date_format, isnan, to_timestamp, trim, when, upper


class Transformation:
    """
    Transformation Layer Operations
    """

    def __init__(self, spark):
        self.spark = spark

    def to_null(self, column_name: str):
        """
        Replaces values in the column with None if they are NULL, NaN, empty strings,
        whitespace-only strings, or special text values like "NaN", "NULL", or "N/A".

        :param column_name: column name
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

    def drop_null_values(self, df):
        before = df.count()
        cleaned_df = df.select(list(map(lambda c: self.to_null(c).alias(c), df.columns))).na.drop()
        after = cleaned_df.count()
        logging.info(f"{before - after} row(s) removed. Row count after cleaning: {after}")
        return cleaned_df

    def convert_to_timestamp(self, df, column_name, date_format):
        return df.withColumn(column_name, to_timestamp(df[column_name], date_format))