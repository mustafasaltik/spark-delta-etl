import logging
import yaml
import pandas as pd
from ETLOperations.extract import Extract
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, date_format, isnan, to_timestamp, trim, \
    when, upper, sum, avg, weekofyear, row_number


def error_handler(func):
    def wrapper(*args, **kwargs):
        try:
            arg_str = ", ".join([repr(arg) for arg in args])
            kwarg_str = ", ".join([f"{key}={repr(value)}" for key, value in kwargs.items()])
            logging.info(f"Calling {func.__name__} with args: {arg_str}, kwargs: {kwarg_str}")
            return func(*args, **kwargs)
        except FileNotFoundError as err:
            logging.error(f"{err}, {func.__name__}, ETL1001")
            raise
        except PermissionError as err:
            logging.error(f"{err}, {func.__name__}, ETL1002")
            raise
        except Exception as err:
            logging.error(f"{err}, {func.__name__}, ETL1003")
            raise

    return wrapper


@error_handler
def load_config(config_file):
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


@error_handler
def to_null(column_name: str):
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


@error_handler
def drop_null_values(df):
    before = df.count()
    cleaned_df = df.select(list(map(lambda c: to_null(c).alias(c), df.columns))).na.drop()
    after = cleaned_df.count()
    logging.info(f"{before - after} row(s) removed. Row count after cleaning: {after}")
    return cleaned_df


def convert_to_timestamp(df, column_name, format):
    """
    Converts the specified column in the DataFrame to a timestamp format.

    :param df: The DataFrame to modify
    :param column_name: The name of the column to convert
    :param format: The timestamp format to use (e.g., "yyyy-MM-dd")
    :return: A new DataFrame with the specified column converted to a timestamp
    """
    return df.withColumn(column_name, to_timestamp(df[column_name], format))


# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@error_handler
def main():
    config = load_config("config.yaml")

    builder = (
        SparkSession.builder.appName("DataPipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Extraction Layer
    accounts_df = Extract(spark).from_csv(config["input"]["accounts"])
    customers_df = Extract(spark).from_csv(config["input"]["customers"])
    transactions_df = Extract(spark).from_csv(config["input"]["transactions"])

    # Data Cleaning
    logging.info(f"Accounts data is going to be cleaned..")
    accounts_df_cleaned = drop_null_values(accounts_df)

    logging.info(f"Transactions data is going to be cleaned..")
    transactions_df_cleaned = drop_null_values(transactions_df)

    logging.info(f"Customers data is going to be cleaned..")
    customers_df_cleaned = drop_null_values(customers_df)

    # Data Transformation
    transactions_df_cleaned = convert_to_timestamp(transactions_df_cleaned, "transaction_date", "yyyy-MM-dd")\
        .withColumn("year_month", date_format(col("transaction_date"), "yyyy-MM"))
    customers_df_cleaned = convert_to_timestamp(customers_df_cleaned, "join_date", "yyyy-MM-dd")

    transactions_accounts_df = transactions_df_cleaned.join(
        accounts_df_cleaned, on="account_id", how="inner"
    )
    final_trx_df = transactions_accounts_df.join(
        customers_df, on="customer_id", how="inner"
    )

    # Data Analysis and export process
    final_trx_df.groupBy("account_id", "year_month", "transaction_type")\
        .agg(sum("amount").alias("total_amount"))\
        .write.format("delta").mode("overwrite")\
        .save(config["output"]["aggregated_data"] + "/total_amounts_per_acc_yearmonth_trx_type")

    final_trx_df.groupBy("account_type", "year_month")\
        .agg(sum("balance").alias("total_balance"))\
        .write.format("delta").mode("overwrite")\
        .save(config["output"]["aggregated_data"] + "/total_balance_per_acc_type")

    final_trx_df.groupBy("customer_id")\
        .agg(avg("amount").alias("avg_transaction_amount"))\
        .write.format("delta").mode("overwrite")\
        .save(config["output"]["aggregated_data"] + "/avg_transaction_amt_per_customer")

    # Data enrichment and export process
    trx_classification = final_trx_df.withColumn("transaction_classification",
                                                 when(col("amount") > 1000, "high")
                                                 .when((col("amount") >= 500)
                                                       & (col("amount") <= 1000),
                                                       "medium",)
                                                 .otherwise("low"))

    trx_classification.write.format("delta").mode("overwrite").save(config["output"]["enriched_transactions"]
                                                                    + "/trx_classification")

    trx_numbers_per_classification_per_week = \
        trx_classification.withColumn("week_of_year", weekofyear(col("transaction_date")))\
        .groupBy("transaction_classification", "week_of_year").count()

    trx_numbers_per_classification_per_week.write.format("delta").mode("overwrite")\
        .save(config["output"]["enriched_transactions"] + "/trx_numbers_per_classification_week")

    # Transactions with the largest amount per classification
    window_spec = Window.partitionBy("transaction_classification").orderBy(
        col("amount").desc()
    )
    trx_classification.withColumn("rn", row_number().over(window_spec))\
        .filter(col("rn") == 1).drop("rn").show(10)


if __name__ == '__main__':
    main()
