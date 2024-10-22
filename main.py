import logging
from typing import Dict, Any

import yaml
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, date_format, when, sum, avg, weekofyear, row_number

from ETLOperations.extraction import Extraction
from ETLOperations.transformation import Transformation
from ETLOperations.utils import ErrorHandler


@ErrorHandler.handle_errors
def load_config(config_file: str) -> Dict[str, Any]:
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@ErrorHandler.handle_errors
def main():
    """
    Main function for executing the ETL data pipeline.

    This function performs the following steps:
    1. Loads the configuration from a YAML file.
    2. Sets up a Spark session with Delta Lake integration.
    3. Executes the extraction layer to read data from CSV files.
    4. Applies transformations for data cleaning and enrichment.
    5. Performs data aggregation and analysis.
    6. Saves the processed data to Delta tables for further use.
    7. Logs each step for tracking the ETL process.
    8. Stops the Spark session after the pipeline is completed.

    return: None
    """
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
    extraction = Extraction(spark)
    accounts_df = extraction.from_csv(config["input"]["accounts"])
    customers_df = extraction.from_csv(config["input"]["customers"])
    transactions_df = extraction.from_csv(config["input"]["transactions"])

    # Transformation Layer
    # Data Cleaning
    transformation = Transformation(spark)
    logging.info(f"Accounts data is going to be cleaned..")
    accounts_df_cleaned = transformation.drop_null_values(accounts_df)

    logging.info(f"Transactions data is going to be cleaned..")
    transactions_df_cleaned = transformation.drop_null_values(transactions_df)

    logging.info(f"Customers data is going to be cleaned..")
    customers_df_cleaned = transformation.drop_null_values(customers_df)

    # Date conversion
    transactions_df_cleaned = transformation\
        .convert_to_timestamp(transactions_df_cleaned, "transaction_date", "yyyy-MM-dd")\
        .withColumn("year_month", date_format(col("transaction_date"), "yyyy-MM"))
    customers_df_cleaned = transformation.convert_to_timestamp(customers_df_cleaned, "join_date", "yyyy-MM-dd")

    # Merge data
    merged_data = transactions_df_cleaned\
        .join(accounts_df_cleaned, on="account_id", how="inner")\
        .join(customers_df_cleaned, on="customer_id", how="inner")

    # Data Analysis and export process
    merged_data.groupBy("account_id", "year_month", "transaction_type")\
        .agg(sum("amount").alias("total_amount"))\
        .write.format("delta").mode("overwrite")\
        .save(config["output"]["aggregated_data"] + "/total_amounts_per_acc_yearmonth_trx_type")

    merged_data.groupBy("account_type", "year_month")\
        .agg(sum("balance").alias("total_balance"))\
        .write.format("delta").mode("overwrite")\
        .save(config["output"]["aggregated_data"] + "/total_balance_per_acc_type")

    merged_data.groupBy("customer_id")\
        .agg(avg("amount").alias("avg_transaction_amount"))\
        .write.format("delta").mode("overwrite")\
        .save(config["output"]["aggregated_data"] + "/avg_transaction_amt_per_customer")

    # Data enrichment and export process
    trx_classification = merged_data.withColumn("transaction_classification",
                                                 when(col("amount") > 1000, "high")
                                                 .when((col("amount") >= 500)
                                                       & (col("amount") <= 1000),
                                                       "medium",)
                                                 .otherwise("low"))

    trx_classification.write.format("delta").mode("overwrite")\
        .save(config["output"]["enriched_transactions"] + "/trx_classification")

    trx_numbers_per_classification_per_week = trx_classification\
        .withColumn("week_of_year", weekofyear(col("transaction_date")))\
        .groupBy("transaction_classification", "week_of_year").count()

    trx_numbers_per_classification_per_week.write.format("delta").mode("overwrite")\
        .save(config["output"]["enriched_transactions"] + "/trx_numbers_per_classification_week")

    # Transactions with the largest amount per classification
    window_spec = Window.partitionBy("transaction_classification").orderBy(
        col("amount").desc()
    )
    trx_classification.withColumn("rn", row_number().over(window_spec))\
        .filter(col("rn") == 1).drop("rn").show()

    spark.stop()


if __name__ == '__main__':
    main()
