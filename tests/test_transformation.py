# test_transformation.py

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from ETLOperations.transformation import Transformation

class TestTransformation(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
        cls.transformation = Transformation(cls.spark)

        # Example DataFrame for testing
        cls.sample_data = [
            (1, "2023-01-01", 500),
            (2, "2023-01-02", None),
            (3, "2023-01-03", 100),
            (4, None, 300),
            (5, "N/A", 400)
        ]
        cls.schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("amount", IntegerType(), True)
        ])
        cls.sample_df = cls.spark.createDataFrame(cls.sample_data, schema=cls.schema)

    def test_drop_null_values(self):
        # Test dropping null values
        result_df = self.transformation.drop_null_values(self.sample_df)
        expected_count = 2  # Rows with valid transaction_date and amount are 3
        self.assertEqual(result_df.count(), expected_count)
        self.assertFalse(result_df.filter(result_df.transaction_date.isNull()).count() > 0)
        self.assertFalse(result_df.filter(result_df.amount.isNull()).count() > 0)

    def test_convert_to_timestamp(self):
        # Test converting to timestamp
        result_df = self.transformation.convert_to_timestamp(self.sample_df, "transaction_date", "yyyy-MM-dd")
        self.assertTrue("transaction_date" in result_df.columns)
        self.assertEqual(result_df.schema["transaction_date"].dataType, TimestampType())

        # Check if the conversion was successful for valid dates
        converted_dates = result_df.filter(result_df.transaction_date.isNotNull()).select("transaction_date").collect()
        self.assertEqual(len(converted_dates), 3)  # Three rows should have valid dates

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()
