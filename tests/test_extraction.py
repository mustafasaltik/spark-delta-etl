import unittest
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from ETLOperations.extraction import Extraction


class TestExtraction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
        cls.extraction = Extraction(cls.spark)

    @patch("pandas.read_csv")
    def test_from_csv_success(self, mock_read_csv):
        # Mock pandas DataFrame and its conversion to Spark DataFrame
        mock_pdf = MagicMock()
        mock_read_csv.return_value = mock_pdf
        mock_spark_df = self.spark.createDataFrame([(1, "Test")], ["id", "name"])
        self.spark.createDataFrame = MagicMock(return_value=mock_spark_df)

        result = self.extraction.from_csv("fake_path.csv")

        # Assertions
        self.spark.createDataFrame.assert_called_once_with(mock_pdf)
        self.assertEqual(result.columns, ["id", "name"])

    @patch("pandas.read_csv", side_effect=FileNotFoundError)
    def test_from_csv_file_not_found(self, mock_read_csv):
        with self.assertRaises(FileNotFoundError):
            self.extraction.from_csv("non_existent_file.csv")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
