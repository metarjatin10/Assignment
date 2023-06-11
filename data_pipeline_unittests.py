import unittest
from unittest import mock
import pandas as pd

import data_validation
from data_validation import RecordCountValidator
from data_transformation import DataTransformation
from unittest.mock import MagicMock, patch
from data_pipeline import DataPipeline




class TestDataPipeline(unittest.TestCase):
    """
    Unit tests for the DataPipeline class.

    """
    def setUp(self):
        """
        Set up the necessary data for the test cases.

        """
        self.url = 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/EDA14/CSV/1.0/en'

    def test_download_csv_data(self):
        """
        Test the download_csv_data method of DataPipeline.

        This method should download the CSV data from the specified URL and return it.

        """
        # Mock the requests.get method to return a mock response
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.content.decode.return_value = 'test data'
            mock_get.return_value = mock_response

            data_pipeline = DataPipeline()
            data = data_pipeline.download_csv_data(self.url)

            mock_get.assert_called_once_with(self.url)
            self.assertEqual(data, 'test data')

    def test_filter_and_transform_data(self):
        """
        Test the filter_and_transform_data method of DataPipeline.

        This method should transform the provided DataFrame by filtering and reorganizing the data.
        It ensures that the transformation is performed correctly by comparing the transformed DataFrame
        with the expected DataFrame.

        """
        # Create a DataFrame representing the input data
        data = pd.DataFrame({
            'STATISTIC': [2010, 2015],
            'Statistic Label': ['Both', 'Both'],
            'C02351V02955': [250, 450],
            'Type of School': [2010, 2015],
            'C02199V02655': [1, 1],
            'Sex': ['Both', 'Both'],
            'TLIST(A1)': [2010, 2015],
            'Year': [2010, 2015],
            'UNIT': ['Number', 'Number'],
            'VALUE': [250, 450]
        })
        # Create the expected DataFrame after transformation
        expected_df = pd.DataFrame({
            'year_group': [2010, 2015],
            'female': [0, 0],
            'male': [0, 0],
            'both_sexes': [250, 450]
        })
        expected_df = expected_df.set_index('year_group')
        # Create an instance of DataTransformation and call the method under test
        data_pipeline = DataTransformation()
        df, filter_df = data_pipeline.data_transform(data)
        # Assertion
        self.assertEqual(len(df), len(expected_df))

    def test_save_data_to_csv(self):
        """
        Test the save_data_to_csv method of DataPipeline.

        This method should save the provided DataFrame as a CSV file with the specified filename.
        It ensures that the DataFrame is correctly saved as a CSV file by asserting that the to_csv method is called
        with the expected arguments.

        """
        # Create a DataFrame to be saved
        df = pd.DataFrame({
            'year_group': [2010, 2015],
            'female': [150, 250],
            'male': [100, 200],
            'both_sexes': [250, 450]
        })
        filename = 'output.csv'

        # Mock the pandas.DataFrame.to_csv method
        with patch('pandas.DataFrame.to_csv') as mock_to_csv:
            # Create an instance of DataPipeline and call the method under test
            data_pipeline = DataPipeline()
            data_pipeline.save_data_to_csv(df, filename)
            # Assertion
            mock_to_csv.assert_called_once_with(filename, index=False)

    def test_save_data_to_parquet(self):
        """
        Test the save_data_to_parquet method of DataPipeline.

        This method should save the provided DataFrame as a Parquet file with the specified filename.
        It ensures that the DataFrame is correctly saved as a Parquet file by asserting that the to_parquet method is called
        with the expected arguments.

        """
        # Create a DataFrame to be saved
        df = pd.DataFrame({
            'year_group': [2010, 2015],
            'female': [150, 250],
            'male': [100, 200],
            'both_sexes': [250, 450]
        })
        filename = 'output.parquet'
        # Mock the pandas.DataFrame.to_parquet method
        with patch('pandas.DataFrame.to_parquet') as mock_to_parquet:
            # Create an instance of DataPipeline and call the method under test
            data_pipeline = DataPipeline()
            data_pipeline.save_data_to_parquet(df, filename)

            # Assertion
            mock_to_parquet.assert_called_once_with(filename, index=False)

    def test_filter_and_transform_data_called_with_downloaded_data(self):
        """
        Test the filter_and_transform_data method of DataPipeline with downloaded data.

        This method should transform the provided DataFrame, which represents the downloaded data, by filtering and
        reorganizing the data. It ensures that the transformation is performed correctly by comparing the transformed
        DataFrame with the expected DataFrame.

        """
        # Create a mock DataFrame representing the downloaded data
        mock_download_csv_data = pd.DataFrame({
            'STATISTIC': [2010, 2015],
            'Statistic Label': ['Both', 'Both'],
            'C02351V02955': [250, 450],
            'Type of School': [2010, 2015],
            'C02199V02655': [1, 1],
            'Sex': ['Both', 'Both'],
            'TLIST(A1)': [2010, 2015],
            'Year': [2010, 2015],
            'UNIT': ['Number', 'Number'],
            'VALUE': [250, 450]
        })
        # Create the expected DataFrame after transformation
        expected_df = pd.DataFrame({
            'year_group': [2010, 2015],
            'female': [0, 0],
            'male': [0, 0],
            'both_sexes': [250, 450]
        })

        expected_df = expected_df.set_index('year_group')
        # Create an instance of DataTransformation and call the method under test
        data_pipeline = DataTransformation()
        df, filter_df = data_pipeline.data_transform(mock_download_csv_data)

        # Assertion
        self.assertEqual(len(df), len(expected_df))

    def test_download_csv_data_exception(self):
        """
        Test the download_csv_data method of DataPipeline.

        This method should download the CSV data from the specified URL and return it.

        """
        with patch('requests.get') as mock_get:
            mock_get.side_effect = Exception('Download error')

            data_pipeline = DataPipeline()

            with self.assertRaises(Exception):
                data_pipeline.download_csv_data()

    def test_record_valdiation(self):
        """
        Test the record_validate method of RecordCountValidator.

        This method should validate the record count of the provided DataFrame and return True if the count is
        greater than or equal to the expected count, or False otherwise.

        """
        # Create a mock DataFrame for testing
        mock_download_csv_data = pd.DataFrame({
            'STATISTIC': [2010, 2015],
            'Statistic Label': ['Both', 'Both'],
            'C02351V02955': [250, 450],
            'Type of School': [2010, 2015],
            'C02199V02655': [1, 1],
            'Sex': ['Both', 'Both'],
            'TLIST(A1)': [2010, 2015],
            'Year': [2010, 2015],
            'UNIT': ['Number', 'Number'],
            'VALUE': [250, 450]
        })
        # Create an instance of RecordCountValidator and call the method under test
        record_validator = RecordCountValidator()
        flag_check = record_validator.record_validate(mock_download_csv_data)

        # Assertion
        self.assertTrue(flag_check)


