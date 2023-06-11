import unittest
from unittest import mock
import pandas as pd

import data_validation
from data_validation import RecordCountValidator
from data_transformation import DataTransformation
from unittest.mock import MagicMock, patch
from data_pipeline import DataPipeline




class TestDataPipeline(unittest.TestCase):
    def setUp(self):
        self.url = 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/EDA14/CSV/1.0/en'

    def test_download_csv_data(self):
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.content.decode.return_value = 'test data'
            mock_get.return_value = mock_response

            data_pipeline = DataPipeline()
            data = data_pipeline.download_csv_data(self.url)

            mock_get.assert_called_once_with(self.url)
            self.assertEqual(data, 'test data')

    def test_filter_and_transform_data(self):
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

        expected_df = pd.DataFrame({
            'year_group': [2010, 2015],
            'female': [0, 0],
            'male': [0, 0],
            'both_sexes': [250, 450]
        })
        expected_df = expected_df.set_index('year_group')
        data_pipeline = DataTransformation()
        df, filter_df = data_pipeline.data_transform(data)
        self.assertEqual(len(df), len(expected_df))

    def test_save_data_to_csv(self):
        df = pd.DataFrame({
            'year_group': [2010, 2015],
            'female': [150, 250],
            'male': [100, 200],
            'both_sexes': [250, 450]
        })
        filename = 'output.csv'

        with patch('pandas.DataFrame.to_csv') as mock_to_csv:
            data_pipeline = DataPipeline()
            data_pipeline.save_data_to_csv(df, filename)

            mock_to_csv.assert_called_once_with(filename, index=False)

    def test_save_data_to_parquet(self):
        df = pd.DataFrame({
            'year_group': [2010, 2015],
            'female': [150, 250],
            'male': [100, 200],
            'both_sexes': [250, 450]
        })
        filename = 'output.parquet'

        with patch('pandas.DataFrame.to_parquet') as mock_to_parquet:
            data_pipeline = DataPipeline()
            data_pipeline.save_data_to_parquet(df, filename)

            mock_to_parquet.assert_called_once_with(filename, index=False)

    def test_filter_and_transform_data_called_with_downloaded_data(self):
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

        expected_df = pd.DataFrame({
            'year_group': [2010, 2015],
            'female': [0, 0],
            'male': [0, 0],
            'both_sexes': [250, 450]
        })

        expected_df = expected_df.set_index('year_group')

        data_pipeline = DataTransformation()
        df, filter_df = data_pipeline.data_transform(mock_download_csv_data)
        self.assertEqual(len(df), len(expected_df))

    def test_download_csv_data_exception(self):
        with patch('requests.get') as mock_get:
            mock_get.side_effect = Exception('Download error')

            data_pipeline = DataPipeline()

            with self.assertRaises(Exception):
                data_pipeline.download_csv_data()

    def test_record_valdiation(self):
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

        record_validator = RecordCountValidator()
        flag_check = record_validator.record_validate(mock_download_csv_data)
        self.assertTrue(flag_check)


