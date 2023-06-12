import os
import pandas as pd
import time
import requests
import logging
from data_validation import DataValidator
from data_transformation import DataTransformation

_logger = logging.getLogger("logger_name")
logging.basicConfig(filename='C:\\Downloads\\data_pipeline.log',
                    level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')


class DataPipeline:
    """
        A data pipeline for downloading, transforming, and saving data.

        Attributes:
            None

        Methods:
            __init__: Initializes the DataPipeline object.
            download_csv_data: Downloads data from a given URL .
            convert_to_dataframe: Converts raw data into a pandas DataFrame.
            save_raw_text: Saves raw data to a file.
            save_data_to_csv: Saves a DataFrame to a CSV file.
            save_data_to_parquet: Saves a DataFrame to a Parquet file.
    """

    def __int__(self):
        pass

    def download_csv_data(self, data_url=''):
        """
        Downloads data from the given URL .

        Args:
            data_url (str): The URL of the CSV file.

        Returns:
            The content of the CSV file.
        """
        response = requests.get(data_url)
        return response.content.decode('utf-8')

    def convert_to_dataframe(self, raw_data):
        """
        Converts raw data into a pandas DataFrame.

        Args:
            raw_data (str): The raw data in CSV format.

        Returns:
            pandas.DataFrame: The data as a DataFrame.
        """
        data_frame = pd.read_csv(raw_data, sep=',', header=0, encoding='utf-8')
        return data_frame

    def save_raw_text(self, destination_folder, raw_data):
        """
        Saves raw data to a file.

        Args:
            destination_folder (str): The path to the destination file.
            raw_data (str): The raw data to be saved.

        Returns:
            bool: True if the data was successfully saved, False otherwise.
        """
        try:
            with open(destination_folder, 'w+', encoding='utf-8') as fwrite:
                fwrite.write(raw_data)
            return True
        except Exception as e:
            logging.info('Cannot write to this file ::')
            logging.info(e)
            return False

    def save_data_to_csv(self, data, csvdata):
        """
        Saves the DataFrame to a CSV file.

        Args:
            data (pandas.DataFrame): The DataFrame to be saved.
            csvdata (str): The path to the output CSV file.

        Returns:
            None
        """
        data.to_csv(csvdata, index=False)

    def save_data_to_parquet(self, data, parquetdata):
        """
        Saves the DataFrame to a Parquet file.

        Args:
            data (pandas.DataFrame): The DataFrame to be saved.
            parquetdata (str): The path to the output Parquet file.

        Returns:
            None
        """
        data.to_parquet(parquetdata, index=False)


if __name__ == '__main__':
    # Measure the performance by recording the start time
    start_time = time.time()

    # Define the URL, file names, and directories
    url = 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/EDA14/CSV/1.0/en'
    file_name = 'school_data.csv'
    des_dir = 'C:\\Downloads\\' + file_name
    csv_dir = 'C:\\Downloads\\'
    parquet_dir = 'C:\\Downloads\\'

    # Create an instance of the DataPipeline class
    dp = DataPipeline()

    # Download the CSV data from the specified URL
    data = dp.download_csv_data(url)
    if dp.save_raw_text(des_dir, data):
        logging.info('Converting the CSV file to dataframe')
        # Convert the raw data to a pandas DataFrame
        school_data = dp.convert_to_dataframe(des_dir)
    else:
        raise Exception('File Cannot be written.')
    # Perform data validation
    dv = DataValidator()
    validate_results = dv.validate(school_data)

    # Perform data transformation
    dt = DataTransformation()
    transformed_data, filter_data = dt.data_transform(school_data)
    logging.info(transformed_data)
    logging.info(filter_data.head())

    # Description of the transformed data
    """
    The transformed_data DataFrame represents the transformed version of the data.
    It contains the results of the data transformation process, which may include cleaning,
    reformatting, or aggregating the original data to meet specific requirements.
    """

    # Description of the filter_data
    """
    The filter_data DataFrame represents a subset of the transformed_data based on certain conditions or filters.
    It contains a filtered view of the transformed data, including only the rows or columns that satisfy specific
    criteria.
    """

    # Save the transformed data and filter_data to CSV files
    dp.save_data_to_csv(transformed_data, csv_dir + 'results1.csv')
    dp.save_data_to_csv(filter_data, csv_dir + 'results2.csv')

    # Save the transformed data and filter_data to Parquet files
    dp.save_data_to_parquet(transformed_data, parquet_dir + 'results1.parquet')
    dp.save_data_to_parquet(filter_data, parquet_dir + 'results2.parquet')

    # Calculate the execution time and print the result
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Total execution time: {execution_time} seconds")
