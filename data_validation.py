import logging


class RecordCountValidator:
    """
    A class for validating the record count of a DataFrame.

    Attributes:
        _actual_record_count (int): The actual record count based on the DataFrame.
        _expected_record_count (int): The expected record count based on the DataFrame.

    Methods:
        record_validate: Validates the record count of a DataFrame.

    """
    def __init__(self):
        self._actual_record_count = None
        self._expected_record_count = None

    def record_validate(self, data):
        """
        Validates the record count of a DataFrame.

        Args:
            data (pandas.DataFrame): The DataFrame to validate.

        Returns:
            bool: True if the record count validation passes, False otherwise.
        """
        # Get the expected record count based on the DataFrame
        self._expected_record_count = len(data) * 0.6

        # Get the actual record count based on the CSV data
        self._actual_record_count = len(data)

        # Compare the expected and actual record counts
        if self._actual_record_count >= self._expected_record_count:
            logging.info("Record count validation passed.")
            return True
        else:
            logging.info("Record count validation failed.")
            return False


class DataValidator:
    """
    A class for performing data validation on a DataFrame.

    Attributes:
        _data (pandas.DataFrame): The DataFrame to validate.
        _data_validator (RecordCountValidator): An instance of RecordCountValidator for record count validation.
        _record_validator (RecordCountValidator): An instance of RecordCountValidator for record count validation.
        _sanity_validator (DataSanityValidator): An instance of DataSanityValidator for sanity validation.
        _validate_results (dict): A dictionary to store the validation results.

    Methods:
        validate: Performs data validation on the specified DataFrame.

    """
    def __init__(self):
        self._data = None
        self._data_validator = None
        self._record_validator = RecordCountValidator()
        self._sanity_validator = DataSanityValidator()
        self._validate_results = {}

    def validate(self, data):
        """
        Performs data validation on the specified DataFrame.

        Args:
            data (pandas.DataFrame): The DataFrame to validate.

        Returns:
            dict: A dictionary containing the validation results.

        """
        logging.info('************************************************************************************************')
        self._data = data
        if self._record_validator.record_validate(self._data):
            self._validate_results['Record_Validation'] = 'True'
        else:
            self._validate_results['Record_Validation'] = 'False'
        if self._sanity_validator.perform_sanity_checks(self._data):
            self._validate_results['Sanity_Validation'] = 'True'
        else:
            self._validate_results['Sanity_Validation'] = 'False'
        logging.info('************************************************************************************************')
        return self._validate_results


class DataSanityValidator:
    """
    A class for performing data sanity checks on a DataFrame.

    Attributes:
        _data (pandas.DataFrame): The DataFrame to perform sanity checks on.

    Methods:
        check_data_types: Checks the data types of the DataFrame columns.
        check_missing_values: Checks for missing values in the DataFrame columns.
        perform_sanity_checks: Performs sanity checks on the specified DataFrame.

    """
    def __init__(self):
        self._data = None

    def check_data_types(self, type_data):
        """
        Checks the data types of the DataFrame columns.

        Args:
            type_data (pandas.DataFrame): The DataFrame to check.

        Returns:
            dict: A dictionary containing the data type check results for each column.

        """
        self._data = type_data
        self._data_type_check_result = {}
        column_types = {'STATISTIC': object, 'Statistic Label': object, 'C02351V02955': int,
                        'Type of School': object, 'C02199V02655': object, 'Sex': object, 'TLIST(A1)': int,
                        'Year': int, 'UNIT': object, 'VALUE': float}
        # Check data types of columns
        for column in self._data.columns:
            logging.info(f"Column '{column}': {self._data[column].dtype}")
            if self._data[column].dtype == column_types[column]:
                self._data_type_check_result[column]: True
            else:
                self._data_type_check_result[column]: False

        return self._data_type_check_result

    def check_missing_values(self, missing_data):
        """
        Checks for missing values in the DataFrame columns.

        Args:
            missing_data (pandas.DataFrame): The DataFrame to check.

        Returns:
            pandas.Series: A Series containing the count of missing values for each column.

        """
        # Check for missing values in columns
        self._data = missing_data
        missing_values = self._data.isnull().sum()
        logging.info("Missing Values:")
        logging.info(missing_values)
        return missing_values

    def perform_sanity_checks(self, data):
        """
        Performs sanity checks on the specified DataFrame.

        Args:
            data (pandas.DataFrame): The DataFrame to perform sanity checks on.

        Returns:
            bool: True if all sanity checks pass, False otherwise.

        """
        self._data = data
        flag_check = False
        sanity_results = self.check_data_types(self._data)
        missing_value_results = self.check_missing_values(self._data)
        logging.info(sanity_results)
        if False in sanity_results:
            flag_check = True
            logging.info('Sanity Validation failed ::'+ str(sanity_results))
        else:
            logging.info('Sanity Validation passed ::'+ str(sanity_results))
        return flag_check


