import logging

class DataTransformation:
    _filter_data: object

    def __init__(self):
        """
        Initializes a new instance of the DataTransformation class.

        This class provides methods to perform data transformation on a DataFrame.

        Attributes:
            _agg_group_year_data (pandas.DataFrame or None): The transformed DataFrame with grouped and aggregated data.
            _data (pandas.DataFrame or None): The input DataFrame for transformation.
            data_lowercase (pandas.DataFrame or None): The DataFrame with lowercase column names.
        """
        self._agg_group_year_data = None
        self._data = None
        self.data_lowercase = None

    def all_column_lower_case(self, lowercdata):
        """
        Converts all column names in the DataFrame to lowercase.

        Args:
            lowercdata (pandas.DataFrame): The DataFrame to convert.

        Returns:
            pandas.DataFrame: The DataFrame with lowercase column names.

        """
        self._data = lowercdata
        lowercdata.columns = lowercdata.columns.str.lower()
        return lowercdata

    def aggregator_group(self, data_lowercase):
        """
        Groups the DataFrame by 'year_group' and aggregates the 'value' column by sum for different sexes.

        Args:
            data_lowercase (pandas.DataFrame): The DataFrame to perform grouping and aggregation on.

        Returns:
            pandas.DataFrame: The resulting DataFrame with grouped and aggregated data.

        """
        self._data = data_lowercase
        self._data['year'] = self._data['year'].astype(int)
        self._data['year_group'] = self._data['year'] // 5 * 5

        # Group by 'year_group' and aggregate the 'value' column by sum for different sexes
        self._data = self._data.groupby(['year_group']).agg(female=('value',
                                                                    lambda x: x[self._data['sex'] == 'Female'].sum()),
                                                            male=(
                                                                'value',
                                                                lambda x: x[self._data['sex'] == 'Male'].sum()),
                                                            both_sexes=(
                                                                'value', 'sum'))
        return self._data

    def data_filter(self, filter_data):
        """
        Filters the DataFrame based on the 'statistic label' column containing 'First Year'.

        Args:
            filter_data (pandas.DataFrame): The DataFrame to filter.

        Returns:
            pandas.DataFrame: The filtered DataFrame.

        """
        self._data = filter_data
        result_filter = self._data[self._data['statistic label'].str.contains('First Year')]
        return result_filter

    def data_transform(self, data):
        """
        Performs data transformation on the specified DataFrame.

        Args:
            data (pandas.DataFrame): The DataFrame to transform.

        Returns:
            Tuple[pandas.DataFrame, pandas.DataFrame]: A tuple containing the transformed DataFrame with grouped and
            aggregated data, and the filtered DataFrame.

        """
        logging.info('************************************************************************************************')
        logging.info('Transforming the data')
        self._data = data
        self.data_lowercase = self.all_column_lower_case(self._data)
        self._agg_group_year_data = self.aggregator_group(self.data_lowercase)
        self._filter_data = self.data_filter(self.data_lowercase)
        logging.info('************************************************************************************************')
        return self._agg_group_year_data, self._filter_data

