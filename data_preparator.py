import os

import pandas as pd
from airflow.models.baseoperator import BaseOperator


class DataPreparator(BaseOperator):
    def __init__(self, input_file_name: str, output_file_directory: str, **kwargs):
        super().__init__(**kwargs)
        self.input_file_name = input_file_name
        self.output_file_directory = output_file_directory

    def execute(self, context):
        data = pd.read_csv(self.input_file_name)

        for column in data.select_dtypes(['object']).columns:
            if column not in ['Original_Release_Date', 'Current_Version_Release_Date']:
                data[column] = data[column].fillna('-')

        for column in data[data.columns.difference(data.select_dtypes(['object']).columns)].columns:
            data[column] = data[column].fillna(-999)

        data['Original_Release_Date'] = data['Original_Release_Date'].fillna('0000-00-00T00:00:00Z')
        data['Current_Version_Release_Date'] = data['Current_Version_Release_Date'].fillna('0000-00-00T00:00:00Z')

        data['All_Genres'] = data['All_Genres'].str.replace('\'', '')
        data['Languages'] = data['All_Genres'].str.replace('\'', '')

        os.chdir(self.output_file_directory)
        if (os.listdir().__len__() == 0):
            self.count = 0
        else:
            self.count = max([int(item.split('.')[0]) for item in os.listdir()])

        data.to_csv(os.path.join(self.output_file_directory, str(self.count + 1) + '.csv'), index=False)