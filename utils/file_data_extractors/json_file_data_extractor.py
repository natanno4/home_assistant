from utils.file_data_extractors.file_data_extractor import FileDataExtractor
import pandas as pd


class JsonFileDataExtractor(FileDataExtractor):
    def __init__(self):
        """
        constructor
        """
        pass

    def data_extractor(self, file_path):
        """
        extracts the data from given json file in the given path. if failed return None.
        the data is returned as a list with field names in first place and the values.
        :param file_path: the json file path.
         :type file_path: str.
        """
        try:
            with open(file_path, encoding='utf-8-sig', newline='') as json_file:
                df = pd.read_json(json_file)  # read json
            values = df.values
            values = values.tolist()  # create values lists
            field_names = df.columns.values.tolist()  # create list of field names
            values.insert(0, field_names)
            return values
        except:
            print('json file error')
            return None
