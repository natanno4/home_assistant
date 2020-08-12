import csv
from utils.file_data_extractors.file_data_extractor import FileDataExtractor


class CsvFileDataExtractor(FileDataExtractor):
    def __init__(self):
        """
        constructor
        """
        pass

    def data_extractor(self, file_path):
        """
        extracts the data from given csv file in the given path. if failed return None.
        the data is returned as a list with field names in first place and the values.
        :param file_path: the csv file path.
         :type file_path: str.
        """
        try:
            with open(file_path, newline='', encoding='utf-8', errors='ignore') as csv_file:
                data = list(csv.reader(csv_file))  # get data from file.
            return data
        except:
            print('csv file error')
            return None

