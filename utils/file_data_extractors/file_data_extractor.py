from abc import ABC, abstractmethod


class FileDataExtractor(ABC):
    def __init__(self):
        """
        constructor
        """
        pass

    @abstractmethod
    def data_extractor(self, file_path):
        """
        extracts the data from given file in the given path. abstract method.
        :param file_path: the file path.
         :type file_path: str.
        """
        pass
