from abc import abstractmethod, ABC

import pandas as pd
from loguru import logger

from fuzzydata.core.generator import generate_table


class Artifact(ABC):
    def __init__(self, label, schema_map=None, filename=None, file_format='csv',
                 in_memory=True):

        self.filename = filename
        self.label = label
        self.in_memory = in_memory
        self.format = file_format
        self.schema_map = schema_map
        self.table = None

        logger.info('New Artifact: {label}', label=label)

    @abstractmethod
    def generate(self, num_rows, schema):
        pass

    @abstractmethod
    def deserialize(self):
        pass

    @abstractmethod
    def serialize(self):
        pass


class DataFrameArtifact(Artifact):

    def __init__(self):
        super(DataFrameArtifact, self).__init__()
        self._deserialization_function = {
            'csv': pd.read_csv
        }
        self._serialization_function = {
            'csv': self.table.to_csv
        }

    def generate(self, num_rows, schema):
        self.table = generate_table(num_rows, schema=schema)
        pass

    def deserialize(self):
        self.table = self._deserialization_function[self.file_format](self.filename)
        self.in_memory = True

    def serialize(self):
        if self.in_memory:
            self._serialization_function[self.file_format](self.filename)


