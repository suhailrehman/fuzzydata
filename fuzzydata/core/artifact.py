from abc import abstractmethod, ABC

import pandas as pd
import logging

from fuzzydata.core.generator import generate_table

logger = logging.getLogger(__name__)


class Artifact(ABC):
    def __init__(self, label, schema_map=None, filename=None, file_format='csv',
                 in_memory=True):

        self.filename = filename
        self.label = label
        self.in_memory = in_memory
        self.file_format = file_format
        self.schema_map = schema_map
        self.table = None

        logger.info(f'New Artifact: {label}')

    @abstractmethod
    def generate(self, num_rows, schema):
        pass

    @abstractmethod
    def deserialize(self):
        pass

    @abstractmethod
    def serialize(self):
        pass

    @abstractmethod
    def destroy(self):
        pass


class DataFrameArtifact(Artifact):

    def __init__(self, *args, **kwargs):
        super(DataFrameArtifact, self).__init__(*args, **kwargs)
        self._deserialization_function = {
            'csv': pd.read_csv
        }
        self._serialization_function = {
            'csv': 'to_csv'
        }

    def generate(self, num_rows, schema):
        self.table = generate_table(num_rows, column_dict=schema)

    def deserialize(self):
        self.table = self._deserialization_function[self.file_format](self.filename)
        self.in_memory = True

    def serialize(self):
        if self.in_memory:
            serialization_method = getattr(self.table, self._serialization_function[self.file_format])
            serialization_method(self.filename)

    def destroy(self):
        del self.table


class SQLArtifact(Artifact):

    def __init__(self, *args, **kwargs):
        self.sql_engine = kwargs.pop("sql_engine")
        super(SQLArtifact, self).__init__(*args, **kwargs)
        self._deserialization_function = {
            'csv': pd.read_csv
        }
        self._serialization_function = {
            'csv': 'to_csv'
        }

        self._get_table = f'SELECT * FROM {self.label}'
        self._del_table = f'DROP TABLE IF EXISTS {self.label}'

    def generate(self, num_rows, schema):
        df = generate_table(num_rows, column_dict=schema)
        df.to_sql(self.label, con=self.sql_engine)  # Convert Generated Dataframe to SQL Table with Schema conversion
        self.table = df

    def deserialize(self):
        df = self._deserialization_function[self.file_format](self.filename)
        df.to_sql(self.label, con=self.sql_engine)
        self.table = df
        self.in_memory = True

    def serialize(self):
        if self.in_memory:
            df = pd.read_sql(self._get_table, con=self.sql_engine)
            serialization_method = getattr(df, self._serialization_function[self.file_format])
            serialization_method(self.filename)

    def destroy(self):
        del self.table
        self.sql_engine.execute(self._del_table)

